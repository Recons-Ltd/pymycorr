"""MyCorr API client for fetching table data with Arrow format support."""

from __future__ import annotations

import asyncio
import os
import queue
import threading
from collections.abc import AsyncIterator, Coroutine, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from pymycorr._json_api import (
    CheckpointInfo,
    ModelMetadata,
    TableDataPage,
    TableInfo,
    TableMetadata,
)
from pymycorr._progress import ProgressTracker
from pymycorr.exceptions import (
    AuthenticationError,
    ForbiddenError,
    JsonAPIError,
    StreamingError,
    TableConversionError,
    NotFoundError,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

T = TypeVar("T")


class _StreamingBuffer:
    """Bridges async HTTP chunks to PyArrow's synchronous read() interface."""

    def __init__(self) -> None:
        self._buffer = bytearray()
        self._chunks: queue.Queue[bytes | None] = queue.Queue()
        self._eof = False

    def feed(self, chunk: bytes) -> None:
        """Called by producer with incoming chunks."""
        self._chunks.put(chunk)

    def close(self) -> None:
        """Signal end of stream."""
        self._chunks.put(None)

    def read(self, n: int = -1) -> bytes:
        """Called by PyArrow - blocks until data available."""
        while not self._eof and (n == -1 or len(self._buffer) < n):
            try:
                chunk = self._chunks.get(timeout=300)
            except queue.Empty:
                break
            if chunk is None:
                self._eof = True
                break
            self._buffer.extend(chunk)

        if n == -1 or n >= len(self._buffer):
            result = bytes(self._buffer)
            self._buffer.clear()
        else:
            result = bytes(self._buffer[:n])
            del self._buffer[:n]
        return result


class MyCorr:
    """Client for fetching table data from API with Arrow format support."""

    DEFAULT_URL = "https://api.mycorr.recons-ltd.com"
    _default_progress: bool | Literal["auto"]

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        env_file: str | Path | None = None,
        progress: bool | Literal["auto"] = "auto",
    ) -> None:
        """Initialize the client with authentication token and API URL.

        Configuration is resolved in the following order:
        1. Explicit parameters (highest priority)
        2. Environment variables (MYCORR_API_URL, MYCORR_API_TOKEN)
        3. Default values (URL only)

        Args:
            url: API base URL. Falls back to MYCORR_API_URL env var, then default.
            token: Authentication token. Falls back to MYCORR_API_TOKEN env var.
            env_file: Optional path to .env file. If None, auto-discovers .env.
            progress: Show download progress. True always shows, False never shows,
                'auto' (default) shows in notebooks/terminals but not in non-interactive.

        Raises:
            ValueError: If token cannot be resolved.
        """
        load_dotenv(dotenv_path=env_file)

        self.url = (url or os.getenv("MYCORR_API_URL") or self.DEFAULT_URL).rstrip("/")
        self.token = token or os.getenv("MYCORR_API_TOKEN")
        self._verify_ssl = "localhost" not in self.url
        self._default_progress = progress

        if not self.token:
            raise ValueError(
                "token is required (pass explicitly or set MYCORR_API_TOKEN environment variable)"
            )

    def _run_sync(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run an async coroutine from a sync context.

        Handles detection of running event loops and applies nest_asyncio
        when needed (e.g., in Jupyter notebooks).
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        if loop.is_running():
            try:
                import nest_asyncio

                nest_asyncio.apply()
                return loop.run_until_complete(coro)
            except ImportError as e:
                raise RuntimeError(
                    "nest_asyncio is required when calling from within an async context "
                    "(e.g., Jupyter notebooks). Install with: pip install nest-asyncio"
                ) from e
        return loop.run_until_complete(coro)

    @staticmethod
    def _build_version_params(
        table_id: str,
        version: int | str | None,
        *,
        include_scope: bool = True,
    ) -> dict[str, Any]:
        """Build query params dict for version specification.

        Args:
            table_id: The table identifier.
            version: Version number (int), alias (str), or None for 'latest'.
            include_scope: Whether to include scope='read' in params.

        Returns:
            Dict with table_id, version/version_alias, and optionally scope.

        Raises:
            TypeError: If version has an invalid type.
        """
        if version is None:
            version = "latest"

        params: dict[str, Any] = {"table_id": table_id}
        if include_scope:
            params["scope"] = "read"

        if isinstance(version, int):
            params["version"] = version
        elif isinstance(version, str):
            params["version_alias"] = version
        else:
            raise TypeError(
                f"Expected 'version' to be int, str, or None, got {type(version).__name__}"
            )

        return params

    async def _get_data_stream(
        self,
        table_id: str,
        version: int | str | None = None,
        progress: bool | Literal["auto"] | None = None,
    ) -> pa.Table:
        """Fetch Arrow stream from API asynchronously.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            progress: Show download progress. None uses client default.

        Returns:
            PyArrow Table containing the data.

        Raises:
            ValueError: If table_id is empty.
            TableAPIError: For API or parsing errors.
        """
        batches = []
        async for batch in self._stream_record_batches(table_id, version, progress=progress):
            batches.append(batch)

        if not batches:
            return pa.table({})

        return pa.Table.from_batches(batches)

    async def _stream_record_batches(
        self,
        table_id: str,
        version: int | str | None = None,
        progress: bool | Literal["auto"] | None = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Stream Arrow record batches from API asynchronously.

        Yields individual RecordBatch objects as they arrive over the network,
        allowing processing of large tables without loading everything into memory.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            progress: Show download progress. None uses client default.

        Yields:
            PyArrow RecordBatch objects.

        Raises:
            ValueError: If table_id is empty.
            StreamingError: For streaming or parsing errors.
        """
        if not table_id:
            raise ValueError("Table ID is required")

        params = self._build_version_params(table_id, version)
        effective_progress: bool | Literal["auto"] = (
            progress if progress is not None else self._default_progress
        )
        timeout = httpx.Timeout(timeout=300.0, connect=30.0)

        try:
            async with (
                httpx.AsyncClient(timeout=timeout, verify=self._verify_ssl) as client,
                client.stream(
                    "GET",
                    f"{self.url}/data/table/stream",
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Accept": "application/vnd.apache.arrow.stream",
                    },
                    params=params,
                ) as response,
            ):
                if response.status_code != 200:
                    await response.aread()
                    try:
                        error = response.json()
                        error_msg = error.get("message", "Internal server error")
                    except Exception:
                        error_msg = f"HTTP {response.status_code}"
                    raise StreamingError(f"Error fetching table: {error_msg}")

                # Collect all chunks (server now sends single IPC stream)
                chunks: list[bytes] = []
                with ProgressTracker(effective_progress, desc=f"Fetching {table_id}") as tracker:
                    async for chunk in response.aiter_bytes():
                        tracker.update(len(chunk))
                        chunks.append(chunk)

                # Parse single IPC stream with PyArrow's native reader
                data = b"".join(chunks)
                try:
                    reader = ipc.open_stream(data)
                    for batch in reader:
                        yield batch
                except (pa.ArrowInvalid, pa.ArrowIOError) as e:
                    raise StreamingError(f"Failed to parse IPC stream: {e}") from e

        except httpx.HTTPError as e:
            raise StreamingError(f"Connection error: {e!s}") from e

    def _iter_record_batches(
        self,
        table_id: str,
        version: int | str | None = None,
        progress: bool | Literal["auto"] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Synchronous iterator over record batches.

        Uses a background thread with queue for true streaming in sync contexts.
        In Jupyter notebooks or running event loops, falls back to collecting
        all batches first (requires nest-asyncio).

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            progress: Show download progress. None uses client default.

        Yields:
            PyArrow RecordBatch objects.

        Raises:
            ValueError: If table_id is empty.
            StreamingError: For streaming or parsing errors.
            RuntimeError: If nest_asyncio required but not installed.
        """
        try:
            asyncio.get_running_loop()
            has_running_loop = True
        except RuntimeError:
            has_running_loop = False

        if has_running_loop:
            # In Jupyter or async context - collect all batches via _run_sync
            async def collect() -> list[pa.RecordBatch]:
                return [
                    batch
                    async for batch in self._stream_record_batches(
                        table_id, version, progress=progress
                    )
                ]

            yield from self._run_sync(collect())
        else:
            # No running loop - use thread + queue for true streaming
            yield from self._sync_stream_batches(table_id, version, progress=progress)

    def _sync_stream_batches(
        self,
        table_id: str,
        version: int | str | None,
        progress: bool | Literal["auto"] | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """True streaming sync iteration using thread + PyArrow native reader.

        Uses _StreamingBuffer to bridge async HTTP chunks to PyArrow's
        synchronous read() interface, enabling true streaming where batches
        are yielded as data arrives over the network.
        """
        buffer = _StreamingBuffer()
        error_holder: list[Exception] = []

        def producer() -> None:
            async def fetch() -> None:
                params = self._build_version_params(table_id, version)
                effective_progress: bool | Literal["auto"] = (
                    progress if progress is not None else self._default_progress
                )
                timeout = httpx.Timeout(timeout=300.0, connect=30.0)

                try:
                    async with (
                        httpx.AsyncClient(timeout=timeout, verify=self._verify_ssl) as client,
                        client.stream(
                            "GET",
                            f"{self.url}/data/table/stream",
                            headers={
                                "Authorization": f"Bearer {self.token}",
                                "Accept": "application/vnd.apache.arrow.stream",
                            },
                            params=params,
                        ) as response,
                    ):
                        if response.status_code != 200:
                            await response.aread()
                            try:
                                error_msg = response.json().get("message", "Internal server error")
                            except Exception:
                                error_msg = f"HTTP {response.status_code}"
                            raise StreamingError(f"Error fetching table: {error_msg}")

                        with ProgressTracker(
                            effective_progress, desc=f"Fetching {table_id}"
                        ) as tracker:
                            async for chunk in response.aiter_bytes():
                                tracker.update(len(chunk))
                                buffer.feed(chunk)
                except Exception as e:
                    error_holder.append(e)
                finally:
                    buffer.close()

            asyncio.run(fetch())

        thread = threading.Thread(target=producer, daemon=True)
        thread.start()

        # PyArrow reads from buffer as data arrives
        try:
            reader = ipc.open_stream(buffer)
            yield from reader
        except (pa.ArrowInvalid, pa.ArrowIOError) as e:
            thread.join()
            if error_holder:
                raise error_holder[0] from e
            raise StreamingError(f"Failed to parse IPC stream: {e}") from e

        thread.join()

        # Re-raise any error from the producer thread
        if error_holder:
            raise error_holder[0]

    def _stream_to_dataframes(
        self,
        table_id: str,
        version: int | str | None = None,
        engine: Literal["pandas", "polars"] = "pandas",
        progress: bool | Literal["auto"] | None = None,
    ) -> Iterator[Any]:
        """Stream data as individual DataFrames per batch.

        Useful for processing large tables in chunks without loading
        everything into memory.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            engine: Data processing engine ('pandas' or 'polars').
            progress: Show download progress. None uses client default.

        Yields:
            DataFrame in the specified format (pandas or polars).

        Raises:
            ValueError: If engine is not supported.
            StreamingError: For streaming errors.
            TableConversionError: If conversion to DataFrame fails.
        """
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        for batch in self._iter_record_batches(table_id, version, progress=progress):
            try:
                if engine == "pandas":
                    # Convert batch to table first to ensure DataFrame output
                    table = pa.Table.from_batches([batch])
                    yield table.to_pandas()
                else:
                    import polars as pl_module

                    yield pl_module.from_arrow(batch)
            except Exception as e:
                raise TableConversionError(
                    f"Failed to convert batch to {engine} format: {e!s}"
                ) from e

    def get_table(
        self,
        table_id: str,
        version: int | str | None = None,
        engine: Literal["pandas", "polars"] = "pandas",
        progress: bool | Literal["auto"] | None = None,
    ) -> pd.DataFrame | pl.DataFrame:
        """Fetch table data synchronously and convert to DataFrame.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            engine: Data processing engine ('pandas' or 'polars').
            progress: Show download progress. None uses client default.

        Returns:
            DataFrame in the specified format (pandas or polars).

        Raises:
            TypeError: If version has wrong type.
            ValueError: If engine is not supported.
            TableAPIError: For API-related errors.
            TableConversionError: If conversion to DataFrame fails.
        """
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        async def get_dataframe_async() -> pd.DataFrame | pl.DataFrame:
            """Internal async function to fetch and convert data."""
            data_stream = await self._get_data_stream(table_id, version, progress=progress)

            try:
                if engine == "pandas":
                    if len(data_stream) == 0:
                        print(f"Table is empty with schema: {data_stream.schema}")
                    return cast("pd.DataFrame", data_stream.to_pandas())
                else:
                    import polars as pl_module

                    return cast("pl.DataFrame", pl_module.from_arrow(data_stream))
            except Exception as e:
                raise TableConversionError(
                    f"Failed to convert table to {engine} format: {e!s}"
                ) from e

        return self._run_sync(get_dataframe_async())

    def get_table_info(self, table_id: str, version: int | str | None = None) -> dict[str, Any]:
        """Get table schema and metadata without fetching full data.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').

        Returns:
            Dictionary containing table snapshot (version, meta, schema).

        Raises:
            TableAPIError: If fetching table info fails.
        """
        if not table_id:
            raise ValueError("Table ID is required")

        params: dict[str, Any] = {
            "table_id": table_id,
            "scope": "read",
            "version_alias": version if isinstance(version, str) else "latest",
        }
        if isinstance(version, int):
            params["version"] = version

        response = httpx.get(
            f"{self.url}/data/tableinfo",
            headers={"Authorization": f"Bearer {self.token}"},
            params=params,
            verify=self._verify_ssl,
        )

        if response.status_code != 200:
            try:
                error_msg = response.json().get("message", "Internal server error")
            except Exception:
                error_msg = f"HTTP {response.status_code}"
            raise StreamingError(f"Error fetching table info: {error_msg}")

        return cast(dict[str, Any], response.json())

    # ── JSON Client API (/data/json/v1) ──────────────────────────────

    @property
    def _json_client(self) -> httpx.Client:
        """Lazy-initialized httpx.Client for JSON API requests."""
        if not hasattr(self, "_json_client_instance"):
            self._json_client_instance = httpx.Client(
                base_url=f"{self.url}/data/json/v1",
                headers={"Authorization": f"Bearer {self.token}"},
                verify=self._verify_ssl,
                timeout=httpx.Timeout(timeout=60.0, connect=10.0),
            )
        return self._json_client_instance

    def _json_api_request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make a GET request to the JSON client API.

        Args:
            path: Path relative to /data/json/v1 (e.g., "/models").
            params: Optional query parameters.

        Returns:
            The parsed JSON response body (envelope with "data" and optional "meta").

        Raises:
            AuthenticationError: On 401 responses.
            ForbiddenError: On 403 responses.
            NotFoundError: On 404 responses.
            JsonAPIError: On other error responses.
        """
        response = self._json_client.get(path, params=params)

        if response.status_code != 200:
            try:
                error_body = response.json()
                error_info = error_body.get("error", {})
                error_msg = error_info.get("message", f"HTTP {response.status_code}")
            except Exception:
                error_msg = f"HTTP {response.status_code}"

            if response.status_code == 401:
                raise AuthenticationError(error_msg)
            if response.status_code == 403:
                raise ForbiddenError(error_msg)
            if response.status_code == 404:
                raise NotFoundError(error_msg)
            raise JsonAPIError(error_msg, status_code=response.status_code)

        return cast(dict[str, Any], response.json())

    def list_models(self) -> list[ModelMetadata]:
        """List all models the authenticated user has access to.

        Returns:
            List of model metadata dictionaries.

        Raises:
            AuthenticationError: If the token is invalid or expired.
            JsonAPIError: For other API errors.
        """

        response = self._json_api_request("/models")
        return cast(list[ModelMetadata], response["data"])

    def get_model(self, model_id: str) -> ModelMetadata:
        """Get metadata for a single model.

        Args:
            model_id: The model identifier.

        Returns:
            Model metadata dictionary.

        Raises:
            ValueError: If model_id is empty.
            NotFoundError: If the model is not found.
            ForbiddenError: If the user lacks permission.
            JsonAPIError: For other API errors.
        """
        if not model_id:
            raise ValueError("model_id is required")
        response = self._json_api_request(f"/models/{model_id}")
        return cast(ModelMetadata, response["data"])

    def list_tables(self, model_id: str) -> list[TableInfo]:
        """List all tables in a model.

        Args:
            model_id: The model identifier.

        Returns:
            List of table info dictionaries.

        Raises:
            ValueError: If model_id is empty.
            ForbiddenError: If the user lacks permission.
            JsonAPIError: For other API errors.
        """
        if not model_id:
            raise ValueError("model_id is required")
        response = self._json_api_request(f"/models/{model_id}/tables")
        return cast(list[TableInfo], response["data"])

    def get_table_schema(self, table_id: str) -> TableMetadata:
        """Get metadata and column schema for a table.

        Args:
            table_id: The table identifier.

        Returns:
            Table metadata with version, row/column counts, and column schema.

        Raises:
            ValueError: If table_id is empty.
            NotFoundError: If table not found.
            ForbiddenError: If the user lacks permission.
            JsonAPIError: For other API errors.
        """
        if not table_id:
            raise ValueError("table_id is required")
        response = self._json_api_request(f"/tables/{table_id}")
        return cast(TableMetadata, response["data"])

    def list_checkpoints(self, table_id: str) -> list[CheckpointInfo]:
        """List all checkpoints for a table, sorted by most recent first.

        Args:
            table_id: The table identifier.

        Returns:
            List of checkpoint metadata dictionaries.

        Raises:
            ValueError: If table_id is empty.
            NotFoundError: If table not found.
            ForbiddenError: If the user lacks permission.
            JsonAPIError: For other API errors.
        """
        if not table_id:
            raise ValueError("table_id is required")
        response = self._json_api_request(f"/tables/{table_id}/checkpoints")
        return cast(list[CheckpointInfo], response["data"])

    def get_table_data(
        self,
        table_id: str,
        *,
        version: int | None = None,
        table_checkpoint: str | None = None,
        limit: int | None = None,
        page_token: str | None = None,
    ) -> TableDataPage:
        """Fetch a single page of table data in column-oriented JSON format.

        Args:
            table_id: The table identifier.
            version: Specific table version. Mutually exclusive with table_checkpoint.
            table_checkpoint: Checkpoint alias. Mutually exclusive with version.
            limit: Maximum rows per page (server may cap this).
            page_token: Pagination cursor from a previous response's meta.next_page_token.

        Returns:
            Dictionary with "data" (TableDataPayload) and "meta" (ResponseMeta).

        Raises:
            ValueError: If both version and table_checkpoint are provided, or table_id empty.
            NotFoundError: If table not found.
            ForbiddenError: If the user lacks permission.
            JsonAPIError: For other API errors.
        """
        if not table_id:
            raise ValueError("table_id is required")
        if version is not None and table_checkpoint is not None:
            raise ValueError("Cannot specify both version and table_checkpoint")

        params: dict[str, Any] = {}
        if version is not None:
            params["version"] = version
        if table_checkpoint is not None:
            params["table_checkpoint"] = table_checkpoint
        if limit is not None:
            params["limit"] = limit
        if page_token is not None:
            params["page_token"] = page_token

        response = self._json_api_request(
            f"/tables/{table_id}/data",
            params=params,
        )
        return cast(TableDataPage, response)

    def iter_table_pages(
        self,
        table_id: str,
        *,
        version: int | None = None,
        table_checkpoint: str | None = None,
        limit: int | None = None,
    ) -> Iterator[TableDataPage]:
        """Iterate over all pages of table data, auto-following pagination.

        Args:
            table_id: The table identifier.
            version: Specific table version. Mutually exclusive with table_checkpoint.
            table_checkpoint: Checkpoint alias. Mutually exclusive with version.
            limit: Maximum rows per page.

        Yields:
            TableDataPage dictionaries, one per page.

        Raises:
            ValueError: If both version and table_checkpoint provided, or table_id empty.
            JsonAPIError: For API errors on any page.
        """
        if not table_id:
            raise ValueError("table_id is required")
        if version is not None and table_checkpoint is not None:
            raise ValueError("Cannot specify both version and table_checkpoint")

        next_page_token: str | None = None
        while True:
            page = self.get_table_data(
                table_id,
                version=version,
                table_checkpoint=table_checkpoint,
                limit=limit,
                page_token=next_page_token,
            )
            yield page
            next_page_token = page.get("meta", {}).get("next_page_token")
            if next_page_token is None:
                break

    def get_table_dataframe(
        self,
        table_id: str,
        *,
        version: int | None = None,
        table_checkpoint: str | None = None,
        limit: int | None = None,
        engine: Literal["pandas", "polars"] = "pandas",
    ) -> pd.DataFrame | pl.DataFrame:
        """Fetch all pages of table data and return as a single DataFrame.

        Auto-paginates through all pages, collects column data, and constructs
        a DataFrame. For large tables, consider iter_table_pages() instead.

        Args:
            table_id: The table identifier.
            version: Specific table version. Mutually exclusive with table_checkpoint.
            table_checkpoint: Checkpoint alias. Mutually exclusive with version.
            limit: Maximum rows per page (controls page size, all pages still fetched).
            engine: DataFrame engine ('pandas' or 'polars').

        Returns:
            DataFrame containing all table data.

        Raises:
            ValueError: If engine invalid, both version/checkpoint specified, or table_id empty.
            TableConversionError: If DataFrame conversion fails.
            JsonAPIError: For API errors.
        """
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        all_columns: dict[str, list[Any]] = {}
        column_names_ordered: list[str] = []

        for page in self.iter_table_pages(
            table_id,
            version=version,
            table_checkpoint=table_checkpoint,
            limit=limit,
        ):
            for col in page["data"]["columns"]:
                col_name = col["name"]
                if col_name not in all_columns:
                    all_columns[col_name] = []
                    column_names_ordered.append(col_name)
                all_columns[col_name].extend(col["data"])

        try:
            if engine == "pandas":
                import pandas as pd_module

                return cast(
                    "pd.DataFrame",
                    pd_module.DataFrame(
                        {name: all_columns[name] for name in column_names_ordered}
                    ),
                )
            else:
                import polars as pl_module

                return cast(
                    "pl.DataFrame",
                    pl_module.DataFrame(
                        {name: all_columns[name] for name in column_names_ordered}
                    ),
                )
        except Exception as e:
            raise TableConversionError(
                f"Failed to convert table data to {engine} format: {e!s}"
            ) from e

