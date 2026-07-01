"""MyCorr API client for fetching table data with Arrow format support."""

from __future__ import annotations

import asyncio
import io
import os
import queue
import threading
from collections.abc import AsyncIterator, Coroutine, Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast
from urllib.parse import urlparse

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from pymycorr._progress import ProgressTracker
from pymycorr.exceptions import (
    QuotaExceededError,
    RateLimitError,
    StreamingError,
    TableAPIError,
    TableConversionError,
    TableNotFoundError,
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

    DEFAULT_URL = "https://space.mycorr.app"
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
        hostname = urlparse(self.url).hostname or ""
        self._verify_ssl = hostname not in ("localhost", "127.0.0.1")
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
    def _handle_error_response(status_code: int, body: bytes | str, context: str = "") -> None:
        """Raise the appropriate exception for an HTTP error response.

        Args:
            status_code: The HTTP status code.
            body: The raw response body (bytes or str).
            context: Description of the operation for error messages.

        Raises:
            TableNotFoundError: If the status code is 404.
            QuotaExceededError: If the status code is 429.
            TableAPIError: For all other non-200 status codes.
        """
        try:
            import json

            error_body = json.loads(body) if isinstance(body, bytes | str) else {}
        except Exception:
            error_body = {}

        if status_code == 404:
            msg = error_body.get("message", "Not found")
            raise TableNotFoundError(f"{context}{msg}")
        if status_code == 429:
            if error_body.get("error") == "rate_limit_exceeded":
                raise RateLimitError(error_body)
            raise QuotaExceededError(error_body)

        msg = error_body.get("message", f"HTTP {status_code}")
        raise TableAPIError(f"{context}{msg}")

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
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API errors.
            StreamingError: If IPC stream parsing fails.
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
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API errors.
            StreamingError: If IPC stream parsing fails.
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
                    self._handle_error_response(
                        response.status_code, response.content, "Error fetching table: "
                    )

                # Collect all chunks (server now sends single IPC stream)
                chunks: list[bytes] = []
                tracker = ProgressTracker(effective_progress, desc=f"Fetching {table_id}")
                prev_downloaded = 0
                with tracker:
                    async for chunk in response.aiter_bytes():
                        wire_bytes = response.num_bytes_downloaded
                        tracker.update(wire_bytes - prev_downloaded)
                        prev_downloaded = wire_bytes
                        chunks.append(chunk)

                # Parse single IPC stream with PyArrow's native reader
                data = b"".join(chunks)
                try:
                    reader = ipc.open_stream(data)
                    uncompressed = 0
                    for batch in reader:
                        uncompressed += batch.nbytes
                        yield batch
                except (pa.ArrowInvalid, pa.ArrowIOError) as e:
                    raise StreamingError(f"Failed to parse IPC stream: {e}") from e
                else:
                    tracker.print_summary(uncompressed_bytes=uncompressed)

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
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API errors.
            StreamingError: If IPC stream parsing fails.
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
        tracker_holder: list[ProgressTracker] = []

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
                            self._handle_error_response(
                                response.status_code,
                                response.content,
                                "Error fetching table: ",
                            )

                        tracker = ProgressTracker(effective_progress, desc=f"Fetching {table_id}")
                        tracker_holder.append(tracker)
                        prev_downloaded = 0
                        with tracker:
                            async for chunk in response.aiter_bytes():
                                wire_bytes = response.num_bytes_downloaded
                                tracker.update(wire_bytes - prev_downloaded)
                                prev_downloaded = wire_bytes
                                buffer.feed(chunk)
                except Exception as e:
                    error_holder.append(e)
                finally:
                    buffer.close()

            asyncio.run(fetch())

        thread = threading.Thread(target=producer, daemon=True)
        thread.start()

        # PyArrow reads from buffer as data arrives
        uncompressed = 0
        try:
            reader = ipc.open_stream(buffer)
            for batch in reader:
                uncompressed += batch.nbytes
                yield batch
        except (pa.ArrowInvalid, pa.ArrowIOError) as e:
            thread.join()
            if error_holder:
                raise error_holder[0] from e
            raise StreamingError(f"Failed to parse IPC stream: {e}") from e

        thread.join()

        # Re-raise any error from the producer thread
        if error_holder:
            raise error_holder[0]

        # Print summary with uncompressed size from main thread
        if tracker_holder:
            tracker_holder[0].print_summary(uncompressed_bytes=uncompressed)

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
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API errors.
            StreamingError: If IPC stream parsing fails.
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
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API-related errors.
            TableConversionError: If conversion to DataFrame fails.
            StreamingError: If IPC stream parsing fails.
        """
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        async def get_dataframe_async() -> pd.DataFrame | pl.DataFrame:
            """Internal async function to fetch and convert data."""
            data_stream = await self._get_data_stream(table_id, version, progress=progress)

            try:
                if engine == "pandas":
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
            ValueError: If table_id is empty.
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API-related errors.
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
            self._handle_error_response(
                response.status_code, response.text, "Error fetching table info: "
            )

        return cast(dict[str, Any], response.json())

    @staticmethod
    def _to_arrow_table(
        data: pd.DataFrame | pl.DataFrame | pa.Table | pa.RecordBatch,
    ) -> pa.Table:
        """Coerce a DataFrame / Arrow value into a PyArrow Table with plain Utf8
        strings.

        Accepts a pyarrow Table/RecordBatch, a polars DataFrame, or a pandas
        DataFrame. String columns are downcast from ``LargeUtf8``/``Utf8View`` to
        plain ``Utf8`` — MyCorr persistence rejects the wide string variants that
        polars (and arrow-backed pandas) emit natively.
        """
        if isinstance(data, pa.Table):
            table = data
        elif isinstance(data, pa.RecordBatch):
            table = pa.Table.from_batches([data])
        elif type(data).__module__.startswith("polars"):
            table = data.to_arrow()
        elif type(data).__module__.startswith("pandas"):
            table = pa.Table.from_pandas(data, preserve_index=False)
        else:
            raise TypeError(
                "data must be a pandas/polars DataFrame or a pyarrow Table/RecordBatch, "
                f"got {type(data).__name__}"
            )

        def to_utf8(t: pa.DataType) -> pa.DataType:
            if pa.types.is_large_string(t):
                return pa.string()
            if getattr(pa.types, "is_string_view", lambda _t: False)(t):
                return pa.string()
            return t

        target = pa.schema([pa.field(f.name, to_utf8(f.type), f.nullable) for f in table.schema])
        return table.cast(target) if target != table.schema else table

    def create_table(
        self,
        model_id: str,
        data: pd.DataFrame | pl.DataFrame | pa.Table | pa.RecordBatch,
        *,
        name: str,
        primary_key: str | Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Create a new table in a model from tabular data.

        Encodes ``data`` as an Arrow IPC stream and uploads it to the write API;
        the server persists the table and adds it to ``model_id``. Requires a
        write-scoped token with edit access to the model.

        Args:
            model_id: The model the table is created in.
            data: A pandas/polars DataFrame or a pyarrow Table/RecordBatch.
            name: Name for the new table.
            primary_key: Primary-key column name(s) — a single name or a sequence
                for a composite key. Marking a key here is what lets the table be
                diff-synced later.

        Returns:
            Dict with the created ``model_id`` and ``table_id``.

        Raises:
            ValueError: If model_id or name is empty.
            TypeError: If data is not a supported type.
            TableNotFoundError: If the model is not found (404).
            RateLimitError: If the API rate limit is exceeded (429).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API errors.
        """
        if not model_id:
            raise ValueError("model_id is required")
        if not name:
            raise ValueError("name is required")

        if primary_key is None:
            pk_cols: list[str] = []
        elif isinstance(primary_key, str):
            pk_cols = [primary_key]
        else:
            pk_cols = list(primary_key)

        table = self._to_arrow_table(data)
        sink = io.BytesIO()
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        params: dict[str, Any] = {"name": name, "model": model_id}
        if pk_cols:
            params["pk"] = ",".join(pk_cols)

        response = httpx.post(
            f"{self.url}/server/api/datasets/tables",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/vnd.apache.arrow.stream",
            },
            params=params,
            content=sink.getvalue(),
            verify=self._verify_ssl,
            timeout=httpx.Timeout(timeout=300.0, connect=30.0),
        )

        if response.status_code != 200:
            self._handle_error_response(
                response.status_code, response.text, "Error creating table: "
            )

        return cast(dict[str, Any], response.json())
