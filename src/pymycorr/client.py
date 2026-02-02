"""MyCorr API client for fetching table data with Arrow format support."""

from __future__ import annotations

import asyncio
import os
import queue
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import aiohttp
import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from pymycorr.exceptions import StreamingError, TableConversionError

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


class MyCorr:
    """Client for fetching table data from API with Arrow format support."""

    DEFAULT_URL = "https://api.mycorr.recons-ltd.com"

    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        env_file: str | Path | None = None,
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

        Raises:
            ValueError: If token cannot be resolved.
        """
        load_dotenv(dotenv_path=env_file)

        self.url = (url or os.getenv("MYCORR_API_URL") or self.DEFAULT_URL).rstrip("/")
        self.token = token or os.getenv("MYCORR_API_TOKEN")
        self._verify_ssl = "localhost" not in self.url

        if not self.token:
            raise ValueError(
                "token is required (pass explicitly or set MYCORR_API_TOKEN environment variable)"
            )

    async def get_data_stream(
        self,
        table_id: str,
        version: int | str | None = None,
    ) -> pa.Table:
        """Fetch Arrow stream from API asynchronously.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').

        Returns:
            PyArrow Table containing the data.

        Raises:
            ValueError: If table_id is empty.
            TableAPIError: For API or parsing errors.
        """
        batches = []
        async for batch in self.stream_record_batches(table_id, version):
            batches.append(batch)

        if not batches:
            return pa.table({})

        return pa.Table.from_batches(batches)

    async def stream_record_batches(
        self,
        table_id: str,
        version: int | str | None = None,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Stream Arrow record batches from API asynchronously.

        Yields individual RecordBatch objects as they arrive from the server,
        allowing processing of large tables without loading everything into memory.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').

        Yields:
            PyArrow RecordBatch objects.

        Raises:
            ValueError: If table_id is empty.
            StreamingError: For streaming or parsing errors.
        """
        if not table_id:
            raise ValueError("Table ID is required")

        if version is None:
            version = "latest"

        params: dict[str, Any] = {"table_id": table_id, "scope": "read"}

        if isinstance(version, int):
            params["version"] = version
        elif isinstance(version, str):
            params["version_alias"] = version
        else:
            raise TypeError(
                f"Expected 'version' to be int, str, or None, got {type(version).__name__}"
            )

        batches_received = 0
        timeout = aiohttp.ClientTimeout(total=None, sock_read=300)
        ssl_context: bool = self._verify_ssl

        try:
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.get(
                    f"{self.url}/data/table/stream",
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Accept": "application/vnd.apache.arrow.stream",
                    },
                    params=params,
                    ssl=ssl_context,
                ) as response,
            ):
                if response.status != 200:
                    try:
                        error = await response.json()
                        error_msg = error.get("message", "Internal server error")
                    except Exception:
                        error_msg = f"HTTP {response.status}"
                    raise StreamingError(f"Error fetching table: {error_msg}")

                # Read full response
                data = await response.read()
                if not data:
                    return

                # Try parsing as a single IPC stream first
                try:
                    table = ipc.open_stream(data).read_all()
                    for batch in table.to_batches():
                        batches_received += 1
                        yield batch
                    return
                except (pa.ArrowInvalid, pa.ArrowIOError):
                    pass  # Fall through to multi-stream parsing

                # The backend sends multiple complete IPC streams (one per batch)
                eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"
                offset = 0

                while offset < len(data):
                    eos_pos = data.find(eos_marker, offset)
                    if eos_pos == -1:
                        break

                    stream_end = eos_pos + len(eos_marker)
                    complete_stream = data[offset:stream_end]
                    offset = stream_end

                    try:
                        reader = ipc.open_stream(complete_stream)
                        table = reader.read_all()
                        for batch in table.to_batches():
                            batches_received += 1
                            yield batch
                    except (pa.ArrowInvalid, pa.ArrowIOError):
                        continue

        except aiohttp.ClientError as e:
            raise StreamingError(
                f"Connection error after {batches_received} batches: {e!s}",
                batches_received=batches_received,
            ) from e

    def iter_record_batches(
        self,
        table_id: str,
        version: int | str | None = None,
    ) -> Iterator[pa.RecordBatch]:
        """Synchronous iterator over record batches.

        Uses a background thread with queue for true streaming in sync contexts.
        In Jupyter notebooks or running event loops, falls back to collecting
        all batches first (requires nest-asyncio).

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').

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
            # In Jupyter or async context - use nest_asyncio fallback
            try:
                import nest_asyncio

                nest_asyncio.apply()
            except ImportError as e:
                raise RuntimeError(
                    "nest_asyncio is required when calling from within an async context "
                    "(e.g., Jupyter notebooks). Install with: pip install nest-asyncio"
                ) from e

            # Collect all batches and yield
            async def collect() -> list[pa.RecordBatch]:
                batches = []
                async for batch in self.stream_record_batches(table_id, version):
                    batches.append(batch)
                return batches

            loop = asyncio.get_event_loop()
            batches = loop.run_until_complete(collect())
            yield from batches
        else:
            # No running loop - use thread + queue for true streaming
            yield from self._sync_stream_batches(table_id, version)

    def _sync_stream_batches(
        self,
        table_id: str,
        version: int | str | None,
    ) -> Iterator[pa.RecordBatch]:
        """True streaming sync iteration using thread + queue."""
        batch_queue: queue.Queue[pa.RecordBatch | None | Exception] = queue.Queue(maxsize=4)

        def producer() -> None:
            async def fetch() -> None:
                try:
                    async for batch in self.stream_record_batches(table_id, version):
                        batch_queue.put(batch)
                    batch_queue.put(None)  # Signal completion
                except Exception as e:
                    batch_queue.put(e)

            asyncio.run(fetch())

        thread = threading.Thread(target=producer, daemon=True)
        thread.start()

        while True:
            item = batch_queue.get()
            if item is None:
                break
            if isinstance(item, Exception):
                raise item
            yield item

        thread.join()

    def stream_to_dataframes(
        self,
        table_id: str,
        version: int | str | None = None,
        engine: Literal["pandas", "polars"] = "pandas",
    ) -> Iterator[Any]:
        """Stream data as individual DataFrames per batch.

        Useful for processing large tables in chunks without loading
        everything into memory.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            engine: Data processing engine ('pandas' or 'polars').

        Yields:
            DataFrame in the specified format (pandas or polars).

        Raises:
            ValueError: If engine is not supported.
            StreamingError: For streaming errors.
            TableConversionError: If conversion to DataFrame fails.
        """
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        for batch in self.iter_record_batches(table_id, version):
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
    ) -> pd.DataFrame | pl.DataFrame:
        """Fetch table data synchronously and convert to DataFrame.

        Args:
            table_id: Unique identifier for the table.
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable').
            engine: Data processing engine ('pandas' or 'polars').

        Returns:
            DataFrame in the specified format (pandas or polars).

        Raises:
            TypeError: If version has wrong type.
            ValueError: If engine is not supported.
            TableAPIError: For API-related errors.
            TableConversionError: If conversion to DataFrame fails.
        """
        # Validate input parameters
        if version is not None and not isinstance(version, int | str):
            raise TypeError(
                f"Expected 'version' to be int, str, or None, got {type(version).__name__}"
            )
        if engine not in ("pandas", "polars"):
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")

        async def get_dataframe_async() -> pd.DataFrame | pl.DataFrame:
            """Internal async function to fetch and convert data."""
            data_stream = await self.get_data_stream(table_id, version)

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

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(get_dataframe_async())

        if loop.is_running():
            try:
                import nest_asyncio

                nest_asyncio.apply()
                return loop.run_until_complete(get_dataframe_async())
            except ImportError as e:
                raise RuntimeError(
                    "nest_asyncio is required when calling from within an async context "
                    "(e.g., Jupyter notebooks). Install with: pip install nest-asyncio"
                ) from e
        else:
            return loop.run_until_complete(get_dataframe_async())

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
        import requests

        if not table_id:
            raise ValueError("Table ID is required")

        params: dict[str, Any] = {
            "table_id": table_id,
            "scope": "read",
            "version_alias": version if isinstance(version, str) else "latest",
        }
        if isinstance(version, int):
            params["version"] = version

        response = requests.get(
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
