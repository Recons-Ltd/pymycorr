"""MyCorr API client for fetching table data with Arrow format support."""

from __future__ import annotations

import asyncio
import io
import json
import os
import queue
import threading
import warnings
from collections.abc import AsyncIterator, Coroutine, Iterator, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypeVar, cast
from urllib.parse import quote, urlparse

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from pymycorr._progress import ProgressTracker
from pymycorr.exceptions import (
    AuthenticationError,
    InvalidDataError,
    MyCorrDataWarning,
    PermissionDeniedError,
    QuotaExceededError,
    RateLimitError,
    StorageQuotaExceededError,
    StreamingError,
    TableAPIError,
    TableConversionError,
    TableNotFoundError,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

T = TypeVar("T")

#: Target size of one uploaded record batch — well under the server's cap
#: (64 MiB by default), so a batch holds its size after IPC framing.
DEFAULT_BATCH_BYTES = 16 * 1024 * 1024

#: The Arrow schema metadata key an upload carries its table description under.
TABLE_DESCRIPTION_METADATA_KEY = "mycorr.description"

#: Longest table description the server stores.
MAX_DESCRIPTION_BYTES = 10_000


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

        Error bodies come in three shapes, all read here: ``{error: <code>,
        error_description}`` (the upload door), ``{error: <code>, message,
        ...figures}`` (quota refusals) and ``{status, error: <message>}``
        (data-service reads).

        Raises:
            AuthenticationError: If the status code is 401.
            PermissionDeniedError: If the status code is 403.
            TableNotFoundError: If the status code is 404.
            StorageQuotaExceededError: If the status code is 413 for storage.
            InvalidDataError: If the status code is 400, 422, or 413 for size.
            RateLimitError: If the status code is 429 from a rate limiter.
            QuotaExceededError: If the status code is 429 for egress.
            TableAPIError: For all other non-2xx status codes.
        """
        try:
            error_body = json.loads(body) if isinstance(body, bytes | str) else {}
        except Exception:
            error_body = {}
        if not isinstance(error_body, dict):
            error_body = {}

        raw_code = error_body.get("error")
        code = raw_code if isinstance(raw_code, str) else None
        msg = (
            error_body.get("error_description")
            or error_body.get("message")
            or code
            or f"HTTP {status_code}"
        )
        # A data-service body puts its human message in `error`; only a
        # snake_case word there is a code a caller could branch on.
        if code is not None and not code.replace("_", "").isalnum():
            code = None
        text = f"{context}{msg}"

        if status_code == 401:
            raise AuthenticationError(text, status_code=401, code=code)
        if status_code == 403:
            raise PermissionDeniedError(text, status_code=403, code=code)
        if status_code == 404:
            raise TableNotFoundError(text, status_code=404, code=code)
        if status_code == 413 and code != "upload_too_large":
            raise StorageQuotaExceededError(error_body)
        if status_code in (400, 413, 422):
            raise InvalidDataError(text, status_code=status_code, code=code)
        if status_code == 429:
            if code in ("rate_limit_exceeded", "too_many_concurrent_uploads"):
                raise RateLimitError(error_body)
            raise QuotaExceededError(error_body)

        raise TableAPIError(text, status_code=status_code, code=code)

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
            Dictionary with ``table_id``, ``name``, ``active_rows``,
            ``created_at``, ``schema_last_modified``, ``columns`` and
            ``scheduled_for_deletion``: ``None`` for a live table, or the Unix
            time (seconds) at which a table in the trash is permanently
            deleted. Servers older than this field omit it, so read it with
            ``info.get("scheduled_for_deletion")``.

        Raises:
            ValueError: If table_id is empty.
            TypeError: If version has an invalid type.
            TableNotFoundError: If the table is not found (404).
            QuotaExceededError: If the egress quota is exceeded (429).
            TableAPIError: For other API-related errors.
        """
        if not table_id:
            raise ValueError("Table ID is required")

        params = self._build_version_params(table_id, version)

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
    def _is_frame(value: Any) -> bool:
        """Whether ``value`` is one table (rather than an iterable of them)."""
        if isinstance(value, pa.Table | pa.RecordBatch):
            return True
        cls = type(value)
        return cls.__module__.startswith(("pandas", "polars")) and cls.__name__ in (
            "DataFrame",
            "LazyFrame",
        )

    @staticmethod
    def _rows_per_batch(nbytes: int, num_rows: int, batch_bytes: int) -> int:
        """Rows that fit in about ``batch_bytes``, never fewer than one."""
        if num_rows <= 0 or nbytes <= 0:
            return max(num_rows, 1)
        return max(1, (batch_bytes * num_rows) // nbytes)

    @classmethod
    def _frame_batches(
        cls,
        frame: Any,
        batch_bytes: int,
        schema: pa.Schema | None = None,
    ) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        """One table as record batches of about ``batch_bytes`` each, cast to
        ``schema`` when one is given.

        pandas is converted slice by slice, so a large frame is never copied to
        Arrow whole: pandas → Arrow would otherwise hand the entire frame over as
        ONE batch, which the server refuses above its batch cap.
        """
        module = type(frame).__module__
        if isinstance(frame, pa.RecordBatch):
            table = pa.Table.from_batches([frame])
        elif isinstance(frame, pa.Table):
            table = frame
        elif module.startswith("polars"):
            if not hasattr(frame, "to_arrow"):
                raise TypeError(
                    "a polars LazyFrame must be collected first: pass lf.collect(), "
                    "or an iterator of DataFrames for data larger than memory"
                )
            table = frame.to_arrow()
        elif module.startswith("pandas"):
            return cls._pandas_batches(frame, batch_bytes, schema)
        else:
            raise TypeError(
                "data must be a pandas/polars DataFrame, a pyarrow Table/RecordBatch/"
                f"RecordBatchReader, or an iterable of those; got {type(frame).__name__}"
            )
        if schema is not None and not table.schema.equals(schema):
            table = table.cast(schema)
        rows = cls._rows_per_batch(table.nbytes, table.num_rows, batch_bytes)
        return table.schema, iter(table.to_batches(max_chunksize=rows))

    @classmethod
    def _pandas_batches(
        cls,
        df: pd.DataFrame,
        batch_bytes: int,
        schema: pa.Schema | None,
    ) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
        if schema is None:
            # From the whole frame, so an object column whose first slice is all
            # nulls still gets its real type.
            schema = pa.Schema.from_pandas(df, preserve_index=False)
        target = schema
        n = len(df)

        def convert(part: pd.DataFrame) -> list[pa.RecordBatch]:
            table = pa.Table.from_pandas(part, schema=target, preserve_index=False)
            converted: list[pa.RecordBatch] = table.to_batches()
            return converted

        def batches() -> Iterator[pa.RecordBatch]:
            if n == 0:
                yield from convert(df)
                return
            per_row = max(1, int(df.memory_usage(deep=True, index=False).sum()) // n)
            rows = max(1, batch_bytes // per_row)
            for start in range(0, n, rows):
                yield from convert(df.iloc[start : start + rows])

        return target, batches()

    @classmethod
    def _upload_batches(
        cls,
        data: Any,
        batch_bytes: int,
    ) -> tuple[pa.Schema, Iterator[pa.RecordBatch], int | None]:
        """``data`` as ``(schema, bounded batches, in-memory size)``.

        The size is the Arrow buffers' ``nbytes`` when the whole input is at
        hand — a floor for the IPC stream, so an upload whose data is already
        larger than the server's cap can be refused before it starts. ``None``
        for a reader or iterable, whose size is not known up front.
        """
        if isinstance(data, pa.RecordBatchReader):
            reader = data
            schema = reader.schema

            def from_reader() -> Iterator[pa.RecordBatch]:
                for batch in reader:
                    yield from cls._frame_batches(batch, batch_bytes, schema)[1]

            return schema, from_reader(), None

        if cls._is_frame(data):
            schema, batches = cls._frame_batches(data, batch_bytes)
            nbytes = data.nbytes if isinstance(data, pa.Table | pa.RecordBatch) else None
            return schema, batches, nbytes

        if isinstance(data, str | bytes | dict) or not hasattr(data, "__iter__"):
            raise TypeError(
                "data must be a pandas/polars DataFrame, a pyarrow Table/RecordBatch/"
                f"RecordBatchReader, or an iterable of those; got {type(data).__name__}"
            )
        items = iter(data)
        try:
            first = next(items)
        except StopIteration:
            raise ValueError("data is an empty iterable: pass at least one table") from None
        schema, first_batches = cls._frame_batches(first, batch_bytes)

        def from_iterable() -> Iterator[pa.RecordBatch]:
            yield from first_batches
            for position, item in enumerate(items, start=2):
                try:
                    _, batches = cls._frame_batches(item, batch_bytes, schema)
                    yield from batches
                except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as e:
                    raise ValueError(
                        f"table {position} of the iterable does not match the first "
                        f"table's schema: {e}"
                    ) from e

        return schema, from_iterable(), None

    @staticmethod
    def _ipc_stream(
        schema: pa.Schema,
        batches: Iterator[pa.RecordBatch],
        tracker: ProgressTracker,
    ) -> Iterator[bytes]:
        """An Arrow IPC stream, one chunk per record batch — the request body,
        produced as it is sent so no more than one batch is held at a time."""
        sink = io.BytesIO()

        def drain() -> bytes:
            data = sink.getvalue()
            sink.seek(0)
            sink.truncate(0)
            return data

        with ipc.new_stream(sink, schema) as writer:
            for batch in batches:
                writer.write_batch(batch)
                chunk = drain()
                tracker.update(len(chunk))
                # An empty chunk would end a chunked body early.
                if chunk:
                    yield chunk
        tail = drain()
        if tail:
            tracker.update(len(tail))
            yield tail

    def create_table(
        self,
        model_id: str,
        data: Any,
        *,
        name: str,
        primary_key: str | Sequence[str] | None = None,
        description: str | None = None,
        batch_bytes: int = DEFAULT_BATCH_BYTES,
        progress: bool | Literal["auto"] | None = None,
        timeout: float | httpx.Timeout | None = None,
    ) -> dict[str, Any]:
        """Create a new table in a model by streaming tabular data to MyCorr.

        The data is sent as an Arrow IPC stream in record batches of about
        ``batch_bytes`` each, produced while the request is sent — the whole
        upload is never held in memory. The server converts each column to the
        nearest MyCorr type (``large_string``, ``large_list``, categoricals,
        timezone-aware timestamps and the like need no preparation); a
        conversion that loses information is reported as a
        :class:`MyCorrDataWarning`.

        Requires a write-scoped token bound to the model's organization, edit
        access to the model and membership of its organization (a catalog model
        takes the catalog-manager role instead). The upload is checked before
        any data is sent, and is refused if it does not fit in the
        organization's remaining storage. Not retried automatically: creating a
        table is not idempotent.

        Args:
            model_id: The model the table is created in.
            data: A pandas or polars DataFrame, a pyarrow ``Table``,
                ``RecordBatch`` or ``RecordBatchReader``, or an iterable of
                DataFrames/Tables/RecordBatches sharing one schema — for data
                larger than memory.
            name: Name for the new table.
            primary_key: Primary-key column name(s) — a single name or a
                sequence for a composite key. Marking a key here is what lets
                the table be diff-synced later.
            description: Optional table description, shown in the catalog and
                the table details panel — at most 10,000 bytes. Sent inside the
                upload (as the Arrow schema metadata key ``mycorr.description``),
                never in the URL.
            batch_bytes: Target size of each record batch. The server refuses
                batches over its own cap (64 MiB by default).
            progress: Show upload progress. None uses the client default.
            timeout: Request timeout; by default 30s to connect, 300s between
                writes and 1800s for the server to finish after the last byte.

        Returns:
            Dict with ``model_id``, ``table_id``, ``description`` and
            ``warnings``. A success means the table exists with its
            description; if saving it fails, the server removes the table.

        Raises:
            ValueError: If an argument is empty or invalid, or a table in an
                iterable does not match the first one's schema.
            TypeError: If data is not a supported type.
            AuthenticationError: If the token is invalid or not bound to an
                organization (401).
            PermissionDeniedError: If the token may not create tables in this
                model (403) — see its ``code``.
            StorageQuotaExceededError: If the upload does not fit in the
                organization's storage (413).
            InvalidDataError: If the server refuses the data (400/413/422) —
                see its ``code``.
            RateLimitError: If too many uploads are running or have started
                this hour (429); ``retry_after`` says when to try again.
            TableAPIError: For other API errors.
        """
        if not model_id:
            raise ValueError("model_id is required")
        if not name or not name.strip():
            raise ValueError("name is required")
        if batch_bytes <= 0:
            raise ValueError("batch_bytes must be positive")
        description = description.strip() if description else None
        if description and len(description.encode()) > MAX_DESCRIPTION_BYTES:
            raise ValueError(
                f"description is {len(description.encode())} bytes; at most "
                f"{MAX_DESCRIPTION_BYTES} are allowed"
            )

        if primary_key is None:
            pk_cols: list[str] = []
        elif isinstance(primary_key, str):
            pk_cols = [primary_key]
        else:
            pk_cols = list(primary_key)

        params: dict[str, Any] = {"name": name}
        if pk_cols:
            params["pk"] = ",".join(pk_cols)

        # Before any request, so unsupported input fails without a round-trip.
        schema, batches, known_bytes = self._upload_batches(data, batch_bytes)
        if description:
            # In the body, not the URL: the server reads it off the schema message.
            metadata = dict(schema.metadata or {})
            metadata[TABLE_DESCRIPTION_METADATA_KEY.encode()] = description.encode()
            schema = schema.with_metadata(metadata)

        url = f"{self.url}/server/api/model/{quote(model_id, safe='')}/tables"
        headers = {"Authorization": f"Bearer {self.token}"}

        # Ask first: a body is sent whole before its answer is read, so a refused
        # upload would otherwise learn why only after sending all of it.
        check = httpx.post(
            url,
            headers=headers,
            params={**params, "dry_run": "true"},
            verify=self._verify_ssl,
            timeout=httpx.Timeout(timeout=60.0, connect=30.0),
        )
        if check.status_code != 200:
            self._handle_error_response(check.status_code, check.text, "Error creating table: ")
        grant = check.json()

        max_bytes = grant.get("max_bytes")
        if known_bytes is not None and isinstance(max_bytes, int) and known_bytes > max_bytes:
            detail = {
                "error": "storage_quota_exceeded"
                if grant.get("limit_kind") == "storage_quota"
                else "upload_too_large",
                "message": f"this upload is at least {known_bytes} bytes; at most "
                f"{max_bytes} may be sent",
                "requested_bytes": known_bytes,
            }
            self._handle_error_response(413, json.dumps(detail), "Error creating table: ")

        effective_progress: bool | Literal["auto"] = (
            progress if progress is not None else self._default_progress
        )
        tracker = ProgressTracker(effective_progress, desc=f"Uploading {name}")
        failure: list[BaseException] = []

        def body() -> Iterator[bytes]:
            try:
                yield from self._ipc_stream(schema, batches, tracker)
            except BaseException as e:
                failure.append(e)
                raise

        try:
            with tracker:
                response = httpx.post(
                    url,
                    headers={**headers, "Content-Type": "application/vnd.apache.arrow.stream"},
                    params=params,
                    content=body(),
                    verify=self._verify_ssl,
                    timeout=timeout
                    if timeout is not None
                    else httpx.Timeout(connect=30.0, read=1800.0, write=300.0, pool=30.0),
                )
        except Exception as e:
            # The data failed while being streamed: that is the error to report,
            # not the aborted request it caused.
            if failure:
                raise failure[0] from None
            if isinstance(e, httpx.HTTPError):
                raise TableAPIError(f"Error creating table: upload failed: {e}") from e
            raise

        if response.status_code not in (200, 201):
            self._handle_error_response(
                response.status_code, response.text, "Error creating table: "
            )
        tracker.print_summary()

        result = cast(dict[str, Any], response.json())
        for w in result.get("warnings") or []:
            if isinstance(w, dict):
                warnings.warn(
                    f"column {w.get('column_name')!r} was converted from "
                    f"{w.get('original_arrow_type')} to {w.get('normalized_arrow_type')}: "
                    f"{w.get('reason')}",
                    MyCorrDataWarning,
                    stacklevel=2,
                )
        return result
