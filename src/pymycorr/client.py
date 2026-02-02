"""MyCorr API client for fetching table data with Arrow format support."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import aiohttp
import pyarrow as pa
import pyarrow.ipc as ipc
from dotenv import load_dotenv

from pymycorr.exceptions import TableAPIError, TableConversionError

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
        if not table_id:
            raise ValueError("Table ID is required")

        # Set default version if none provided
        if version is None:
            version = "latest"

        params: dict[str, Any] = {"table_id": table_id, "scope": "read"}

        # Use isinstance to determine if version is int or str
        if isinstance(version, int):
            params["version"] = version
        elif isinstance(version, str):
            params["version_alias"] = version
        else:
            raise TypeError(
                f"Expected 'version' to be int, str, or None, got {type(version).__name__}"
            )

        timeout = aiohttp.ClientTimeout(total=300)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.get(
                f"{self.url}/data/table/stream",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.apache.arrow.stream",
                },
                params=params,
            ) as response,
        ):
            if response.status != 200:
                try:
                    error = await response.json()
                    error_msg = error.get("message", "Internal server error")
                except Exception:
                    error_msg = f"HTTP {response.status}"
                raise TableAPIError(f"Error fetching table: {error_msg}")

            try:
                stream = await response.read()
                return ipc.open_stream(stream).read_all()
            except (pa.ArrowInvalid, pa.ArrowIOError) as e:
                raise TableAPIError(f"Arrow parsing error: {e!s}") from e

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
                    return data_stream.to_pandas()
                else:
                    import polars as pl_module

                    return pl_module.from_arrow(data_stream)
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
            Dictionary containing table information.

        Raises:
            TableAPIError: If fetching table info fails.
        """

        async def get_info_async() -> dict[str, Any]:
            data_stream = await self.get_data_stream(table_id, version)
            return {
                "table_id": table_id,
                "schema": str(data_stream.schema),
                "num_columns": data_stream.num_columns,
                "num_rows": len(data_stream),
                "column_names": data_stream.column_names,
                "column_types": [str(field.type) for field in data_stream.schema],
            }

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(get_info_async())

        if loop.is_running():
            try:
                import nest_asyncio

                nest_asyncio.apply()
                return loop.run_until_complete(get_info_async())
            except ImportError as e:
                raise RuntimeError(
                    "nest_asyncio is required when calling from within an async context "
                    "(e.g., Jupyter notebooks). Install with: pip install nest-asyncio"
                ) from e
        else:
            return loop.run_until_complete(get_info_async())
