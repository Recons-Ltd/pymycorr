import os
import asyncio
from typing import Optional, Union

import aiohttp
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc
from IPython.display import HTML, display


class TableAPIError(Exception):
    """Base exception for table API errors"""
    pass


class TableNotFoundError(TableAPIError):
    """Raised when table is not found"""
    pass


class TableConversionError(TableAPIError):
    """Raised when table conversion fails"""
    pass


class MyCorr:
    """Client for fetching table data from API with Arrow format support"""
    
    def __init__(self, url: str, token: str):
        """Initialize the client with authentication token and API URL
        
        Args:
            token: Authentication token for API access
            url: API base URL
            
        Raises:
            ValueError: If token or url is empty
        """
        if not token:
            raise ValueError("Authentication token is required")
        
        if not url:
            raise ValueError("API URL is required")
        
        self.url = url
        self.token = token

    async def get_data_stream(
        self, 
        table_id: str, 
        version: Optional[Union[int, str]] = None
    ) -> pa.Table:
        """Fetch Arrow stream from API asynchronously
        
        Args:
            table_id: Unique identifier for the table
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable')
            
        Returns:
            PyArrow Table containing the data
            
        Raises:
            ValueError: If table_id is empty
            TableNotFoundError: If table is not found (404)
            TableAPIError: For other API or parsing errors
        """
        if not table_id:
            raise ValueError("Table ID is required")
        
        # Set default version if none provided
        if version is None:
            version = 'latest'
        
        params = {"table_id": table_id, "scope": "read"}
        
        # Use isinstance to determine if version is int or str
        if isinstance(version, int):
            params["version"] = version
        elif isinstance(version, str):
            params["version_alias"] = version
        else:
            raise TypeError(f"Expected 'version' to be int, str, or None, got {type(version).__name__}")

        timeout = aiohttp.ClientTimeout(total=300) 
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                f"{self.url}/stream",
                headers={
                    "Authorization": f"Bearer {self.token}", 
                    "Accept": "application/vnd.apache.arrow.stream"
                },
                params=params
            ) as response:
                if response.status != 200:
                    try:
                        error = await response.json()
                        error_msg = error.get('message', 'Internal server error')
                    except Exception:
                        error_msg = f"HTTP {response.status}"
                    raise TableAPIError(f"Error fetching table: {error_msg}")
                
                try:
                    stream = await response.read()
                    return ipc.open_stream(stream).read_all()
                except (pa.ArrowInvalid, pa.ArrowIOError) as e:
                    raise TableAPIError(f"Arrow parsing error: {str(e)}")

    def get_table(
        self, 
        table_id: str, 
        version: Optional[Union[int, str]] = None,
        engine: str = "pandas"
    ) -> Union[pd.DataFrame, pl.DataFrame]:
        """Fetch table data synchronously and convert to DataFrame
        
        Args:
            table_id: Unique identifier for the table
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable')
            engine: Data processing engine ('pandas' or 'polars')
            
        Returns:
            DataFrame in the specified format (pandas or polars)
            
        Raises:
            TypeError: If version has wrong type
            ValueError: If engine is not supported
            TableAPIError: For API-related errors
            TableConversionError: If conversion to DataFrame fails
        """
        # Validate input parameters
        if version is not None and not isinstance(version, (int, str)):
            raise TypeError(f"Expected 'version' to be int, str, or None, got {type(version).__name__}")
        if engine not in ["pandas", "polars"]:
            raise ValueError(f"Engine must be 'pandas' or 'polars', got '{engine}'")
        
        async def get_dataframe_async():
            """Internal async function to fetch and convert data"""
            data_stream = await self.get_data_stream(table_id, version)
            
            try:
                if engine == "pandas":
                    if len(data_stream) == 0:
                        #here since pandas doesn't visualize empty frames print a message
                        print(
                            f"Table is empty with schema: {data_stream.schema}")
                    return data_stream.to_pandas()
                else: 
                    return pl.from_arrow(data_stream)
            except Exception as e:
                raise TableConversionError(f"Failed to convert table to {engine} format: {str(e)}")
        
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(get_dataframe_async())
        
        if loop.is_running():
            try:
                import nest_asyncio
                nest_asyncio.apply()
                return loop.run_until_complete(get_dataframe_async())
            except ImportError:
                raise RuntimeError(
                    "nest_asyncio is required when calling from within an async context. "
                    "Install with: pip install nest-asyncio"
                )
        else:
            return loop.run_until_complete(get_dataframe_async())

    def get_table_info(self, table_id: str, version: Optional[Union[int, str]] = None) -> dict:
        """Get table schema and metadata without fetching full data
        
        Args:
            table_id: Unique identifier for the table
            version: Version number (int) or version alias (str, e.g., 'latest', 'stable')
            
        Returns:
            Dictionary containing table information
        """
        try:
            data_stream = asyncio.run(self.get_data_stream(table_id, version))
            return {
                "table_id": table_id,
                "schema": str(data_stream.schema),
                "num_columns": data_stream.num_columns,
                "num_rows": len(data_stream),
                "column_names": data_stream.column_names,
                "column_types": [str(field.type) for field in data_stream.schema]
            }
        except Exception as e:
            raise TableAPIError(f"Failed to get table info: {str(e)}")