import os
import aiohttp
import pyarrow.ipc as ipc
import pyarrow as pa
import pandas as pd
import polars as pl
from dotenv import load_dotenv
from IPython.display import display, HTML
import asyncio

load_dotenv()

class MyCorr:
    def __init__(self, token: str):
        self.url = os.getenv("API_BASE_URL")
        self.token = token

    def display_message(self, message: str, color: str):
        display(HTML(f'<div style="color: {color}; font-weight: normal; font-size: 16px;">{message}</div>'))

    def display_error(self, error_message: str):
        self.display_message(error_message, "#d43b20")

    async def get_data_stream(self, table_id: str, version: int) -> pa.Table:
        """Fetch Arrow stream from API"""
        if not table_id or version is None:
            self.display_error("Table ID and version are required")
            return None

        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{self.url}/stream",
                headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.apache.arrow.stream"},
                params={"table_id": table_id, "version": version, "scope": "read"}
            ) as response:
                if response.status != 200:
                    error= await response.json()
                    self.display_error(f"Error fetching dataframe: {error.get("message", "Internal server error")}")
                    return None
                
                try:
                    stream = await response.read()
                    return ipc.open_stream(stream).read_all()
                except (pa.ArrowInvalid, pa.ArrowIOError) as e:
                    self.display_error(f"Arrow error: {str(e)}")
                    return None

    def get_dataframe(self, table_id: str, version: int, engine: str):
        """Fetches and returns dataframe synchronously by internally running async logic."""

        async def get_dataframe_async():
         data_stream = await self.get_data_stream(table_id, version)
         if not data_stream:
            return None
         try:
            return data_stream.to_pandas() if engine == "pandas" else pl.from_arrow(data_stream)
         except Exception as e:
            self.display_error(f"Conversion error: {str(e)}")
            return None

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(get_dataframe_async())
        else:
            return asyncio.run(get_dataframe_async())
