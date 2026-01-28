"""Tests for the MyCorr client."""

import re

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from aioresponses import aioresponses

from pymycorr import MyCorr, TableAPIError


class TestMyCorrrInit:
    """Tests for MyCorr initialization."""

    def test_init_valid(self) -> None:
        """Test valid initialization."""
        client = MyCorr(url="https://api.example.com", token="my-token")
        assert client.url == "https://api.example.com"
        assert client.token == "my-token"

    def test_init_empty_token_raises(self) -> None:
        """Test that empty token raises ValueError."""
        with pytest.raises(ValueError, match="Authentication token is required"):
            MyCorr(url="https://api.example.com", token="")

    def test_init_empty_url_raises(self) -> None:
        """Test that empty URL raises ValueError."""
        with pytest.raises(ValueError, match="API URL is required"):
            MyCorr(url="", token="my-token")


class TestGetTable:
    """Tests for get_table method."""

    def test_invalid_engine_raises(self, client: MyCorr) -> None:
        """Test that invalid engine raises ValueError."""
        with pytest.raises(ValueError, match="Engine must be 'pandas' or 'polars'"):
            client.get_table("table-id", engine="invalid")  # type: ignore

    def test_invalid_version_type_raises(self, client: MyCorr) -> None:
        """Test that invalid version type raises TypeError."""
        with pytest.raises(TypeError, match="Expected 'version' to be int, str, or None"):
            client.get_table("table-id", version=1.5)  # type: ignore


class TestGetDataStream:
    """Tests for get_data_stream method."""

    @pytest.mark.asyncio
    async def test_empty_table_id_raises(self, client: MyCorr) -> None:
        """Test that empty table_id raises ValueError."""
        with pytest.raises(ValueError, match="Table ID is required"):
            await client.get_data_stream("")

    @pytest.mark.asyncio
    async def test_invalid_version_type_raises(self, client: MyCorr) -> None:
        """Test that invalid version type raises TypeError."""
        with pytest.raises(TypeError, match="Expected 'version' to be int, str, or None"):
            await client.get_data_stream("table-id", version=1.5)  # type: ignore

    @pytest.mark.asyncio
    async def test_successful_fetch(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test successful data fetch."""
        # Serialize arrow table to bytes
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        with aioresponses() as m:
            # Use pattern to match URL with any query params
            pattern = re.compile(r"^https://test\.example\.com/api/stream\?.*$")
            m.get(
                pattern,
                body=arrow_bytes,
                status=200,
            )

            result = await client.get_data_stream("test-table")

            assert result.num_rows == 3
            assert result.column_names == ["id", "name", "value"]

    @pytest.mark.asyncio
    async def test_api_error_response(self, client: MyCorr) -> None:
        """Test handling of API error response."""
        with aioresponses() as m:
            pattern = re.compile(r"^https://test\.example\.com/api/stream\?.*$")
            m.get(
                pattern,
                payload={"message": "Table not found"},
                status=404,
            )

            with pytest.raises(TableAPIError, match="Table not found"):
                await client.get_data_stream("nonexistent-table")

    @pytest.mark.asyncio
    async def test_api_error_no_json(self, client: MyCorr) -> None:
        """Test handling of API error without JSON body."""
        with aioresponses() as m:
            pattern = re.compile(r"^https://test\.example\.com/api/stream\?.*$")
            m.get(
                pattern,
                body="Internal Server Error",
                status=500,
            )

            with pytest.raises(TableAPIError, match="HTTP 500"):
                await client.get_data_stream("test-table")

    @pytest.mark.asyncio
    async def test_version_int_param(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test that integer version is passed as 'version' param."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        with aioresponses() as m:
            pattern = re.compile(r"^https://test\.example\.com/api/stream\?.*$")
            m.get(
                pattern,
                body=arrow_bytes,
                status=200,
            )

            await client.get_data_stream("test-table", version=2)

            # Verify the request was made with correct params
            request = list(m.requests.values())[0][0]
            params = request.kwargs.get("params", {})
            assert params.get("version") == 2

    @pytest.mark.asyncio
    async def test_version_string_param(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test that string version is passed as 'version_alias' param."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        with aioresponses() as m:
            pattern = re.compile(r"^https://test\.example\.com/api/stream\?.*$")
            m.get(
                pattern,
                body=arrow_bytes,
                status=200,
            )

            await client.get_data_stream("test-table", version="stable")

            # Verify the request was made with correct params
            request = list(m.requests.values())[0][0]
            params = request.kwargs.get("params", {})
            assert params.get("version_alias") == "stable"


class TestGetTableInfo:
    """Tests for get_table_info method."""

    def test_get_table_info_structure(
        self,
        client: MyCorr,
        sample_arrow_table: pa.Table,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test that get_table_info returns correct structure."""

        async def mock_get_data_stream(table_id: str, version: int | str | None = None) -> pa.Table:
            return sample_arrow_table

        monkeypatch.setattr(client, "get_data_stream", mock_get_data_stream)

        info = client.get_table_info("test-table")

        assert info["table_id"] == "test-table"
        assert info["num_rows"] == 3
        assert info["num_columns"] == 3
        assert info["column_names"] == ["id", "name", "value"]
        assert len(info["column_types"]) == 3
