"""Tests for the MyCorr client."""

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
from aioresponses import aioresponses

from pymycorr import MyCorr, TableAPIError


class TestMyCorrrInit:
    """Tests for MyCorr initialization."""

    def test_init_with_explicit_params(self) -> None:
        """Test initialization with explicit url and token."""
        client = MyCorr(url="https://api.example.com", token="my-token")
        assert client.url == "https://api.example.com"
        assert client.token == "my-token"

    def test_init_from_env_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test initialization from environment variables."""
        monkeypatch.setenv("MYCORR_API_URL", "https://env.example.com")
        monkeypatch.setenv("MYCORR_API_TOKEN", "env-token")

        client = MyCorr()
        assert client.url == "https://env.example.com"
        assert client.token == "env-token"

    def test_init_with_default_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test initialization uses default URL when not specified."""
        monkeypatch.delenv("MYCORR_API_URL", raising=False)
        monkeypatch.setenv("MYCORR_API_TOKEN", "my-token")

        client = MyCorr()
        assert client.url == "https://api.mycorr.recons-ltd.com"
        assert client.token == "my-token"

    def test_init_strips_trailing_slash(self) -> None:
        """Test that trailing slash is stripped from URL."""
        client = MyCorr(url="https://api.example.com/", token="my-token")
        assert client.url == "https://api.example.com"

    def test_init_explicit_overrides_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that explicit params override environment variables."""
        monkeypatch.setenv("MYCORR_API_URL", "https://env.example.com")
        monkeypatch.setenv("MYCORR_API_TOKEN", "env-token")

        client = MyCorr(url="https://explicit.example.com", token="explicit-token")
        assert client.url == "https://explicit.example.com"
        assert client.token == "explicit-token"

    def test_init_from_env_file(self, tmp_path: Path) -> None:
        """Test initialization from custom .env file."""
        env_file = tmp_path / ".env"
        env_file.write_text("MYCORR_API_URL=https://file.example.com\nMYCORR_API_TOKEN=file-token")

        client = MyCorr(env_file=env_file)
        assert client.url == "https://file.example.com"
        assert client.token == "file-token"

    def test_init_missing_token_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that missing token raises ValueError."""
        monkeypatch.delenv("MYCORR_API_URL", raising=False)
        monkeypatch.delenv("MYCORR_API_TOKEN", raising=False)

        with pytest.raises(ValueError, match="token is required"):
            MyCorr(url="https://api.example.com")

    def test_init_empty_string_params_fall_back_to_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that empty string params don't override env vars."""
        monkeypatch.setenv("MYCORR_API_URL", "https://env.example.com")
        monkeypatch.setenv("MYCORR_API_TOKEN", "env-token")

        client = MyCorr(url="", token="")
        assert client.url == "https://env.example.com"
        assert client.token == "env-token"


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
            pattern = re.compile(r"^https://test\.example\.com/api/data/table/stream\?.*$")
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
            pattern = re.compile(r"^https://test\.example\.com/api/data/table/stream\?.*$")
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
            pattern = re.compile(r"^https://test\.example\.com/api/data/table/stream\?.*$")
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
            pattern = re.compile(r"^https://test\.example\.com/api/data/table/stream\?.*$")
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
            pattern = re.compile(r"^https://test\.example\.com/api/data/table/stream\?.*$")
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
