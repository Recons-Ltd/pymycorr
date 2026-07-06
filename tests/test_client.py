"""Tests for the MyCorr client."""

from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import respx

from pymycorr import (
    MyCorr,
    QuotaExceededError,
    RateLimitError,
    StreamingError,
    TableAPIError,
    TableNotFoundError,
)


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
        assert client.url == "https://space.mycorr.app"
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
    """Tests for _get_data_stream method."""

    @pytest.mark.asyncio
    async def test_empty_table_id_raises(self, client: MyCorr) -> None:
        """Test that empty table_id raises ValueError."""
        with pytest.raises(ValueError, match="Table ID is required"):
            await client._get_data_stream("")

    @pytest.mark.asyncio
    async def test_invalid_version_type_raises(self, client: MyCorr) -> None:
        """Test that invalid version type raises TypeError."""
        with pytest.raises(TypeError, match="Expected 'version' to be int, str, or None"):
            await client._get_data_stream("table-id", version=1.5)  # type: ignore

    @pytest.mark.asyncio
    @respx.mock
    async def test_successful_fetch(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test successful data fetch."""
        # Serialize arrow table to bytes
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        result = await client._get_data_stream("test-table")

        assert result.num_rows == 3
        assert result.column_names == ["id", "name", "value"]

    @pytest.mark.asyncio
    @respx.mock
    async def test_api_error_response(self, client: MyCorr) -> None:
        """Test handling of 404 API error response raises TableNotFoundError."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(404, json={"message": "Table not found"})

        with pytest.raises(TableNotFoundError, match="Table not found"):
            await client._get_data_stream("nonexistent-table")

    @pytest.mark.asyncio
    @respx.mock
    async def test_api_error_no_json(self, client: MyCorr) -> None:
        """Test handling of API error without JSON body."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(500, text="Internal Server Error")

        with pytest.raises(TableAPIError, match="HTTP 500"):
            await client._get_data_stream("test-table")

    @pytest.mark.asyncio
    @respx.mock
    async def test_version_int_param(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test that integer version is passed as 'version' param."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        await client._get_data_stream("test-table", version=2)

        # Verify the request was made with correct params
        assert route.called
        request = route.calls.last.request
        assert "version=2" in str(request.url)

    @pytest.mark.asyncio
    @respx.mock
    async def test_version_string_param(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Test that string version is passed as 'version_alias' param."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        await client._get_data_stream("test-table", version="stable")

        # Verify the request was made with correct params
        assert route.called
        request = route.calls.last.request
        assert "version_alias=stable" in str(request.url)


class TestGetTableInfo:
    """Tests for get_table_info method."""

    @respx.mock
    def test_get_table_info_structure(
        self,
        client: MyCorr,
    ) -> None:
        """Test that get_table_info returns correct structure."""
        mock_response = {
            "table_id": "test-table",
            "version": 1,
            "schema": {
                "fields": [
                    {"name": "id", "type": "int64"},
                    {"name": "name", "type": "string"},
                ]
            },
        }

        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(200, json=mock_response)

        info = client.get_table_info("test-table")

        assert info["table_id"] == "test-table"
        assert info["version"] == 1
        assert "schema" in info


class TestStreamingIntegration:
    """Integration tests for true network streaming."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_chunked_streaming_yields_batches(
        self, client: MyCorr, multi_batch_arrow_bytes: bytes
    ) -> None:
        """Verify batches yield from multi-batch response."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(
            200,
            content=multi_batch_arrow_bytes,
            headers={"Content-Type": "application/vnd.apache.arrow.stream"},
        )

        batches = []
        async for batch in client._stream_record_batches("test-table"):
            batches.append(batch)

        # 3 IPC streams, each containing 1 batch with 10 rows
        assert len(batches) == 3
        assert all(batch.num_rows == 10 for batch in batches)

    @pytest.mark.asyncio
    @respx.mock
    async def test_streaming_with_single_ipc_stream(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        """Test streaming works with single complete IPC stream."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=data)

        batches = []
        async for batch in client._stream_record_batches("test-table"):
            batches.append(batch)

        assert len(batches) == 1
        assert batches[0].num_rows == 3

    @pytest.mark.asyncio
    @respx.mock
    async def test_streaming_error_on_invalid_data(self, client: MyCorr) -> None:
        """Test that invalid IPC stream raises StreamingError."""
        # Send data that is not valid IPC format
        invalid_data = b"some random bytes that are not valid IPC"

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=invalid_data)

        with pytest.raises(StreamingError, match="Failed to parse IPC stream"):
            async for _ in client._stream_record_batches("test-table"):
                pass

    @pytest.mark.asyncio
    @respx.mock
    async def test_streaming_api_error(self, client: MyCorr) -> None:
        """Test API error handling in streaming mode."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(403, json={"message": "Access denied"})

        with pytest.raises(TableAPIError, match="Access denied"):
            async for _ in client._stream_record_batches("test-table"):
                pass


class TestSSLVerification:
    """Tests for SSL verification logic."""

    def test_ssl_enabled_for_remote_url(self) -> None:
        """Test SSL verification is enabled for remote URLs."""
        client = MyCorr(url="https://space.mycorr.app", token="test-token")
        assert client._verify_ssl is True

    def test_ssl_disabled_for_localhost(self) -> None:
        """Test SSL verification is disabled for localhost."""
        client = MyCorr(url="https://localhost:8443", token="test-token")
        assert client._verify_ssl is False

    def test_ssl_disabled_for_127_0_0_1(self) -> None:
        """Test SSL verification is disabled for 127.0.0.1."""
        client = MyCorr(url="https://127.0.0.1:8443", token="test-token")
        assert client._verify_ssl is False

    def test_ssl_not_disabled_by_localhost_in_path(self) -> None:
        """Test that localhost in URL path does not disable SSL."""
        client = MyCorr(url="https://evil.com/localhost/api", token="test-token")
        assert client._verify_ssl is True


class TestGetTableHappyPath:
    """Tests for get_table returning DataFrames."""

    @respx.mock
    def test_get_table_returns_pandas_dataframe(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        """Test get_table returns a pandas DataFrame."""
        import pandas as pd

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        df = client.get_table("test-table")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert list(df.columns) == ["id", "name", "value"]

    @respx.mock
    def test_get_table_returns_polars_dataframe(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        """Test get_table with polars engine returns a polars DataFrame."""
        import polars as pl

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        df = client.get_table("test-table", engine="polars")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3

    @respx.mock
    def test_get_table_empty_returns_empty_dataframe(
        self, client: MyCorr, empty_arrow_table: pa.Table
    ) -> None:
        """Test get_table returns empty DataFrame for empty table without printing."""
        import pandas as pd

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, empty_arrow_table.schema)
        writer.write_table(empty_arrow_table)
        writer.close()
        arrow_bytes = sink.getvalue().to_pybytes()

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=arrow_bytes)

        df = client.get_table("test-table")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0


class TestTableNotFoundError:
    """Tests for 404 handling raising TableNotFoundError."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_404_raises_table_not_found_in_stream(self, client: MyCorr) -> None:
        """Test 404 response raises TableNotFoundError in streaming."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(404, json={"message": "Table not found"})

        with pytest.raises(TableNotFoundError, match="Table not found"):
            async for _ in client._stream_record_batches("nonexistent"):
                pass

    @respx.mock
    def test_404_raises_table_not_found_in_info(self, client: MyCorr) -> None:
        """Test 404 response raises TableNotFoundError in get_table_info."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(404, json={"message": "Table not found"})

        with pytest.raises(TableNotFoundError, match="Table not found"):
            client.get_table_info("nonexistent")


class TestQuotaExceededError:
    """Tests for 429 handling raising QuotaExceededError."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_429_raises_quota_exceeded_in_stream(self, client: MyCorr) -> None:
        """Test 429 response raises QuotaExceededError in streaming."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(
            429, json={"error": "daily_egress_quota_exceeded", "message": "Daily limit reached"}
        )

        with pytest.raises(QuotaExceededError):
            async for _ in client._stream_record_batches("test-table"):
                pass

    @respx.mock
    def test_429_raises_quota_exceeded_in_info(self, client: MyCorr) -> None:
        """Test 429 response raises QuotaExceededError in get_table_info."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            429, json={"error": "daily_egress_quota_exceeded", "message": "Daily limit reached"}
        )

        with pytest.raises(QuotaExceededError):
            client.get_table_info("test-table")


class TestRateLimitError:
    """Tests for 429 rate limit handling raising RateLimitError."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_429_rate_limit_raises_in_stream(self, client: MyCorr) -> None:
        """Test 429 rate limit response raises RateLimitError in streaming."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(
            429,
            json={
                "error": "rate_limit_exceeded",
                "retry_after_secs": 5,
                "message": "Too many requests, retry in 5s",
            },
        )

        with pytest.raises(RateLimitError, match="Too many requests"):
            async for _ in client._stream_record_batches("test-table"):
                pass

    @respx.mock
    def test_429_rate_limit_raises_in_info(self, client: MyCorr) -> None:
        """Test 429 rate limit response raises RateLimitError in get_table_info."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            429,
            json={
                "error": "rate_limit_exceeded",
                "retry_after_secs": 5,
                "message": "Too many requests, retry in 5s",
            },
        )

        with pytest.raises(RateLimitError, match="Too many requests"):
            client.get_table_info("test-table")

    @respx.mock
    def test_rate_limit_retry_after_attribute(self, client: MyCorr) -> None:
        """Test RateLimitError exposes retry_after from retry_after_secs."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            429,
            json={
                "error": "rate_limit_exceeded",
                "retry_after_secs": 10,
                "message": "Too many requests",
            },
        )

        with pytest.raises(RateLimitError) as exc_info:
            client.get_table_info("test-table")

        assert exc_info.value.retry_after == 10

    @respx.mock
    def test_rate_limit_missing_retry_after(self, client: MyCorr) -> None:
        """Test RateLimitError.retry_after is None when retry_after_secs absent."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            429,
            json={"error": "rate_limit_exceeded", "message": "Too many requests"},
        )

        with pytest.raises(RateLimitError) as exc_info:
            client.get_table_info("test-table")

        assert exc_info.value.retry_after is None

    @respx.mock
    def test_rate_limit_detail_stored(self, client: MyCorr) -> None:
        """Test RateLimitError stores the full detail dict."""
        detail = {
            "error": "rate_limit_exceeded",
            "retry_after_secs": 5,
            "message": "Too many requests",
        }
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(429, json=detail)

        with pytest.raises(RateLimitError) as exc_info:
            client.get_table_info("test-table")

        assert exc_info.value.detail == detail

    @respx.mock
    def test_429_unknown_error_falls_to_quota(self, client: MyCorr) -> None:
        """Test 429 with unknown error field falls back to QuotaExceededError."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            429, json={"error": "something_unexpected", "message": "Unknown limit"}
        )

        with pytest.raises(QuotaExceededError):
            client.get_table_info("test-table")


class TestGetTableInfoErrors:
    """Tests for get_table_info error handling."""

    def test_empty_table_id_raises(self, client: MyCorr) -> None:
        """Test that empty table_id raises ValueError."""
        with pytest.raises(ValueError, match="Table ID is required"):
            client.get_table_info("")

    @respx.mock
    def test_non_json_error_response(self, client: MyCorr) -> None:
        """Test handling of non-JSON error response."""
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(500, text="Internal Server Error")

        with pytest.raises(TableAPIError, match="HTTP 500"):
            client.get_table_info("test-table")

    @respx.mock
    def test_version_int_param(self, client: MyCorr) -> None:
        """Test that integer version is passed correctly."""
        mock_response = {"table_id": "test-table", "version": 2}

        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(200, json=mock_response)

        client.get_table_info("test-table", version=2)

        assert route.called
        request = route.calls.last.request
        assert "version=2" in str(request.url)

    @respx.mock
    def test_version_string_param(self, client: MyCorr) -> None:
        """Test that string version is passed as version_alias."""
        mock_response = {"table_id": "test-table", "version": 1}

        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(200, json=mock_response)

        client.get_table_info("test-table", version="stable")

        assert route.called
        request = route.calls.last.request
        assert "version_alias=stable" in str(request.url)


class TestConnectionErrors:
    """Tests for network connection error handling."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_connection_error_raises_streaming_error(self, client: MyCorr) -> None:
        """Test that connection errors are wrapped in StreamingError."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.side_effect = httpx.ConnectError("Connection refused")

        with pytest.raises(StreamingError, match="Connection error"):
            async for _ in client._stream_record_batches("test-table"):
                pass


class TestCreateTable:
    """Tests for create_table."""

    _URL = "https://test.example.com/api/server/api/datasets/tables"

    @respx.mock
    def test_create_from_arrow_table(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """A pyarrow Table is uploaded as an IPC stream with the right params."""
        route = respx.post(url__startswith=self._URL)
        route.return_value = httpx.Response(200, json={"model_id": "mod-1", "table_id": "tab-1"})

        result = client.create_table("mod-1", sample_arrow_table, name="People", primary_key="id")

        assert result == {"model_id": "mod-1", "table_id": "tab-1"}
        request = route.calls.last.request
        assert request.headers["Content-Type"] == "application/vnd.apache.arrow.stream"
        assert request.headers["Authorization"] == "Bearer test-token"
        url = str(request.url)
        assert "name=People" in url
        assert "model=mod-1" in url
        assert "pk=id" in url
        # Body is a valid Arrow IPC stream round-tripping to the same rows.
        sent = ipc.open_stream(request.content).read_all()
        assert sent.num_rows == sample_arrow_table.num_rows

    @respx.mock
    def test_composite_primary_key_joined(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        """A sequence primary key is sent comma-joined."""
        route = respx.post(url__startswith=self._URL)
        route.return_value = httpx.Response(200, json={"model_id": "m", "table_id": "t"})

        client.create_table("m", sample_arrow_table, name="T", primary_key=["id", "name"])

        assert "pk=id%2Cname" in str(route.calls.last.request.url)

    @respx.mock
    def test_large_utf8_downcast_to_utf8(self, client: MyCorr) -> None:
        """LargeUtf8 columns are downcast to plain Utf8 before upload."""
        wide = pa.table({"id": pa.array([1], pa.int64()), "s": pa.array(["x"], pa.large_utf8())})
        route = respx.post(url__startswith=self._URL)
        route.return_value = httpx.Response(200, json={"model_id": "m", "table_id": "t"})

        client.create_table("m", wide, name="T", primary_key="id")

        sent = ipc.open_stream(route.calls.last.request.content).read_all()
        assert sent.schema.field("s").type == pa.string()

    @respx.mock
    def test_error_response_raises(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """Non-200 responses raise through the shared error handler."""
        route = respx.post(url__startswith=self._URL)
        route.return_value = httpx.Response(404, json={"message": "model not found"})

        with pytest.raises(TableNotFoundError):
            client.create_table("nope", sample_arrow_table, name="T", primary_key="id")

    def test_empty_model_id_raises(self, client: MyCorr, sample_arrow_table: pa.Table) -> None:
        """An empty model_id is rejected before any request."""
        with pytest.raises(ValueError, match="model_id is required"):
            client.create_table("", sample_arrow_table, name="T")

    def test_unsupported_data_type_raises(self, client: MyCorr) -> None:
        """A non-tabular input is rejected with TypeError."""
        with pytest.raises(TypeError, match="must be a pandas/polars DataFrame"):
            client.create_table("m", {"id": [1]}, name="T")  # type: ignore[arg-type]
