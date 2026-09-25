"""Tests for the MyCorr client."""

from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import respx

from pymycorr import (
    AuthenticationError,
    InvalidDataError,
    MyCorr,
    MyCorrDataWarning,
    PermissionDeniedError,
    QuotaExceededError,
    RateLimitError,
    StorageQuotaExceededError,
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

    @respx.mock
    @pytest.mark.parametrize(
        ("body_extra", "expected"),
        [
            ({"scheduled_for_deletion": 1_900_000_000}, 1_900_000_000),
            ({"scheduled_for_deletion": None}, None),
            # A server from before the field: absent reads as not scheduled.
            ({}, None),
        ],
    )
    def test_scheduled_for_deletion_passes_through(
        self,
        client: MyCorr,
        body_extra: dict[str, int | None],
        expected: int | None,
    ) -> None:
        route = respx.get(url__startswith="https://test.example.com/api/data/tableinfo")
        route.return_value = httpx.Response(
            200, json={"table_id": "t", "name": "T", "columns": {}, **body_extra}
        )

        info = client.get_table_info("t")

        assert info.get("scheduled_for_deletion") == expected


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
        assert "version_alias" not in str(request.url), "an explicit version is sent alone"

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


def _sent_stream(request: httpx.Request) -> pa.Table:
    """The Arrow IPC body a request carried, read back."""
    return ipc.open_stream(request.content).read_all()


def _sent_batches(request: httpx.Request) -> list[pa.RecordBatch]:
    return list(ipc.open_stream(request.content))


class TestCreateTable:
    """Tests for create_table: a dry run, then a streamed Arrow IPC body."""

    _URL = "https://test.example.com/api/server/api/model/mod-1/tables"
    _CREATED = {"model_id": "mod-1", "table_id": "tab-1", "description": None, "warnings": []}

    def _routes(
        self,
        grant: dict[str, object] | None = None,
        created: tuple[int, dict[str, object]] | None = None,
    ) -> tuple[respx.Route, respx.Route]:
        dry = respx.post(url__startswith=self._URL, params__contains={"dry_run": "true"})
        dry.return_value = httpx.Response(
            200,
            json=grant or {"model_id": "mod-1", "max_bytes": 1 << 30, "limit_kind": "upload_size"},
        )
        status, body = created or (201, self._CREATED)
        upload = respx.post(url__startswith=self._URL)
        upload.return_value = httpx.Response(status, json=body)
        return dry, upload

    @respx.mock
    def test_checks_first_then_streams_to_the_model_path(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        """A dry run carries the same params; the upload then sends the rows."""
        dry, upload = self._routes()

        result = client.create_table(
            "mod-1", sample_arrow_table, name="People", primary_key="id", progress=False
        )

        assert result == self._CREATED
        assert dry.called and upload.call_count == 1
        assert "name=People" in str(dry.calls.last.request.url)
        request = upload.calls.last.request
        assert "dry_run" not in str(request.url)
        assert request.headers["Content-Type"] == "application/vnd.apache.arrow.stream"
        assert request.headers["Authorization"] == "Bearer test-token"
        url = str(request.url)
        assert "name=People" in url
        assert "pk=id" in url
        assert _sent_stream(request).equals(sample_arrow_table)

    @respx.mock
    def test_composite_primary_key_is_joined(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        _, upload = self._routes()

        client.create_table(
            "mod-1",
            sample_arrow_table,
            name="T",
            primary_key=["id", "name"],
            progress=False,
        )

        assert "pk=id%2Cname" in str(upload.calls.last.request.url)

    @respx.mock
    def test_the_description_travels_in_the_body_not_the_url(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        dry, upload = self._routes()

        client.create_table(
            "mod-1",
            sample_arrow_table,
            name="T",
            description="  Quarterly output — ünïcode & all.  ",
            progress=False,
        )

        for route in (dry, upload):
            assert "description" not in str(route.calls.last.request.url)
        sent = ipc.open_stream(upload.calls.last.request.content).schema
        assert sent.metadata[b"mycorr.description"].decode() == "Quarterly output — ünïcode & all."

    def test_an_oversized_description_is_refused_before_any_request(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        with pytest.raises(ValueError, match="at most 10000"):
            client.create_table("mod-1", sample_arrow_table, name="T", description="é" * 5_001)

    @respx.mock
    def test_a_large_table_is_sent_in_bounded_batches(self, client: MyCorr) -> None:
        """No batch is much over batch_bytes, whatever the input's own chunking."""
        table = pa.table({"n": pa.array(range(20_000), pa.int64())})
        _, upload = self._routes()

        client.create_table("mod-1", table, name="T", batch_bytes=8 * 1024, progress=False)

        batches = _sent_batches(upload.calls.last.request)
        assert len(batches) > 1
        assert all(b.nbytes <= 8 * 1024 for b in batches)
        assert sum(b.num_rows for b in batches) == 20_000

    @respx.mock
    def test_pandas_is_converted_in_slices_with_the_whole_frames_types(
        self, client: MyCorr
    ) -> None:
        """Slicing must not type a column from a slice that happens to be all null."""
        import pandas as pd

        df = pd.DataFrame(
            {"id": range(5_000), "note": [None] * 4_000 + ["x"] * 1_000},
        )
        _, upload = self._routes()

        client.create_table("mod-1", df, name="T", batch_bytes=16 * 1024, progress=False)

        batches = _sent_batches(upload.calls.last.request)
        assert len(batches) > 1
        sent = pa.Table.from_batches(batches)
        assert sent.num_rows == 5_000
        note = sent.schema.field("note").type
        # Text — not the `null` an all-null first slice would have inferred.
        assert pa.types.is_string(note) or pa.types.is_large_string(note), note

    @respx.mock
    def test_polars_frames_are_uploaded_and_lazy_ones_refused(self, client: MyCorr) -> None:
        pl = pytest.importorskip("polars")
        df = pl.DataFrame({"id": [1, 2, 3], "s": ["a", "b", "c"]})
        _, upload = self._routes()

        client.create_table("mod-1", df, name="T", progress=False)
        assert _sent_stream(upload.calls.last.request).num_rows == 3

        with pytest.raises(TypeError, match="LazyFrame must be collected"):
            client.create_table("mod-1", df.lazy(), name="T", progress=False)

    @respx.mock
    def test_wide_string_types_are_left_for_the_server(self, client: MyCorr) -> None:
        """The server normalizes types now, so large_string is sent as-is."""
        wide = pa.table({"s": pa.array(["x"], pa.large_utf8())})
        _, upload = self._routes()

        client.create_table("mod-1", wide, name="T", progress=False)

        assert _sent_stream(upload.calls.last.request).schema.field("s").type == pa.large_utf8()

    @respx.mock
    def test_an_iterable_of_tables_is_one_upload(self, client: MyCorr) -> None:
        parts = (pa.table({"n": pa.array([i, i + 1], pa.int64())}) for i in range(0, 6, 2))
        _, upload = self._routes()

        client.create_table("mod-1", parts, name="T", progress=False)

        assert _sent_stream(upload.calls.last.request).column("n").to_pylist() == [0, 1, 2, 3, 4, 5]

    @respx.mock
    def test_a_record_batch_reader_is_streamed(self, client: MyCorr) -> None:
        schema = pa.schema([("n", pa.int64())])
        reader = pa.RecordBatchReader.from_batches(
            schema, [pa.record_batch({"n": [1, 2]}, schema=schema)] * 3
        )
        _, upload = self._routes()

        client.create_table("mod-1", reader, name="T", progress=False)

        assert _sent_stream(upload.calls.last.request).num_rows == 6

    @respx.mock
    def test_a_mismatched_table_in_an_iterable_is_the_error_raised(self, client: MyCorr) -> None:
        """The data's own error surfaces, not the aborted request it caused."""
        parts = [pa.table({"n": [1]}), pa.table({"other": ["x"]})]
        self._routes()

        with pytest.raises(ValueError, match="table 2 of the iterable does not match"):
            client.create_table("mod-1", parts, name="T", progress=False)

    @respx.mock
    def test_a_refusing_dry_run_sends_no_data(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        dry = respx.post(url__startswith=self._URL, params__contains={"dry_run": "true"})
        dry.return_value = httpx.Response(
            403,
            json={
                "error": "org_scope_mismatch",
                "error_description": "this model belongs to a different organization",
            },
        )
        upload = respx.post(url__startswith=self._URL)

        with pytest.raises(PermissionDeniedError) as err:
            client.create_table("mod-1", sample_arrow_table, name="T", progress=False)

        assert err.value.code == "org_scope_mismatch"
        assert err.value.status_code == 403
        assert "different organization" in str(err.value)
        assert not upload.called

    @respx.mock
    @pytest.mark.parametrize(
        ("limit_kind", "expected"),
        [("storage_quota", StorageQuotaExceededError), ("upload_size", InvalidDataError)],
    )
    def test_data_already_over_the_cap_is_refused_before_sending(
        self,
        client: MyCorr,
        limit_kind: str,
        expected: type[Exception],
    ) -> None:
        table = pa.table({"n": pa.array(range(1_000), pa.int64())})
        _, upload = self._routes(
            grant={"model_id": "mod-1", "max_bytes": 100, "limit_kind": limit_kind}
        )

        with pytest.raises(expected):
            client.create_table("mod-1", table, name="T", progress=False)

        assert upload.call_count == 0

    @respx.mock
    @pytest.mark.parametrize(
        ("status", "code", "expected"),
        [
            (401, "org_bound_token_required", AuthenticationError),
            (403, "not_org_member", PermissionDeniedError),
            (413, "upload_too_large", InvalidDataError),
            (422, "batch_too_large", InvalidDataError),
            (409, "model_scheduled_for_deletion", TableAPIError),
            (500, "create_failed", TableAPIError),
        ],
    )
    def test_upload_refusals_map_to_exceptions_with_their_code(
        self,
        client: MyCorr,
        sample_arrow_table: pa.Table,
        status: int,
        code: str,
        expected: type[TableAPIError],
    ) -> None:
        self._routes(created=(status, {"error": code, "error_description": "refused"}))

        with pytest.raises(expected) as err:
            client.create_table("mod-1", sample_arrow_table, name="T", progress=False)

        assert err.value.code == code
        assert err.value.status_code == status

    @respx.mock
    def test_storage_refusal_carries_the_figures(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        self._routes(
            created=(
                413,
                {
                    "error": "storage_quota_exceeded",
                    "message": "Storage is full.",
                    "used_bytes": 90,
                    "quota_bytes": 100,
                    "requested_bytes": 11,
                },
            )
        )

        with pytest.raises(StorageQuotaExceededError) as err:
            client.create_table("mod-1", sample_arrow_table, name="T", progress=False)

        assert err.value.detail["quota_bytes"] == 100
        assert isinstance(err.value, QuotaExceededError)

    @respx.mock
    def test_too_many_uploads_is_a_rate_limit_with_its_wait(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        self._routes(
            created=(
                429,
                {"error": "too_many_concurrent_uploads", "message": "wait", "retry_after_secs": 30},
            )
        )

        with pytest.raises(RateLimitError) as err:
            client.create_table("mod-1", sample_arrow_table, name="T", progress=False)

        assert err.value.retry_after == 30
        assert err.value.code == "too_many_concurrent_uploads"

    @respx.mock
    def test_lossy_conversions_are_warned(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        created = {
            **self._CREATED,
            "warnings": [
                {
                    "column_name": "blob",
                    "original_arrow_type": "Binary",
                    "normalized_arrow_type": "Utf8",
                    "reason": "binary data stored as base64 text",
                }
            ],
        }
        self._routes(created=(201, created))

        with pytest.warns(MyCorrDataWarning, match="'blob'"):
            client.create_table("mod-1", sample_arrow_table, name="T", progress=False)

    def test_arguments_are_checked_before_any_request(
        self, client: MyCorr, sample_arrow_table: pa.Table
    ) -> None:
        with pytest.raises(ValueError, match="model_id is required"):
            client.create_table("", sample_arrow_table, name="T")
        with pytest.raises(ValueError, match="name is required"):
            client.create_table("mod-1", sample_arrow_table, name="  ")
        with pytest.raises(ValueError, match="batch_bytes must be positive"):
            client.create_table("mod-1", sample_arrow_table, name="T", batch_bytes=0)
        with pytest.raises(ValueError, match="empty iterable"):
            client.create_table("mod-1", [], name="T")

    def test_unsupported_data_type_raises(self, client: MyCorr) -> None:
        """A non-tabular input is rejected with TypeError, before any request."""
        with pytest.raises(TypeError, match="must be a pandas/polars DataFrame"):
            client.create_table("m", {"id": [1]}, name="T")
