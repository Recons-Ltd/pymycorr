"""Tests for the MyCorr client."""

from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest
import respx

from pymycorr import MyCorr, StreamingError, TableAPIError
from pymycorr.client import _IPCStreamBuffer


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
        """Test handling of API error response."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(404, json={"message": "Table not found"})

        with pytest.raises(TableAPIError, match="Table not found"):
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


class TestIPCStreamBuffer:
    """Tests for the _IPCStreamBuffer class."""

    def test_single_complete_stream_in_one_chunk(self, sample_arrow_table: pa.Table) -> None:
        """Test parsing when entire IPC stream arrives in one chunk."""
        # Create complete IPC stream
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        buffer = _IPCStreamBuffer()
        streams = list(buffer.add_chunk(data))

        assert len(streams) == 1
        assert buffer.streams_parsed == 1
        assert buffer.total_bytes == len(data)
        assert buffer.remaining() == b""

    def test_stream_split_across_multiple_chunks(self, sample_arrow_table: pa.Table) -> None:
        """Test parsing when IPC stream spans multiple HTTP chunks."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Split into 3 chunks
        chunk_size = len(data) // 3
        chunks = [
            data[:chunk_size],
            data[chunk_size : chunk_size * 2],
            data[chunk_size * 2 :],
        ]

        buffer = _IPCStreamBuffer()
        all_streams: list[bytes] = []

        for chunk in chunks:
            all_streams.extend(buffer.add_chunk(chunk))

        assert len(all_streams) == 1
        assert buffer.streams_parsed == 1

    def test_eos_marker_split_across_chunks(self, sample_arrow_table: pa.Table) -> None:
        """Test when EOS marker itself spans two chunks."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Find EOS marker and split right in the middle of it
        eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"
        eos_pos = data.rfind(eos_marker)
        split_point = eos_pos + 4  # Split in middle of EOS marker

        chunk1 = data[:split_point]
        chunk2 = data[split_point:]

        buffer = _IPCStreamBuffer()
        streams1 = list(buffer.add_chunk(chunk1))
        streams2 = list(buffer.add_chunk(chunk2))

        assert len(streams1) == 0  # No complete stream yet
        assert len(streams2) == 1  # Now complete
        assert buffer.streams_parsed == 1

    def test_multiple_streams_in_buffer(self, multi_batch_arrow_bytes: bytes) -> None:
        """Test when buffer contains multiple complete IPC streams."""
        buffer = _IPCStreamBuffer()
        streams = list(buffer.add_chunk(multi_batch_arrow_bytes))

        assert len(streams) == 3
        assert buffer.streams_parsed == 3
        assert buffer.remaining() == b""

    def test_max_buffer_size_exceeded(self, sample_arrow_table: pa.Table) -> None:
        """Test that exceeding buffer size raises StreamingError."""
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, sample_arrow_table.schema)
        writer.write_table(sample_arrow_table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Remove EOS marker so buffer keeps growing
        eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"
        incomplete_data = data[: data.rfind(eos_marker)]

        buffer = _IPCStreamBuffer(max_buffer_size=50)

        with pytest.raises(StreamingError, match="Buffer exceeded"):
            list(buffer.add_chunk(incomplete_data))

    def test_remaining_returns_leftover_data(self) -> None:
        """Test that remaining() returns incomplete data."""
        buffer = _IPCStreamBuffer()
        list(buffer.add_chunk(b"incomplete data"))

        assert buffer.remaining() == b"incomplete data"
        assert buffer.streams_parsed == 0

    def test_false_positive_eos_marker_in_int32_data(self) -> None:
        """Test that EOS marker bytes appearing as int32 values don't cause false splits.

        The EOS marker is 0xFFFFFFFF 0x00000000, which equals int32 values -1 and 0.
        This tests that we don't incorrectly split when these values appear in data.
        """
        # Create table with int32 column containing -1 and 0 (EOS marker bytes)
        table = pa.table(
            {
                "values": pa.array([-1, 0, -1, 0, 42, -1, 0], type=pa.int32()),
            }
        )

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Verify the EOS marker bytes appear multiple times in the data
        eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"
        occurrences = data.count(eos_marker)
        assert occurrences >= 2, f"Expected multiple EOS marker occurrences, got {occurrences}"

        buffer = _IPCStreamBuffer()
        streams = list(buffer.add_chunk(data))

        # Should yield exactly 1 valid stream, not split on false positives
        assert len(streams) == 1
        assert buffer.streams_parsed == 1
        assert buffer.remaining() == b""

        # Verify the stream is valid and contains correct data
        reader = ipc.open_stream(streams[0])
        result_table = reader.read_all()
        assert result_table.num_rows == 7
        assert result_table.column("values").to_pylist() == [-1, 0, -1, 0, 42, -1, 0]

    def test_false_positive_eos_marker_in_int64_data(self) -> None:
        """Test that EOS marker bytes appearing in int64 values don't cause false splits.

        The 8-byte EOS marker could appear as part of int64 values.
        """
        # int64 value that contains EOS marker bytes: 0x00000000FFFFFFFF = 4294967295
        # and 0xFFFFFFFF00000000 = -4294967296 (as signed)
        table = pa.table(
            {
                "big_values": pa.array([4294967295, -4294967296, 0, 100], type=pa.int64()),
            }
        )

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        buffer = _IPCStreamBuffer()
        streams = list(buffer.add_chunk(data))

        assert len(streams) == 1
        assert buffer.streams_parsed == 1

        # Verify data integrity
        reader = ipc.open_stream(streams[0])
        result_table = reader.read_all()
        assert result_table.num_rows == 4

    def test_false_positive_eos_marker_in_binary_data(self) -> None:
        """Test that EOS marker bytes in binary column don't cause false splits."""
        eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"

        # Create binary data containing the EOS marker
        table = pa.table(
            {
                "binary_col": pa.array(
                    [
                        b"hello",
                        eos_marker,  # Exact EOS marker as data
                        b"world",
                        eos_marker + b"extra",
                        b"prefix" + eos_marker + b"suffix",
                    ],
                    type=pa.binary(),
                ),
            }
        )

        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Should have multiple EOS marker occurrences
        assert data.count(eos_marker) >= 4

        buffer = _IPCStreamBuffer()
        streams = list(buffer.add_chunk(data))

        assert len(streams) == 1
        assert buffer.streams_parsed == 1

        # Verify data integrity
        reader = ipc.open_stream(streams[0])
        result_table = reader.read_all()
        assert result_table.num_rows == 5
        assert result_table.column("binary_col")[1].as_py() == eos_marker

    def test_chunked_delivery_with_false_positives(self) -> None:
        """Test that chunked delivery works correctly when data contains false positive markers.

        Simulates real network streaming where data arrives in chunks and contains
        EOS marker bytes as part of the actual data.
        """
        eos_marker = b"\xff\xff\xff\xff\x00\x00\x00\x00"

        # Create a table with data containing EOS marker bytes
        table = pa.table(
            {
                "int_col": pa.array([-1, 0, 100, -1, 0], type=pa.int32()),
                "str_col": ["a", "b", "c", "d", "e"],
            }
        )
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        data = sink.getvalue().to_pybytes()

        # Verify we have false positive markers
        assert data.count(eos_marker) >= 2

        # Simulate chunked delivery - split into small chunks
        chunk_size = 50
        chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

        buffer = _IPCStreamBuffer()
        all_streams: list[bytes] = []

        for chunk in chunks:
            all_streams.extend(buffer.add_chunk(chunk))

        # Should yield exactly 1 valid stream after all chunks processed
        assert len(all_streams) == 1
        assert buffer.streams_parsed == 1
        assert buffer.remaining() == b""

        # Verify the parsed stream has correct data
        reader = ipc.open_stream(all_streams[0])
        result_table = reader.read_all()
        assert result_table.num_rows == 5
        assert result_table.column("int_col").to_pylist() == [-1, 0, 100, -1, 0]


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
    async def test_streaming_error_on_incomplete_data(self, client: MyCorr) -> None:
        """Test that incomplete stream raises StreamingError."""
        # Send data without EOS marker
        incomplete_data = b"some random bytes that are not valid IPC"

        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(200, content=incomplete_data)

        with pytest.raises(StreamingError, match="incomplete data"):
            async for _ in client._stream_record_batches("test-table"):
                pass

    @pytest.mark.asyncio
    @respx.mock
    async def test_streaming_api_error(self, client: MyCorr) -> None:
        """Test API error handling in streaming mode."""
        route = respx.get(url__startswith="https://test.example.com/api/data/table/stream")
        route.return_value = httpx.Response(403, json={"message": "Access denied"})

        with pytest.raises(StreamingError, match="Access denied"):
            async for _ in client._stream_record_batches("test-table"):
                pass
