"""Pytest fixtures for pymycorr tests."""

import pyarrow as pa
import pytest

from pymycorr import MyCorr


@pytest.fixture
def client() -> MyCorr:
    """Create a test client instance."""
    return MyCorr(url="https://test.example.com/api", token="test-token")


@pytest.fixture
def sample_arrow_table() -> pa.Table:
    """Create a sample Arrow table for testing."""
    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        }
    )


@pytest.fixture
def empty_arrow_table() -> pa.Table:
    """Create an empty Arrow table for testing."""
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
        ]
    )
    return pa.table({"id": [], "name": []}, schema=schema)


@pytest.fixture
def multi_batch_arrow_bytes() -> bytes:
    """Create multiple concatenated IPC streams (simulating server format).

    The server sends multiple complete IPC streams, one per batch,
    each with its own schema and terminated by EOS marker.
    """
    import pyarrow.ipc as ipc

    streams = []
    for i in range(3):
        table = pa.table(
            {
                "batch_id": [i] * 10,
                "value": list(range(i * 10, (i + 1) * 10)),
            }
        )
        sink = pa.BufferOutputStream()
        writer = ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        streams.append(sink.getvalue().to_pybytes())
    return b"".join(streams)
