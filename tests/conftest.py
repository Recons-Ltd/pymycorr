"""Pytest fixtures for pymycorr tests."""

from typing import Any

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
    """Create a single IPC stream with multiple batches.

    The server sends one IPC stream containing multiple record batches,
    with a single EOS marker at the end.
    """
    import pyarrow.ipc as ipc

    # Create schema once
    schema = pa.schema([("batch_id", pa.int64()), ("value", pa.int64())])

    # Create IPC stream with multiple batches
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)

    for i in range(3):
        batch = pa.record_batch(
            {
                "batch_id": [i] * 10,
                "value": list(range(i * 10, (i + 1) * 10)),
            },
            schema=schema,
        )
        writer.write_batch(batch)

    writer.close()
    result: bytes = sink.getvalue().to_pybytes()
    return result


@pytest.fixture
def sample_model_metadata() -> dict[str, Any]:
    """Sample model metadata matching the JSON API response."""
    return {
        "model_id": "mdl-test-1234",
        "model_name": "Test Model",
        "description": "A test model",
        "last_modified_at": 1709251200,
        "created_at": 1706572800,
    }


@pytest.fixture
def sample_table_info() -> dict[str, Any]:
    """Sample table info matching the JSON API response."""
    return {
        "name": "Customers",
        "table_id": "tbl-test-5678",
        "model_id": "mdl-test-1234",
        "latest_version": 12,
    }


@pytest.fixture
def sample_table_data_page() -> dict[str, Any]:
    """Sample paginated table data response."""
    return {
        "data": {
            "table_id": "tbl-test-5678",
            "name": "Customers",
            "created_at": 1706572800,
            "columns": [
                {
                    "id": "col-1",
                    "name": "Email",
                    "data_type": "Utf8",
                    "data": ["alice@test.com", "bob@test.com"],
                },
                {
                    "id": "col-2",
                    "name": "Revenue",
                    "data_type": "Float64",
                    "data": [1500.0, 2300.5],
                },
            ],
        },
        "meta": {
            "total_rows": 2,
            "total_columns": 2,
        },
    }
