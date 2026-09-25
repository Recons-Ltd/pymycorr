# pymycorr

[![PyPI version](https://img.shields.io/pypi/v/pymycorr)](https://pypi.org/project/pymycorr/)
[![CI](https://img.shields.io/github/actions/workflow/status/recons-ltd/pymycorr/ci.yml?branch=main&label=CI)](https://github.com/recons-ltd/pymycorr/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-%E2%89%A53.10-blue)](https://github.com/recons-ltd/pymycorr/blob/main/pyproject.toml)
[![License](https://img.shields.io/github/license/recons-ltd/pymycorr)](https://github.com/recons-ltd/pymycorr/blob/main/LICENSE)

Python client for reading and writing table data on the [MyCorr](https://space.mycorr.app) API using Apache Arrow for efficient data transfer.

> **Note:** The production API is coming soon. Set `MYCORR_API_URL` to your endpoint if you have early access.

## Installation

```bash
pip install pymycorr
```

## Configuration

Set your API token and (optionally) the API URL as environment variables:

```bash
export MYCORR_API_TOKEN="your-api-token"
```

Or create a `.env` file in your project root:

```
MYCORR_API_TOKEN=your-api-token
```

The client automatically loads from environment variables and `.env` files.

## Quick Start

```python
from pymycorr import MyCorr

# Initialize client (loads token from MYCORR_API_TOKEN env var or .env file)
client = MyCorr()

# Fetch as pandas DataFrame
df = client.get_table("table-id")

# Fetch a specific version
df = client.get_table("table-id", version=2)

# Fetch using version alias
df = client.get_table("table-id", version="stable")

# Get table metadata without fetching data
info = client.get_table_info("table-id")
print(info["columns"])
if info.get("scheduled_for_deletion"):
    print("in the trash until", info["scheduled_for_deletion"])

# Create a new table in a model from a DataFrame (requires a write-scoped token)
result = client.create_table("model-id", df, name="My Table", primary_key="id")
print(result["table_id"])
```

### Uploading tables

`create_table` streams the data in bounded record batches, so a large frame is
never held in memory twice, and an iterable of frames can be uploaded without
ever holding the whole table:

```python
import pyarrow.parquet as pq

# Larger than memory: stream a parquet file batch by batch
reader = pq.ParquetFile("big.parquet").iter_batches(batch_size=100_000)
client.create_table("model-id", reader, name="Big Table")
```

The token must be **write-scoped and bound to the model's organization**, and its
user must be able to edit the model *and* be a member of that organization —
edit access through a share alone is not enough. Before sending any data the
client asks the server whether the upload would be accepted, so a refusal
(wrong organization, no storage left, too many uploads) is raised immediately
rather than after the upload. Column types are converted server-side to the
nearest MyCorr type; a conversion that loses information is reported as a
`MyCorrDataWarning`.

### Progress Display

Progress is shown automatically in interactive environments (terminals and notebooks). You can control this behavior:

```python
# Always show progress
client = MyCorr(progress=True)

# Never show progress
client = MyCorr(progress=False)

# Override per-request
df = client.get_table("table-id", progress=False)
```

## API Reference

### MyCorr

#### `__init__(url=None, token=None, env_file=None, progress="auto")`

Initialize the client.

- `url`: API base URL (optional). Falls back to `MYCORR_API_URL` env var, then default.
- `token`: Authentication token (Bearer token from MyCorr UI). Falls back to `MYCORR_API_TOKEN` env var or `.env` file.
- `env_file`: Path to custom `.env` file (optional).
- `progress`: Show download progress. `True` always shows, `False` never shows, `"auto"` (default) shows in interactive environments.

#### `get_table(table_id, version=None, engine="pandas", progress=None)`

Fetch table data as a DataFrame.

- `table_id`: Unique identifier for the table.
- `version`: Version number (int) or alias (str like `"latest"`, `"stable"`).
- `engine`: `"pandas"` (default) or `"polars"`.
- `progress`: Override client's progress setting for this request.
- Returns: pandas or polars DataFrame.

#### `get_table_info(table_id, version=None)`

Get table schema and metadata.

- `table_id`: Unique identifier for the table.
- `version`: Version number (int) or alias (str).
- Returns: Dictionary with `table_id`, `name`, `active_rows`, `created_at`,
  `schema_last_modified`, `columns` and `scheduled_for_deletion` — `None` for a
  live table, or the Unix time (seconds) at which a table in the trash is
  permanently deleted. Older servers omit the field; read it with `.get()`.

#### `create_table(model_id, data, *, name, primary_key=None, description=None, batch_bytes=16 MiB, progress=None, timeout=None)`

Create a new table in a model by streaming tabular data. Requires a
**write-scoped** token bound to the model's organization, edit access to the
model and membership of its organization (a catalog model takes the
catalog-manager role instead). Not retried automatically: creating a table is
not idempotent.

- `model_id`: The model the table is created in.
- `data`: A pandas or polars DataFrame, a pyarrow `Table`, `RecordBatch` or
  `RecordBatchReader`, or an iterable of DataFrames/Tables/RecordBatches sharing
  one schema.
- `name`: Name for the new table.
- `primary_key`: Primary-key column name(s) — a single name or a sequence for a
  composite key. Marking a key here is what lets the table be diff-synced later.
- `description`: Table description, shown in the catalog and the details panel
  (at most 10,000 bytes). Sent inside the upload as the Arrow schema metadata key
  `mycorr.description`, never in the URL.
- `batch_bytes`: Target size of each uploaded record batch (the server refuses
  batches over 64 MiB).
- `progress`: Override the client's progress setting for this upload.
- `timeout`: Request timeout (default: 30s connect, 300s between writes, 1800s
  for the server to finish after the last byte).
- Returns: Dictionary with `model_id`, `table_id`, `description` and `warnings`.
  A success means the table exists with its description; if saving it fails,
  the server removes the table.

## Exceptions

Every exception carries `status_code` and the server's `code` when it sent one.

- `TableAPIError`: Base exception for API errors.
- `AuthenticationError`: Token missing, invalid, or not bound to an organization (401).
- `PermissionDeniedError`: Token may not do this — see `code` (403).
- `TableNotFoundError`: Table not found (404).
- `InvalidDataError`: The server refused the uploaded data — see `code` (400/413/422).
- `StorageQuotaExceededError`: The upload does not fit in the organization's storage (413).
- `QuotaExceededError`: Egress quota exceeded (429).
- `RateLimitError`: Too many requests or uploads; `retry_after` says when to retry (429).
- `TableConversionError`: Failed to convert Arrow data to DataFrame.
- `StreamingError`: Error during data streaming or IPC parsing.

`MyCorrDataWarning` is a warning, not an exception: a column was stored with a
lossy conversion.

## Development

See [.env.example](.env.example) for environment variable configuration. For local development:

1. Copy `.env.example` to `.env`
2. Set `MYCORR_API_URL` to your local/dev base URL (no path segments)
3. SSL verification is automatically disabled for `localhost` and `127.0.0.1` URLs

## License

MIT License - see [LICENSE](LICENSE) for details.
