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
print(info["schema"])

# Create a new table in a model from a DataFrame (requires a write-scoped token)
result = client.create_table("model-id", df, name="My Table", primary_key="id")
print(result["table_id"])
```

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
- Returns: Dictionary with table metadata including schema.

#### `create_table(model_id, data, *, name, primary_key=None)`

Create a new table in a model from tabular data. Encodes `data` as an Arrow IPC
stream and uploads it to the write API; the server persists the table and adds
it to the model. Requires a **write-scoped** token with edit access to the model.

- `model_id`: The model the table is created in.
- `data`: A pandas/polars DataFrame or a pyarrow `Table`/`RecordBatch`.
- `name`: Name for the new table.
- `primary_key`: Primary-key column name(s) — a single name or a sequence for a
  composite key. Marking a key here is what lets the table be diff-synced later.
- Returns: Dictionary with the created `model_id` and `table_id`.

## Exceptions

- `TableAPIError`: Base exception for API errors.
- `TableNotFoundError`: Table not found (404).
- `QuotaExceededError`: Egress quota exceeded (429).
- `TableConversionError`: Failed to convert Arrow data to DataFrame.
- `StreamingError`: Error during data streaming or IPC parsing.

## Development

See [.env.example](.env.example) for environment variable configuration. For local development:

1. Copy `.env.example` to `.env`
2. Set `MYCORR_API_URL` to your local/dev base URL (no path segments)
3. SSL verification is automatically disabled for `localhost` and `127.0.0.1` URLs

## License

MIT License - see [LICENSE](LICENSE) for details.
