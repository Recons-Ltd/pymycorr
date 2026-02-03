# pymycorr

[![PyPI version](https://badge.fury.io/py/pymycorr.svg)](https://badge.fury.io/py/pymycorr)
[![Python versions](https://img.shields.io/pypi/pyversions/pymycorr.svg)](https://pypi.org/project/pymycorr/)
[![License](https://img.shields.io/pypi/l/pymycorr.svg)](https://github.com/sambaclab/mycorr-python/blob/main/LICENSE)

Python client for fetching table data from the MyCorr API using Apache Arrow for efficient data transfer.

## Features

- Native support for both pandas and polars DataFrames
- Efficient Arrow IPC streaming for large datasets
- Built-in progress indicator for downloads
- Full type hints for IDE support

## Installation

### Basic Installation

```bash
pip install pymycorr
```

This includes pandas support and progress indicators by default.

### With Optional Features

```bash
# With polars support
pip install "pymycorr[polars]"

# For Jupyter notebooks (async support)
pip install "pymycorr[jupyter]"

# All features (polars + jupyter support)
pip install "pymycorr[all]"
```

### Available Extras

| Extra     | Includes       | Use Case                            |
| --------- | -------------- | ----------------------------------- |
| `polars`  | polars         | Use polars DataFrames               |
| `jupyter` | nest-asyncio   | Async support in Jupyter notebooks  |
| `all`     | All of above   | Install everything                  |

### Combining Extras

You can combine multiple extras:

```bash
# polars + jupyter support
pip install "pymycorr[polars,jupyter]"
```

## Configuration

Set your API token as an environment variable:

```bash
export MYCORR_API_TOKEN="your-api-token"
```

Optionally, you can also set the API URL:

```bash
export MYCORR_API_URL="https://api.mycorr.com/data/table"
```

Or create a `.env` file in your project root:

```
MYCORR_API_TOKEN=your-api-token
MYCORR_API_URL=https://api.mycorr.com/data/table
```

The client automatically loads from environment variables and `.env` files.

## Quick Start

```python
from pymycorr import MyCorr

# Initialize client (loads token from MYCORR_API_TOKEN env var or .env file)
client = MyCorr(url="https://api.mycorr.com/data/table")

# Or with explicit credentials
client = MyCorr(
    url="https://api.mycorr.com/data/table",
    token="your-api-token"
)

# Or specify a custom .env file
client = MyCorr(
    url="https://api.mycorr.com/data/table",
    env_file="/path/to/.env"
)

# Fetch as pandas DataFrame (default)
df = client.get_table("table-id")

# Fetch as polars DataFrame
df = client.get_table("table-id", engine="polars")

# Fetch a specific version
df = client.get_table("table-id", version=2)

# Fetch using version alias
df = client.get_table("table-id", version="stable")

# Get table metadata without fetching data
info = client.get_table_info("table-id")
print(info["schema"])
print(info["num_rows"])
```

### Progress Display

Progress is shown automatically in interactive environments (terminals and notebooks). You can control this behavior:

```python
# Always show progress
client = MyCorr(token="...", progress=True)

# Never show progress
client = MyCorr(token="...", progress=False)

# Auto-detect (default): show in terminals/notebooks, hide in scripts/CI
client = MyCorr(token="...", progress="auto")

# Override per-request
df = client.get_table("table-id", progress=False)
```

## API Reference

### MyCorr

#### `__init__(url: str, token: str = None, env_file: str = None, progress: bool | "auto" = "auto")`

Initialize the client.

- `url`: API base URL
- `token`: Authentication token (Bearer token from MyCorr UI). If not provided, loads from `MYCORR_API_TOKEN` environment variable or `.env` file.
- `env_file`: Path to custom `.env` file (optional)
- `progress`: Show download progress. `True` always shows, `False` never shows, `"auto"` (default) shows in interactive environments.

#### `get_table(table_id, version=None, engine="pandas", progress=None)`

Fetch table data as a DataFrame.

- `table_id`: Unique identifier for the table
- `version`: Version number (int) or alias (str like `"latest"`, `"stable"`)
- `engine`: `"pandas"` or `"polars"`
- `progress`: Override client's progress setting for this request
- Returns: pandas or polars DataFrame

#### `get_table_info(table_id, version=None)`

Get table schema and metadata.

- `table_id`: Unique identifier for the table
- `version`: Version number (int) or alias (str)
- Returns: Dictionary with `table_id`, `schema`, `num_columns`, `num_rows`, `column_names`, `column_types`

## Exceptions

- `TableAPIError`: Base exception for API errors
- `TableNotFoundError`: Table not found (404)
- `TableConversionError`: Failed to convert Arrow data to DataFrame

## License

MIT License - see [LICENSE](LICENSE) for details.
