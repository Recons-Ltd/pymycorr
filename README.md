# pymycorr

[![PyPI version](https://badge.fury.io/py/pymycorr.svg)](https://badge.fury.io/py/pymycorr)
[![Python versions](https://img.shields.io/pypi/pyversions/pymycorr.svg)](https://pypi.org/project/pymycorr/)
[![License](https://img.shields.io/pypi/l/pymycorr.svg)](https://github.com/sambaclab/mycorr-python/blob/main/LICENSE)

Python client for fetching table data from the MyCorr API using Apache Arrow for efficient data transfer.

## Features

- Async and sync APIs for fetching table data
- Native support for both pandas and polars DataFrames
- Efficient Arrow IPC streaming for large datasets
- Full type hints for IDE support

## Installation

```bash
pip install pymycorr

# With pandas support
pip install pymycorr[pandas]

# With polars support
pip install pymycorr[polars]

# With both pandas and polars
pip install pymycorr[all]

# For Jupyter notebook users (includes nest-asyncio)
pip install pymycorr[all,jupyter]
```

## Quick Start

```python
from pymycorr import MyCorr

# Initialize client with your API URL and token
client = MyCorr(
    url="https://api.mycorr.com/data/table",
    token="your-api-token"
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

## Async Usage

```python
import asyncio
from pymycorr import MyCorr

async def main():
    client = MyCorr(
        url="https://api.mycorr.com/data/table",
        token="your-api-token"
    )

    # Get raw Arrow table
    arrow_table = await client.get_data_stream("table-id")

    # Convert to pandas
    df = arrow_table.to_pandas()
    return df

df = asyncio.run(main())
```

## API Reference

### MyCorr

#### `__init__(url: str, token: str)`

Initialize the client.

- `url`: API base URL
- `token`: Authentication token (Bearer token from MyCorr UI)

#### `get_table(table_id, version=None, engine="pandas")`

Fetch table data as a DataFrame.

- `table_id`: Unique identifier for the table
- `version`: Version number (int) or alias (str like `"latest"`, `"stable"`)
- `engine`: `"pandas"` or `"polars"`
- Returns: pandas or polars DataFrame

#### `get_data_stream(table_id, version=None)` *(async)*

Fetch raw Arrow table asynchronously.

- `table_id`: Unique identifier for the table
- `version`: Version number (int) or alias (str)
- Returns: PyArrow Table

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
