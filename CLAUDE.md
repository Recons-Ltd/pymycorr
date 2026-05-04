# pymycorr

Python SDK for fetching table data from the MyCorr API using Apache Arrow IPC streaming.

## Architecture

```
src/pymycorr/
├── client.py        # MyCorr client class (all public + private methods)
├── exceptions.py    # Exception hierarchy (TableAPIError base, specific subclasses)
├── _progress.py     # Progress bar utilities
└── __init__.py      # Public exports: MyCorr + all exception classes
```

Single-file SDK. `client.py` is the whole thing. Three public methods:
- `MyCorr()` — client init (token from env var or .env file, URL defaults to `space.mycorr.app`)
- `get_table(table_id)` — fetch table as pandas/polars DataFrame
- `get_table_info(table_id)` — fetch table metadata as dict

Internal streaming uses httpx async + PyArrow IPC parsing. A `_StreamingBuffer` bridges async HTTP chunks to PyArrow's synchronous reader via a background thread.

## API Endpoints

The client appends these paths to the base URL:
- `GET /data/table/stream` — Arrow IPC binary stream (used by `get_table`)
- `GET /data/tableinfo` — JSON metadata (used by `get_table_info`)

## Exception Hierarchy

```
TableAPIError (base)
├── TableNotFoundError (404)
├── QuotaExceededError (429)
├── StreamingError (IPC parsing failures)
└── TableConversionError (Arrow → DataFrame failures)
```

## Testing

```bash
# Run tests
.venv/bin/python -m pytest tests/ -v

# Lint
.venv/bin/python -m ruff check src/ tests/

# Type check
.venv/bin/python -m mypy src/
```

Tests use `respx` for HTTP mocking and `pyarrow` for generating test Arrow data. All tests use mock URLs (`test.example.com`). No real API calls.

## Development Setup

1. Copy `.env.example` to `.env`
2. Set `MYCORR_API_TOKEN` to your bearer token
3. Optionally set `MYCORR_API_URL` for local dev (defaults to `https://space.mycorr.app`)
4. SSL verification is auto-disabled for `localhost` and `127.0.0.1` URLs

## Conventions

- All internal methods are `_`-prefixed (private by convention)
- Error handling flows through `_handle_error_response()` (single place for HTTP status → exception mapping)
- `python-dotenv` loads `.env` files automatically on client init
- Progress bars auto-detect interactive environments (terminal/notebook vs script/CI)
