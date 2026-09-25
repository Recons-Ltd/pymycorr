# pymycorr

Python SDK for reading and writing MyCorr table data using Apache Arrow IPC streaming.

## Architecture

```
src/pymycorr/
├── client.py        # MyCorr client class (all public + private methods)
├── exceptions.py    # Exception hierarchy (TableAPIError base, specific subclasses)
├── _progress.py     # Progress bar utilities
└── __init__.py      # Public exports: MyCorr + all exception classes
```

Single-file SDK. `client.py` is the whole thing. Four public methods:
- `MyCorr()` — client init (token from env var or .env file, URL defaults to `space.mycorr.app`)
- `get_table(table_id)` — fetch table as pandas/polars DataFrame
- `get_table_info(table_id)` — fetch table metadata as dict
- `create_table(model_id, data, *, name, primary_key=None, ...)` — create a table in a model (write-scoped, org-bound token; member + editor of the model). Sends a `dry_run` first, then streams bounded record batches (`batch_bytes`) as a chunked Arrow IPC body. Accepts DataFrames, Arrow tables/batches/readers, or iterables of them. The server normalizes column types; lossy conversions come back as `MyCorrDataWarning`.

Internal streaming uses httpx async + PyArrow IPC parsing. A `_StreamingBuffer` bridges async HTTP chunks to PyArrow's synchronous reader via a background thread.

## API Endpoints

The client appends these paths to the base URL:
- `GET /data/table/stream` — Arrow IPC binary stream (used by `get_table`)
- `GET /data/tableinfo` — JSON metadata (used by `get_table_info`)
- `POST /server/api/model/{model_id}/tables` — upload an Arrow IPC stream to create a table (used by `create_table`; `?dry_run=true` checks without uploading)

## Exception Hierarchy

```
TableAPIError (base; .status_code, .code)
├── AuthenticationError (401)
├── PermissionDeniedError (403)
├── TableNotFoundError (404)
├── InvalidDataError (400/413/422)
├── QuotaExceededError (429 egress)
│   └── StorageQuotaExceededError (413 storage)
├── RateLimitError (429 rate limit / too many uploads)
├── StreamingError (IPC parsing failures)
└── TableConversionError (Arrow → DataFrame failures)
```

`MyCorrDataWarning` (a `UserWarning`) reports lossy server-side conversions.

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
