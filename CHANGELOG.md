# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-09-25

Requires a MyCorr server with the table-upload door
(`POST /server/api/model/{model_id}/tables`).

### Changed

- `create_table` streams its data in bounded record batches (`batch_bytes`,
  16 MiB by default) instead of building the whole upload in memory, and posts
  to `/server/api/model/{model_id}/tables` (the old
  `/server/api/datasets/tables` no longer exists on the server).
- `create_table` asks the server first (a dry run) and raises a refusal before
  sending any data; data already larger than the server's cap is refused
  without being sent.
- `create_table` no longer downcasts string columns: the server now converts
  every column to the nearest MyCorr type itself.
- `create_table`'s `description` travels inside the upload (the Arrow schema
  metadata key `mycorr.description`) instead of the URL, and is checked against
  the server's 10,000-byte limit before any request. A successful create means
  the table exists with its description; the server removes it if the
  description could not be saved. The result includes `description`.
- Error messages use the server's own description, and every `TableAPIError`
  carries `status_code` and the server's `code`.
- `get_table_info` sends only `version` for an integer version (it also sent
  `version_alias=latest`).

### Removed

- `create_table`'s `labels` argument. Catalog labels are MyCorr's internal
  data-catalog curation, not part of the public API; the server refuses the
  parameter.

### Added

- `create_table` accepts a pyarrow `RecordBatchReader` or an iterable of
  DataFrames/Tables/RecordBatches for data larger than memory, plus
  `batch_bytes`, `progress` and `timeout`.
- `AuthenticationError` (401), `PermissionDeniedError` (403), `InvalidDataError`
  (400/413/422) and `StorageQuotaExceededError` (413).
- `MyCorrDataWarning`, raised for each column the server converted lossily.
- `get_table_info` documents `scheduled_for_deletion`: `None`, or when a table in
  the trash is permanently deleted.

## [0.3.0] - 2026-07-01

### Added

- `MyCorr.create_table(model_id, data, *, name, primary_key=None)` — create a
  new table in a model from a pandas/polars DataFrame or a pyarrow
  Table/RecordBatch. Encodes the data as an Arrow IPC stream and uploads it to
  the write API. String columns are downcast from `LargeUtf8`/`Utf8View` to
  plain `Utf8` (MyCorr persistence rejects the wide variants). Requires a
  write-scoped token with edit access to the target model.

## [0.2.0] - 2026-03-30

### Changed

- Default API URL updated to `https://mycorr.app`
- SSL verification now uses proper hostname parsing (supports both `localhost` and `127.0.0.1`)
- `get_table_info()` now raises `TableAPIError` instead of `StreamingError` for HTTP errors
- Error handling consolidated into shared `_handle_error_response()` helper
- Removed `print()` on empty table in `get_table()` (libraries should not print to stdout)

### Fixed

- `TableNotFoundError` now properly raised on 404 responses (was previously defined but never used)
- Docstrings across all methods now accurately reflect actual exception behavior
- README examples no longer include incorrect URL path segments

### Added

- `RateLimitError` exception for HTTP 429 rate limit responses (governor)
- `RateLimitError.retry_after` attribute exposing seconds to wait before retrying
- `.env.example` for contributor dev setup
- "API coming soon" note in README

### Breaking

- HTTP 429 responses with `"error": "rate_limit_exceeded"` now raise `RateLimitError` instead of `QuotaExceededError`. Code catching `QuotaExceededError` for all 429s should catch `TableAPIError` instead, or handle both `QuotaExceededError` and `RateLimitError`.

## [0.1.0] - Unreleased

### Added

- Initial release
- `MyCorr` client class for API access
- Support for pandas and polars DataFrame output
- Async `get_data_stream` method for fetching Arrow tables
- Sync `get_table` method for fetching DataFrames
- `get_table_info` method for retrieving table metadata
- Full type hints and py.typed marker
