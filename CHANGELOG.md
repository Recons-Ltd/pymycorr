# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
