"""Custom exceptions for the PyMyCorr client."""

from __future__ import annotations

from typing import Any


class TableAPIError(Exception):
    """Base exception for table API errors."""


class TableNotFoundError(TableAPIError):
    """Raised when table is not found."""


class TableConversionError(TableAPIError):
    """Raised when table conversion fails."""


class StreamingError(TableAPIError):
    """Raised when an error occurs during data streaming."""

    def __init__(self, message: str, batches_received: int = 0):
        super().__init__(message)
        self.batches_received = batches_received


class QuotaExceededError(TableAPIError):
    """Raised when the daily egress quota is exceeded (HTTP 429).

    The ``detail`` attribute contains the raw error payload from the server
    (e.g. error code and reset time when the server provides it).
    """

    def __init__(self, detail: dict[str, Any]) -> None:
        message = detail.get("message") or detail.get("error", "egress quota exceeded")
        super().__init__(message)
        self.detail = detail
