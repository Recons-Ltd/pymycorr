"""Custom exceptions for the PyMyCorr client."""

from __future__ import annotations

from typing import Any


class TableAPIError(Exception):
    """Base exception for table API errors.

    ``status_code`` is the HTTP status of the response that raised it and
    ``code`` the server's machine-readable error code (e.g. ``"forbidden"``,
    ``"org_scope_mismatch"``), when there was one.
    """

    def __init__(
        self,
        message: str = "",
        *,
        status_code: int | None = None,
        code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class TableNotFoundError(TableAPIError):
    """Raised when table is not found."""


class TableConversionError(TableAPIError):
    """Raised when table conversion fails."""


class StreamingError(TableAPIError):
    """Raised when an error occurs during data streaming."""

    def __init__(self, message: str, batches_received: int = 0):
        super().__init__(message)
        self.batches_received = batches_received


class AuthenticationError(TableAPIError):
    """Raised when the token is missing, invalid, expired or revoked (HTTP 401).

    ``code == "org_bound_token_required"`` means the token predates
    organization binding: mint a new one for the model's organization.
    """


class PermissionDeniedError(TableAPIError):
    """Raised when the token may not do what was asked (HTTP 403).

    ``code`` says why — for ``create_table``: ``insufficient_scope`` (a
    read-only token), ``forbidden`` (no edit access to the model),
    ``org_scope_mismatch`` (the token is bound to a different organization than
    the model's), ``not_org_member`` (edit access without membership of the
    model's organization), ``model_without_organization`` or
    ``public_catalog_manager_only``.
    """


class InvalidDataError(TableAPIError):
    """Raised when the server refuses the uploaded data itself (HTTP 400/413/422).

    ``code`` says why — ``batch_too_large``, ``upload_too_large``,
    ``unsupported_type``, ``unsupported_value``, ``invalid_ipc`` or
    ``missing_name``.
    """


class RateLimitError(TableAPIError):
    """Raised when the API rate limit is exceeded (HTTP 429, governor).

    The ``retry_after`` attribute contains the number of seconds to wait
    before retrying (parsed from the server's ``retry_after_secs`` field),
    or ``None`` if the server did not provide it. ``code`` is
    ``rate_limit_exceeded`` or, for uploads, ``too_many_concurrent_uploads``.
    """

    def __init__(self, detail: dict[str, Any]) -> None:
        message = detail.get("message") or detail.get("error", "rate limit exceeded")
        super().__init__(message, status_code=429, code=detail.get("error"))
        self.detail = detail
        try:
            self.retry_after: int | None = int(detail["retry_after_secs"])
        except (KeyError, ValueError, TypeError):
            self.retry_after = None


class QuotaExceededError(TableAPIError):
    """Raised when the daily egress quota is exceeded (HTTP 429).

    The ``detail`` attribute contains the raw error payload from the server
    (e.g. error code and reset time when the server provides it).
    """

    def __init__(self, detail: dict[str, Any], *, status_code: int = 429) -> None:
        message = detail.get("message") or detail.get("error", "egress quota exceeded")
        super().__init__(message, status_code=status_code, code=detail.get("error"))
        self.detail = detail


class StorageQuotaExceededError(QuotaExceededError):
    """Raised when an upload does not fit in the organization's storage (HTTP 413).

    ``detail`` carries the server's figures: ``used_bytes``, ``quota_bytes``
    and ``requested_bytes``.
    """

    def __init__(self, detail: dict[str, Any]) -> None:
        super().__init__(detail, status_code=413)


class MyCorrDataWarning(UserWarning):
    """Issued when the server converted a column's values with a loss — for
    example a type MyCorr does not store natively, stored as text."""
