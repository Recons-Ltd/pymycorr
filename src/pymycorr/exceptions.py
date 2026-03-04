"""Custom exceptions for the PyMyCorr client."""


class TableAPIError(Exception):
    """Base exception for table API errors."""


class NotFoundError(TableAPIError):
    """Raised when a resource is not found (404)."""


class TableConversionError(TableAPIError):
    """Raised when table conversion fails."""


class StreamingError(TableAPIError):
    """Raised when an error occurs during data streaming."""

    def __init__(self, message: str, batches_received: int = 0):
        super().__init__(message)
        self.batches_received = batches_received


class JsonAPIError(TableAPIError):
    """Raised when the JSON client API returns an error response."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class AuthenticationError(JsonAPIError):
    """Raised on 401 Unauthorized responses."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=401)


class ForbiddenError(JsonAPIError):
    """Raised on 403 Forbidden responses."""

    def __init__(self, message: str) -> None:
        super().__init__(message, status_code=403)
