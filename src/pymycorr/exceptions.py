"""Custom exceptions for the PyMyCorr client."""


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
