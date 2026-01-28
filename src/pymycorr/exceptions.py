"""Custom exceptions for the PyMyCorr client."""


class TableAPIError(Exception):
    """Base exception for table API errors."""


class TableNotFoundError(TableAPIError):
    """Raised when table is not found."""


class TableConversionError(TableAPIError):
    """Raised when table conversion fails."""
