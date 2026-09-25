"""PyMyCorr - Python client for fetching table data from the MyCorr API."""

from importlib.metadata import version

from pymycorr.client import MyCorr
from pymycorr.exceptions import (
    AuthenticationError,
    InvalidDataError,
    MyCorrDataWarning,
    PermissionDeniedError,
    QuotaExceededError,
    RateLimitError,
    StorageQuotaExceededError,
    StreamingError,
    TableAPIError,
    TableConversionError,
    TableNotFoundError,
)

__version__ = version("pymycorr")
__all__ = [
    "AuthenticationError",
    "InvalidDataError",
    "MyCorr",
    "MyCorrDataWarning",
    "PermissionDeniedError",
    "QuotaExceededError",
    "RateLimitError",
    "StorageQuotaExceededError",
    "StreamingError",
    "TableAPIError",
    "TableNotFoundError",
    "TableConversionError",
    "__version__",
]
