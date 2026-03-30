"""PyMyCorr - Python client for fetching table data from the MyCorr API."""

from importlib.metadata import version

from pymycorr.client import MyCorr
from pymycorr.exceptions import (
    QuotaExceededError,
    RateLimitError,
    StreamingError,
    TableAPIError,
    TableConversionError,
    TableNotFoundError,
)

__version__ = version("pymycorr")
__all__ = [
    "MyCorr",
    "QuotaExceededError",
    "RateLimitError",
    "StreamingError",
    "TableAPIError",
    "TableNotFoundError",
    "TableConversionError",
    "__version__",
]
