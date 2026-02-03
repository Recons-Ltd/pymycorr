"""PyMyCorr - Python client for fetching table data from the MyCorr API."""

from pymycorr.client import MyCorr
from pymycorr.exceptions import (
    StreamingError,
    TableAPIError,
    TableConversionError,
    TableNotFoundError,
)

__version__ = "0.1.0"
__all__ = [
    "MyCorr",
    "StreamingError",
    "TableAPIError",
    "TableNotFoundError",
    "TableConversionError",
    "__version__",
]
