"""PyMyCorr - Python client for fetching table data from the MyCorr API."""

from pymycorr._json_api import (
    CheckpointInfo,
    ColumnData,
    ColumnMetadata,
    ModelMetadata,
    ResponseMeta,
    TableDataPage,
    TableDataPayload,
    TableInfo,
    TableMetadata,
)
from pymycorr.client import MyCorr
from pymycorr.exceptions import (
    AuthenticationError,
    ForbiddenError,
    JsonAPIError,
    NotFoundError,
    StreamingError,
    TableAPIError,
    TableConversionError,
)

__version__ = "0.1.0"
__all__ = [
    "AuthenticationError",
    "CheckpointInfo",
    "ColumnData",
    "ColumnMetadata",
    "ForbiddenError",
    "JsonAPIError",
    "ModelMetadata",
    "MyCorr",
    "ResponseMeta",
    "StreamingError",
    "TableAPIError",
    "TableConversionError",
    "TableDataPage",
    "TableDataPayload",
    "TableInfo",
    "TableMetadata",
    "NotFoundError",
    "__version__",
]
