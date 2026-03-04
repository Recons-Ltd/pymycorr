"""TypedDict definitions for JSON client API response types."""

from __future__ import annotations

from typing import Any, TypedDict


class ModelMetadata(TypedDict):
    """Model metadata returned by /models endpoints."""

    model_id: str
    model_name: str | None
    description: str | None
    last_modified_at: int
    created_at: int


class TableInfo(TypedDict):
    """Table summary returned by /models/{id}/tables."""

    name: str
    table_id: str
    model_id: str
    latest_version: int


class ColumnMetadata(TypedDict):
    """Column metadata within a table schema."""

    id: str
    name: str | None
    data_type: str
    is_nullable: bool


class TableMetadata(TypedDict):
    """Table metadata returned by /tables/{id}."""

    table_id: str
    name: str
    shape: str
    version: int
    row_count: int
    column_count: int
    created_at: int
    last_modified: int
    columns: list[ColumnMetadata]


class CheckpointInfo(TypedDict):
    """Checkpoint metadata returned by /tables/{id}/checkpoints."""

    name: str
    aliases: list[str]
    version: int
    created_at: int
    version_timestamp: int


class ColumnData(TypedDict):
    """Column-oriented data for a single column."""

    id: str
    name: str
    data_type: str
    data: list[Any]


class TableDataPayload(TypedDict):
    """Table data in column-oriented format."""

    table_id: str
    name: str
    created_at: int
    columns: list[ColumnData]


class ResponseMeta(TypedDict, total=False):
    """Pagination metadata."""

    next_page_token: str
    total_rows: int
    total_columns: int


class TableDataPage(TypedDict):
    """A single page of table data with metadata."""

    data: TableDataPayload
    meta: ResponseMeta
