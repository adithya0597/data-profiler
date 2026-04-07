"""Schema discovery: table/column metadata with comments and type mapping."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import inspect, text, Engine

from data_profiler.schema.portable_types import map_type

logger = logging.getLogger(__name__)


@dataclass
class ColumnSchema:
    """Schema metadata for a single column."""
    name: str
    engine_type: str
    canonical_type: str
    nullable: bool
    comment: str | None = None


@dataclass
class TableSchema:
    """Schema metadata for a table."""
    name: str
    columns: list[ColumnSchema] = field(default_factory=list)
    comment: str | None = None


def discover_tables(engine: Engine, schema: str | None = None) -> list[str]:
    """List all base table names in the given schema."""
    insp = inspect(engine)
    return insp.get_table_names(schema=schema)


def discover_schema(engine: Engine, table_name: str, schema: str | None = None) -> TableSchema:
    """Discover full schema for a single table including column types and comments."""
    insp = inspect(engine)

    # Table comment
    table_comment = None
    try:
        comment_info = insp.get_table_comment(table_name, schema=schema)
        table_comment = comment_info.get("text")
    except (NotImplementedError, Exception):
        pass

    # Column metadata
    columns = []
    try:
        raw_columns = insp.get_columns(table_name, schema=schema)
    except Exception as e:
        logger.error("Failed to get columns for %s: %s", table_name, e)
        return TableSchema(name=table_name, comment=table_comment)

    for col in raw_columns:
        engine_type = str(col["type"])
        columns.append(ColumnSchema(
            name=col["name"],
            engine_type=engine_type,
            canonical_type=map_type(engine_type),
            nullable=col.get("nullable", True),
            comment=col.get("comment"),
        ))

    return TableSchema(name=table_name, columns=columns, comment=table_comment)


def get_row_count(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
    quote_fn: Any = None,
) -> int:
    """Get exact row count for a table."""
    qi = quote_fn if quote_fn else lambda x: f'"{x}"'
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {qualified}"))
        return result.scalar() or 0
