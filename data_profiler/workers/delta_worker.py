"""Delta detection for incremental profiling."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from data_profiler.workers.schema_worker import ColumnSchema
    from data_profiler.workers.stats_worker import ProfiledTable


@dataclass
class DeltaResult:
    """Result of delta detection for a single table."""
    needs_profiling: bool
    reason: str  # "new_table", "schema_changed", "watermark_advanced", "row_count_changed", "unchanged"
    prior_profile: "ProfiledTable | None" = None
    watermark_filter: str | None = None  # SQL WHERE clause for append-only profiling


def compute_column_hash(columns: list["ColumnSchema"]) -> str:
    """SHA256 hash of sorted (column_name, canonical_type) pairs."""
    pairs = sorted((c.name, c.canonical_type) for c in columns)
    content = "|".join(f"{name}:{ctype}" for name, ctype in pairs)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def check_delta(
    engine: Any,
    table_name: str,
    schema: str | None,
    prior_metadata: dict[str, Any] | None,
    prior_profile: "ProfiledTable | None",
    watermark_column: str | None,
    current_columns: list["ColumnSchema"],
    quote_fn: Any = None,
) -> DeltaResult:
    """Determine whether a table needs re-profiling.

    Checks in order: new table → schema change → watermark advance → row count change → unchanged.
    """
    if prior_metadata is None:
        return DeltaResult(needs_profiling=True, reason="new_table")

    # 1. Schema change
    current_hash = compute_column_hash(current_columns)
    prior_hash = prior_metadata.get("column_hash", "")
    if current_hash != prior_hash:
        return DeltaResult(needs_profiling=True, reason="schema_changed")

    # 2. Watermark column (append-only detection)
    if watermark_column:
        qi = quote_fn if quote_fn else lambda x: x
        qtable = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
        qcol = qi(watermark_column)

        sql = text(f"SELECT MAX({qcol}) FROM {qtable}")
        with engine.connect() as conn:
            row = conn.execute(sql).fetchone()
            current_watermark = str(row[0]) if row and row[0] is not None else None

        prior_watermark = prior_metadata.get("watermark_value")
        if current_watermark and current_watermark != prior_watermark:
            wm_filter = None
            if prior_watermark:
                wm_filter = f"{qcol} > '{prior_watermark}'"
            return DeltaResult(
                needs_profiling=True,
                reason="watermark_advanced",
                prior_profile=prior_profile,
                watermark_filter=wm_filter,
            )

    # 3. Row count change
    qi = quote_fn if quote_fn else lambda x: x
    qtable = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    sql = text(f"SELECT COUNT(*) FROM {qtable}")
    with engine.connect() as conn:
        row = conn.execute(sql).fetchone()
        current_count = row[0] if row else 0

    prior_count = prior_metadata.get("row_count", 0)
    if current_count != prior_count:
        return DeltaResult(needs_profiling=True, reason="row_count_changed")

    # 4. Unchanged
    return DeltaResult(
        needs_profiling=False,
        reason="unchanged",
        prior_profile=prior_profile,
    )
