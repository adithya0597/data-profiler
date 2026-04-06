"""Auto-generate constraint suggestions from profiling data."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable


@dataclass
class SuggestedConstraint:
    table: str
    column: str
    constraint_type: str  # "NOT NULL", "UNIQUE", "CHECK"
    expression: str
    confidence: float
    evidence: str


def suggest_constraints(table: ProfiledTable) -> list[dict]:
    """Generate constraint suggestions from a profiled table's statistics."""
    suggestions: list[SuggestedConstraint] = []

    for col in table.columns:
        # NOT NULL: column has zero nulls but is declared nullable
        if col.null_rate == 0.0 and col.nullable and table.total_row_count > 0:
            suggestions.append(SuggestedConstraint(
                table=table.name,
                column=col.name,
                constraint_type="NOT NULL",
                expression=f"ALTER TABLE {table.name} ALTER COLUMN {col.name} SET NOT NULL",
                confidence=0.95,
                evidence=f"0 nulls across {table.total_row_count} rows",
            ))

        # UNIQUE: all values are unique (all_unique anomaly)
        if "all_unique" in col.anomalies:
            suggestions.append(SuggestedConstraint(
                table=table.name,
                column=col.name,
                constraint_type="UNIQUE",
                expression=f"ALTER TABLE {table.name} ADD CONSTRAINT uq_{table.name}_{col.name} UNIQUE ({col.name})",
                confidence=0.9,
                evidence=f"{col.approx_distinct} distinct values across {table.total_row_count} rows",
            ))

        # CHECK: non-negative for numeric columns with min >= 0
        if (col.canonical_type in ("integer", "float")
                and col.min is not None
                and col.min >= 0
                and col.negative_count is not None
                and col.negative_count == 0
                and table.total_row_count > 0):
            suggestions.append(SuggestedConstraint(
                table=table.name,
                column=col.name,
                constraint_type="CHECK",
                expression=f"ALTER TABLE {table.name} ADD CONSTRAINT ck_{table.name}_{col.name}_nonneg CHECK ({col.name} >= 0)",
                confidence=0.85,
                evidence=f"min={col.min}, no negative values in {table.total_row_count} rows",
            ))

    return [asdict(s) for s in suggestions]
