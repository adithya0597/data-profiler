"""Auto-generate constraint suggestions from profiling data."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable
    from data_profiler.workers.relationship_worker import Relationship


@dataclass
class SuggestedConstraint:
    table: str
    column: str
    constraint_type: str  # "NOT NULL", "UNIQUE", "CHECK"
    expression: str
    confidence: float
    evidence: str


def suggest_constraints(table: ProfiledTable, quote_fn=None) -> list[dict]:
    """Generate constraint suggestions from a profiled table's statistics."""
    if quote_fn is None:
        quote_fn = lambda name: f'"{name}"'
    suggestions: list[SuggestedConstraint] = []

    for col in table.columns:
        # NOT NULL: column has zero nulls but is declared nullable
        if col.null_rate == 0.0 and col.nullable and table.total_row_count > 0:
            suggestions.append(SuggestedConstraint(
                table=table.name,
                column=col.name,
                constraint_type="NOT NULL",
                expression=f"ALTER TABLE {quote_fn(table.name)} ALTER COLUMN {quote_fn(col.name)} SET NOT NULL",
                confidence=0.95,
                evidence=f"0 nulls across {table.total_row_count} rows",
            ))

        # UNIQUE: all values are unique (all_unique anomaly)
        if "all_unique" in col.anomalies:
            suggestions.append(SuggestedConstraint(
                table=table.name,
                column=col.name,
                constraint_type="UNIQUE",
                expression=f"ALTER TABLE {quote_fn(table.name)} ADD CONSTRAINT uq_{table.name}_{col.name} UNIQUE ({quote_fn(col.name)})",
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
                expression=f"ALTER TABLE {quote_fn(table.name)} ADD CONSTRAINT ck_{table.name}_{col.name}_nonneg CHECK ({quote_fn(col.name)} >= 0)",
                confidence=0.85,
                evidence=f"min={col.min}, no negative values in {table.total_row_count} rows",
            ))

        # ENUM: low-cardinality string columns → CHECK (col IN (...))
        if (col.canonical_type == "string"
                and col.top_values
                and 1 < col.approx_distinct <= 10
                and table.total_row_count > 0):
            # Only suggest if top_values aren't PII-redacted
            values = [tv["value"] for tv in col.top_values if tv["value"] != "[REDACTED]"]
            if len(values) == col.approx_distinct:
                escaped = [v.replace("'", "''") for v in values]
                in_list = ", ".join(f"'{v}'" for v in escaped)
                # Scale confidence with evidence: 100+ rows → 0.90, 20+ → 0.80, <20 → 0.70
                rows = table.total_row_count
                conf = 0.90 if rows >= 100 else (0.80 if rows >= 20 else 0.70)
                suggestions.append(SuggestedConstraint(
                    table=table.name,
                    column=col.name,
                    constraint_type="CHECK",
                    expression=f"ALTER TABLE {quote_fn(table.name)} ADD CONSTRAINT ck_{table.name}_{col.name}_enum CHECK ({quote_fn(col.name)} IN ({in_list}))",
                    confidence=conf,
                    evidence=f"{col.approx_distinct} distinct values across {table.total_row_count} rows",
                ))

    return [asdict(s) for s in suggestions]


def suggest_fk_constraints(
    relationships: list[Relationship],
    quote_fn=None,
) -> list[dict]:
    """Generate FK constraint suggestions from discovered relationships."""
    if quote_fn is None:
        quote_fn = lambda name: f'"{name}"'
    suggestions: list[SuggestedConstraint] = []

    for rel in relationships:
        src_cols = ", ".join(quote_fn(c) for c in rel.source_columns)
        tgt_cols = ", ".join(quote_fn(c) for c in rel.target_columns)
        col_suffix = "_".join(rel.source_columns)
        suggestions.append(SuggestedConstraint(
            table=rel.source_table,
            column=rel.source_columns[0] if len(rel.source_columns) == 1 else f"({', '.join(rel.source_columns)})",
            constraint_type="FK",
            expression=(
                f"ALTER TABLE {quote_fn(rel.source_table)} "
                f"ADD CONSTRAINT fk_{rel.source_table}_{col_suffix} "
                f"FOREIGN KEY ({src_cols}) REFERENCES {quote_fn(rel.target_table)} ({tgt_cols})"
            ),
            confidence=rel.confidence,
            evidence=f"{rel.relationship_type}: {rel.source_table}.{src_cols} → {rel.target_table}.{tgt_cols}",
        ))

    return [asdict(s) for s in suggestions]
