"""Cross-table relationship discovery: FK graph + inferred join keys."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Optional

from data_profiler.enrichment.constraints import TableConstraints
from data_profiler.workers.stats_worker import ProfiledTable

logger = logging.getLogger(__name__)


@dataclass
class Relationship:
    """A discovered relationship between two tables."""
    source_table: str
    source_columns: list[str]
    target_table: str
    target_columns: list[str]
    relationship_type: str  # "declared_fk" or "inferred"
    constraint_name: Optional[str] = None
    confidence: float = 1.0  # 1.0 for declared_fk; 0.0-1.0 for inferred
    overlap_count: Optional[int] = None


def _check_value_overlap(
    engine: Any,
    table1: str,
    table2: str,
    column: str,
    schema: str | None = None,
    limit: int = 10_000,
    timeout: int = 30,
    quote_fn: Any = None,
) -> tuple[float, int]:
    """Sample top values from both tables and compute overlap ratio.

    Returns (confidence, overlap_count). Confidence is overlap/min(sample_sizes).
    """
    from sqlalchemy import text

    qi = quote_fn if quote_fn else lambda x: x
    qcol = qi(column)
    q1 = f"{qi(schema)}.{qi(table1)}" if schema else qi(table1)
    q2 = f"{qi(schema)}.{qi(table2)}" if schema else qi(table2)

    sql = text(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT {qcol} FROM {q1} WHERE {qcol} IS NOT NULL LIMIT :lim
            INTERSECT
            SELECT {qcol} FROM {q2} WHERE {qcol} IS NOT NULL LIMIT :lim
        ) AS _overlap
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, {"lim": limit}).fetchone()
        overlap_count = row[0] if row else 0

    # Compute confidence as overlap / limit (approximation)
    confidence = min(1.0, overlap_count / max(1, limit // 10))
    if overlap_count == 0:
        confidence = 0.0
    elif confidence > 0.1:
        confidence = min(1.0, 0.5 + (confidence * 0.5))
    return confidence, overlap_count


def discover_relationships(
    results: list[ProfiledTable],
    include_inferred: bool = True,
    engine: Any | None = None,
    config: Any | None = None,
    quote_fn: Any = None,
) -> list[Relationship]:
    """Analyze profiled tables for cross-table relationships.

    1. Collect declared FKs from constraint metadata.
    2. Optionally infer relationships from matching column names + compatible types
       where one side is all_unique (potential join key).
    """
    relationships: list[Relationship] = []

    # Phase 1: Declared FKs from constraint discovery
    for table in results:
        if table.constraints is None:
            continue
        constraints = table.constraints
        if not isinstance(constraints, TableConstraints):
            # Handle dict form (from deserialization)
            if isinstance(constraints, dict):
                fks = constraints.get("foreign_keys", [])
            else:
                continue
        else:
            fks = constraints.foreign_keys

        for fk in fks:
            relationships.append(Relationship(
                source_table=table.name,
                source_columns=fk.get("constrained_columns", []),
                target_table=fk.get("referred_table", ""),
                target_columns=fk.get("referred_columns", []),
                relationship_type="declared_fk",
                constraint_name=fk.get("name"),
            ))

    if not include_inferred:
        return relationships

    # Phase 2: Inferred relationships
    # Build index of columns that are all_unique (potential PKs/join keys)
    unique_columns: dict[str, dict[str, str]] = {}  # {table: {col_name: canonical_type}}
    all_columns: dict[str, dict[str, str]] = {}  # {table: {col_name: canonical_type}}

    for table in results:
        all_columns[table.name] = {}
        unique_columns[table.name] = {}
        for col in table.columns:
            all_columns[table.name][col.name] = col.canonical_type
            if "all_unique" in col.anomalies:
                unique_columns[table.name][col.name] = col.canonical_type

    # Already-declared pairs (avoid duplicating FK relationships)
    declared_pairs = {
        (r.source_table, tuple(r.source_columns), r.target_table, tuple(r.target_columns))
        for r in relationships
    }

    # Find matching column names across tables
    table_names = list(all_columns.keys())
    for i, t1 in enumerate(table_names):
        for t2 in table_names[i + 1:]:
            for col_name, col_type in all_columns[t1].items():
                if col_name not in all_columns[t2]:
                    continue
                if all_columns[t2][col_name] != col_type:
                    continue
                # At least one side should be all_unique
                t1_unique = col_name in unique_columns.get(t1, {})
                t2_unique = col_name in unique_columns.get(t2, {})
                if not (t1_unique or t2_unique):
                    continue
                # Skip if already declared
                pair_fwd = (t1, (col_name,), t2, (col_name,))
                pair_rev = (t2, (col_name,), t1, (col_name,))
                if pair_fwd in declared_pairs or pair_rev in declared_pairs:
                    continue
                # Source is the non-unique side (FK), target is the unique side (PK)
                if t2_unique:
                    src, tgt = t1, t2
                else:
                    src, tgt = t2, t1
                conf = 0.5  # Heuristic: name+type+uniqueness match only
                overlap = None

                # Value-overlap validation when engine is available
                if engine is not None:
                    try:
                        conf, overlap = _check_value_overlap(
                            engine, src, tgt, col_name,
                            schema=getattr(config, "schema_name", None) if config else None,
                            quote_fn=quote_fn,
                        )
                    except Exception:
                        logger.warning(
                            "Overlap check failed for %s.%s ↔ %s.%s, using heuristic",
                            src, col_name, tgt, col_name,
                            exc_info=True,
                        )

                relationships.append(Relationship(
                    source_table=src,
                    source_columns=[col_name],
                    target_table=tgt,
                    target_columns=[col_name],
                    relationship_type="inferred",
                    confidence=conf,
                    overlap_count=overlap,
                ))

    return relationships


def relationships_to_dict(rels: list[Relationship]) -> dict[str, Any]:
    """Convert relationships list to a serializable trailer record."""
    return {
        "_relationships": True,
        "count": len(rels),
        "relationships": [asdict(r) for r in rels],
    }
