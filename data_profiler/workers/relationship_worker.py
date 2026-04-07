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
            SELECT {qcol} FROM (SELECT {qcol} FROM {q1} WHERE {qcol} IS NOT NULL LIMIT :lim) _a
            INTERSECT
            SELECT {qcol} FROM (SELECT {qcol} FROM {q2} WHERE {qcol} IS NOT NULL LIMIT :lim) _b
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


_SEMANTIC_SUFFIXES = ("_sk", "_id", "_key")


def _extract_entity_hint(col_name: str, source_table: str) -> str | None:
    """Extract entity name from a column like 'ss_customer_sk' → 'customer'.

    Strategy: strip the known FK suffix, then strip the source table's column prefix
    (e.g., 'ss_' for store_sales) if present.
    """
    lower = col_name.lower()
    for suffix in _SEMANTIC_SUFFIXES:
        if lower.endswith(suffix):
            stem = col_name[:len(col_name) - len(suffix)]
            # TPC-DS pattern: columns are prefixed with table abbreviation (ss_, sr_, ws_, etc.)
            # Try stripping longest matching prefix
            parts = stem.split("_")
            if len(parts) >= 2:
                # Try without first segment (e.g., "ss_customer" → "customer")
                entity = "_".join(parts[1:])
                if entity:
                    return entity
            # Fallback: use the full stem
            if stem:
                return stem
    return None


def _discover_semantic_fks(
    results: list[ProfiledTable],
    existing: list["Relationship"],
    engine: Any | None = None,
    config: Any | None = None,
    quote_fn: Any = None,
) -> list["Relationship"]:
    """Discover FK relationships via naming conventions (*_sk, *_id, *_key).

    Matches column entity hints against table names and their PK columns.
    """
    # Build lookup: table_name → {col_name: canonical_type}
    table_cols: dict[str, dict[str, str]] = {}
    # Build lookup: table_name → set of unique column names
    unique_cols: dict[str, set[str]] = {}
    table_set = set()

    for t in results:
        table_set.add(t.name)
        table_cols[t.name] = {}
        unique_cols[t.name] = set()
        for col in t.columns:
            table_cols[t.name][col.name] = col.canonical_type
            if "all_unique" in col.anomalies:
                unique_cols[t.name].add(col.name)

    # Already-seen pairs to avoid duplicates
    seen = {
        (r.source_table, tuple(r.source_columns), r.target_table, tuple(r.target_columns))
        for r in existing
    }
    seen |= {
        (r.target_table, tuple(r.target_columns), r.source_table, tuple(r.source_columns))
        for r in existing
    }

    rels: list[Relationship] = []

    for source_table in results:
        for col in source_table.columns:
            entity = _extract_entity_hint(col.name, source_table.name)
            if entity is None:
                continue

            # Try matching entity against table names (exact, _dim suffix, plural)
            candidates = [entity, f"{entity}_dim", f"{entity}s"]
            for target_name in candidates:
                if target_name not in table_set or target_name == source_table.name:
                    continue

                # Find matching PK column in target table
                # Candidates: exact patterns + any target column ending in {entity}_{suffix}
                target_pk_candidates = [
                    f"{entity}_sk", f"{entity}_id", f"{entity}_key",
                    col.name,  # exact match fallback
                ]
                # Also search target columns that end with entity_sk/id/key (e.g., c_customer_sk)
                for tcol_name in table_cols.get(target_name, {}):
                    for sfx in _SEMANTIC_SUFFIXES:
                        if tcol_name.endswith(f"{entity}{sfx}") and tcol_name not in target_pk_candidates:
                            target_pk_candidates.append(tcol_name)

                for pk_col in target_pk_candidates:
                    if pk_col not in table_cols.get(target_name, {}):
                        continue
                    # Type compatibility check
                    src_type = col.canonical_type
                    tgt_type = table_cols[target_name][pk_col]
                    if src_type != tgt_type:
                        continue
                    # Target column should be unique (it's a PK)
                    if pk_col not in unique_cols.get(target_name, set()):
                        continue

                    pair_fwd = (source_table.name, (col.name,), target_name, (pk_col,))
                    pair_rev = (target_name, (pk_col,), source_table.name, (col.name,))
                    if pair_fwd in seen or pair_rev in seen:
                        continue

                    conf = 0.6  # Heuristic: naming pattern + type + uniqueness
                    overlap = None
                    if engine is not None:
                        try:
                            conf, overlap = _check_value_overlap(
                                engine, source_table.name, target_name, col.name,
                                schema=getattr(config, "schema_name", None) if config else None,
                                quote_fn=quote_fn,
                            )
                        except Exception:
                            logger.warning(
                                "Semantic FK overlap check failed: %s.%s → %s.%s",
                                source_table.name, col.name, target_name, pk_col,
                            )

                    rels.append(Relationship(
                        source_table=source_table.name,
                        source_columns=[col.name],
                        target_table=target_name,
                        target_columns=[pk_col],
                        relationship_type="semantic_fk",
                        confidence=conf,
                        overlap_count=overlap,
                    ))
                    seen.add(pair_fwd)
                    break  # Found a match for this entity, stop trying PK candidates
                else:
                    continue
                break  # Found a matching target table, stop trying candidates

    return rels


def _check_multi_column_overlap(
    engine: Any,
    table1: str,
    table2: str,
    columns: list[str],
    schema: str | None = None,
    limit: int = 10_000,
    quote_fn: Any = None,
) -> tuple[float, int]:
    """Check value overlap for multiple columns (composite key)."""
    from sqlalchemy import text

    qi = quote_fn if quote_fn else lambda x: x
    q1 = f"{qi(schema)}.{qi(table1)}" if schema else qi(table1)
    q2 = f"{qi(schema)}.{qi(table2)}" if schema else qi(table2)

    col_list = ", ".join(qi(c) for c in columns)
    null_checks = " AND ".join(f"{qi(c)} IS NOT NULL" for c in columns)

    sql = text(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT {col_list} FROM (SELECT {col_list} FROM {q1} WHERE {null_checks} LIMIT :lim) _a
            INTERSECT
            SELECT {col_list} FROM (SELECT {col_list} FROM {q2} WHERE {null_checks} LIMIT :lim) _b
        ) AS _overlap
    """)

    with engine.connect() as conn:
        row = conn.execute(sql, {"lim": limit}).fetchone()
        overlap_count = row[0] if row else 0

    confidence = min(1.0, overlap_count / max(1, limit // 10))
    if overlap_count == 0:
        confidence = 0.0
    elif confidence > 0.1:
        confidence = min(1.0, 0.5 + (confidence * 0.5))
    return confidence, overlap_count


def _discover_composite_fks(
    results: list[ProfiledTable],
    existing: list["Relationship"],
    engine: Any | None = None,
    config: Any | None = None,
    quote_fn: Any = None,
) -> list["Relationship"]:
    """Discover composite key relationships by matching declared multi-column PKs."""
    # Build lookup: table_name → {col_name: canonical_type}
    table_cols: dict[str, dict[str, str]] = {}
    # Build lookup: table_name → composite PK columns (if multi-column)
    composite_pks: dict[str, list[str]] = {}

    for t in results:
        table_cols[t.name] = {col.name: col.canonical_type for col in t.columns}
        if t.constraints and isinstance(t.constraints, TableConstraints):
            pk = t.constraints.primary_key
            if pk and len(pk) >= 2:
                composite_pks[t.name] = pk

    if not composite_pks:
        return []

    seen = {
        (r.source_table, tuple(r.source_columns), r.target_table, tuple(r.target_columns))
        for r in existing
    }
    seen |= {
        (r.target_table, tuple(r.target_columns), r.source_table, tuple(r.source_columns))
        for r in existing
    }

    rels: list[Relationship] = []

    for pk_table, pk_cols in composite_pks.items():
        pk_types = [table_cols[pk_table].get(c) for c in pk_cols]
        if None in pk_types:
            continue

        for t in results:
            if t.name == pk_table:
                continue
            # Check if all PK columns exist in this table with compatible types
            all_match = True
            for col_name, col_type in zip(pk_cols, pk_types):
                if col_name not in table_cols.get(t.name, {}):
                    all_match = False
                    break
                if table_cols[t.name][col_name] != col_type:
                    all_match = False
                    break
            if not all_match:
                continue

            pair_fwd = (t.name, tuple(pk_cols), pk_table, tuple(pk_cols))
            pair_rev = (pk_table, tuple(pk_cols), t.name, tuple(pk_cols))
            if pair_fwd in seen or pair_rev in seen:
                continue

            conf = 0.5
            overlap = None
            if engine is not None:
                try:
                    conf, overlap = _check_multi_column_overlap(
                        engine, t.name, pk_table, pk_cols,
                        schema=getattr(config, "schema_name", None) if config else None,
                        quote_fn=quote_fn,
                    )
                except Exception:
                    logger.warning(
                        "Composite overlap check failed: %s → %s on %s",
                        t.name, pk_table, pk_cols,
                    )

            rels.append(Relationship(
                source_table=t.name,
                source_columns=list(pk_cols),
                target_table=pk_table,
                target_columns=list(pk_cols),
                relationship_type="inferred_composite",
                confidence=conf,
                overlap_count=overlap,
            ))
            seen.add(pair_fwd)

    return rels


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

    # Phase 1.5: Semantic FK discovery (naming patterns like *_sk, *_id, *_key)
    semantic_rels = _discover_semantic_fks(
        results, relationships, engine=engine, config=config, quote_fn=quote_fn,
    )
    relationships.extend(semantic_rels)

    # Phase 1.75: Composite key relationship detection
    composite_rels = _discover_composite_fks(
        results, relationships, engine=engine, config=config, quote_fn=quote_fn,
    )
    relationships.extend(composite_rels)

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
