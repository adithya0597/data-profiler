"""OpenMetadata-compatible JSON export.

Produces a static JSON file matching OpenMetadata's table/column profile schema.
This is a one-way export (no API calls) for catalog ingestion.

Reference: https://docs.open-metadata.org/v1.3.x/main-concepts/metadata-standard/schemas
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from data_profiler.workers.stats_worker import ProfiledTable


def _column_profile(col: dict[str, Any], total_rows: int) -> dict[str, Any]:
    """Convert a ColumnProfile dict to OpenMetadata columnProfile format."""
    profile: dict[str, Any] = {
        "name": col["name"],
        "dataType": col["canonical_type"].upper(),
        "dataTypeDisplay": col["engine_type"],
    }

    # Stats
    stats: dict[str, Any] = {}
    if total_rows > 0:
        stats["valuesCount"] = total_rows - col.get("null_count", 0)
        stats["nullCount"] = col.get("null_count", 0)
        stats["nullProportion"] = col.get("null_rate", 0.0)
    stats["uniqueCount"] = col.get("approx_distinct", 0)
    if total_rows > 0 and col.get("approx_distinct"):
        stats["uniqueProportion"] = col["approx_distinct"] / total_rows
    if col.get("min") is not None:
        stats["min"] = str(col["min"])
    if col.get("max") is not None:
        stats["max"] = str(col["max"])
    if col.get("mean") is not None:
        stats["mean"] = col["mean"]
    if col.get("stddev") is not None:
        stats["stddev"] = col["stddev"]
    if col.get("median") is not None:
        stats["median"] = col["median"]
    if col.get("max_length") is not None:
        stats["maxLength"] = col["max_length"]

    # Patterns (custom extension, not in base OpenMetadata schema)
    if col.get("patterns"):
        stats["customMetrics"] = [
            {"name": f"pattern_{p}", "value": col.get("pattern_scores", {}).get(p, 0)}
            for p in col["patterns"]
        ]

    profile["profile"] = stats
    return profile


def _table_profile(table: ProfiledTable, relationships: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Convert a ProfiledTable to OpenMetadata table format."""
    d = asdict(table)
    columns = [_column_profile(c, table.total_row_count) for c in d["columns"]]

    result: dict[str, Any] = {
        "name": table.name,
        "description": table.comment or "",
        "tableType": "Regular",
        "columns": columns,
        "tableProfile": {
            "timestamp": table.profiled_at,
            "rowCount": table.total_row_count,
            "columnCount": len(table.columns),
            "sampleCount": table.sampled_row_count,
        },
    }

    if table.duplicate_row_count > 0:
        result["tableProfile"]["duplicateCount"] = table.duplicate_row_count
        result["tableProfile"]["duplicateProportion"] = table.duplicate_rate

    # Constraints
    if table.constraints is not None:
        constraints_out = []
        cstr = table.constraints
        if isinstance(cstr, dict):
            pk = cstr.get("primary_key", [])
            fks = cstr.get("foreign_keys", [])
            uqs = cstr.get("unique_constraints", [])
        else:
            pk = getattr(cstr, "primary_key", [])
            fks = getattr(cstr, "foreign_keys", [])
            uqs = getattr(cstr, "unique_constraints", [])

        if pk:
            constraints_out.append({"constraintType": "PRIMARY_KEY", "columns": pk})
        for fk in fks:
            constraints_out.append({
                "constraintType": "FOREIGN_KEY",
                "columns": fk.get("constrained_columns", []),
                "referredColumns": [
                    f"{fk.get('referred_table', '')}.{c}"
                    for c in fk.get("referred_columns", [])
                ],
            })
        for uq in uqs:
            cols = uq.get("columns", uq.get("column_names", []))
            constraints_out.append({"constraintType": "UNIQUE", "columns": cols})

        if constraints_out:
            result["tableConstraints"] = constraints_out

    return result


def export_openmetadata(
    results: list[ProfiledTable],
    output_path: str,
    run_id: str = "",
    engine: str = "",
    database: str | None = None,
    schema: str | None = None,
    relationships: list[dict[str, Any]] | None = None,
) -> None:
    """Export profiling results as OpenMetadata-compatible JSON."""
    tables = [_table_profile(t) for t in results if not t.error]

    document: dict[str, Any] = {
        "openMetadataExport": True,
        "version": "1.0",
        "runId": run_id,
        "source": {
            "engine": engine,
            "database": database or "",
            "schema": schema or "",
        },
        "tables": tables,
    }

    if relationships:
        document["relationships"] = relationships

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps(document, indent=2, default=str))
