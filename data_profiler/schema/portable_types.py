"""Portable type schema: maps engine-specific types to 10 canonical types."""

from __future__ import annotations

import re

CANONICAL_TYPES = {
    "string": ["VARCHAR", "TEXT", "STRING", "CHAR", "NVARCHAR", "NCHAR", "CLOB", "NCLOB"],
    "integer": [
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "HUGEINT",
        "INT2", "INT4", "INT8", "INT16", "INT32", "INT64", "INT128",
        "UBIGINT", "USMALLINT", "UTINYINT", "UINTEGER", "UHUGEINT",
    ],
    "float": ["FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "NUMBER", "REAL", "FLOAT4", "FLOAT8"],
    "boolean": ["BOOLEAN", "BOOL", "BIT"],
    "datetime": [
        "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
        "DATETIME", "TIMESTAMPTZ",
    ],
    "date": ["DATE"],
    "binary": ["BYTES", "BINARY", "VARBINARY", "BLOB", "BYTEA"],
    "time": ["TIME", "TIME_WITH_TIMEZONE"],
    "semi_structured": [
        "VARIANT", "OBJECT", "ARRAY",       # Snowflake
        "MAP", "STRUCT",                      # Databricks
        "GEOGRAPHY", "GEOMETRY",              # Spatial
        "INTERVAL", "VOID",                   # Databricks misc
    ],
    "unknown": [],
}

# Reverse lookup: normalized engine type -> canonical type
_REVERSE_MAP: dict[str, str] = {}
for canonical, engine_types in CANONICAL_TYPES.items():
    for et in engine_types:
        _REVERSE_MAP[et] = canonical

# Regex for types with precision/scale like NUMBER(38,0) or VARCHAR(255)
_PARAMETERIZED_RE = re.compile(r"^([A-Z_]+)\s*\(.*\)$")


def map_type(engine_type: str) -> str:
    """Map an engine-specific type string to one of the 10 canonical types.

    Handles parameterized types like NUMBER(38,0) by stripping parameters.
    Special case: NUMBER(p,0) where scale=0 maps to integer.
    """
    normalized = engine_type.strip().upper()

    # Direct match first
    if normalized in _REVERSE_MAP:
        return _REVERSE_MAP[normalized]

    # Try stripping parameters: VARCHAR(255) -> VARCHAR
    m = _PARAMETERIZED_RE.match(normalized)
    if m:
        base = m.group(1)

        # Special case: NUMBER/NUMERIC with scale=0 is integer
        if base in ("NUMBER", "NUMERIC"):
            parts = normalized.split("(")[1].rstrip(")").split(",")
            if len(parts) == 2 and parts[1].strip() == "0":
                return "integer"

        if base in _REVERSE_MAP:
            return _REVERSE_MAP[base]

    return "unknown"
