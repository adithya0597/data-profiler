"""Pattern detection: regex-based PII and format identification on string columns."""

from __future__ import annotations

import re
from typing import Any


# Each entry: (pattern_name, compiled_regex, min_match_rate_to_flag)
# min_match_rate: fraction of non-null sampled values that must match to tag the column.
# Lower thresholds for sensitive patterns (SSN, credit card) to catch partial PII.
PATTERN_REGISTRY: list[tuple[str, re.Pattern, float]] = [
    ("email", re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    ), 0.5),
    ("phone_us", re.compile(
        r"^\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$"
    ), 0.5),
    ("ssn", re.compile(
        r"^\d{3}-\d{2}-\d{4}$"
    ), 0.3),
    ("uuid", re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        re.IGNORECASE,
    ), 0.5),
    ("ipv4", re.compile(
        r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
    ), 0.5),
    ("credit_card", re.compile(
        r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$"
    ), 0.3),
    ("url", re.compile(
        r"^https?://\S+$"
    ), 0.5),
    ("date_string", re.compile(
        r"^\d{4}-\d{2}-\d{2}$"
    ), 0.5),
]


def detect_patterns(
    values: list[str],
    min_match_rate: float = 0.1,
) -> tuple[list[str], dict[str, float]]:
    """Test sampled string values against known patterns.

    Args:
        values: Non-null string values from a sampled column.
        min_match_rate: Global minimum match rate override (patterns also have
            their own per-pattern threshold).

    Returns:
        (matched_pattern_names, {pattern_name: match_rate})
    """
    if not values:
        return [], {}

    total = len(values)
    matched: list[str] = []
    scores: dict[str, float] = {}

    for name, regex, threshold in PATTERN_REGISTRY:
        hits = sum(1 for v in values if regex.match(v.strip()))
        rate = hits / total
        if rate >= max(threshold, min_match_rate):
            matched.append(name)
            scores[name] = round(rate, 3)

    return matched, scores


def fetch_string_sample(
    engine: Any,
    table_name: str,
    column_names: list[str],
    sample_clause: str,
    schema: str | None = None,
    limit: int = 1000,
    quote_fn: Any = None,
) -> dict[str, list[str]]:
    """Fetch sampled string values for pattern detection.

    Returns {column_name: [values]} for each column. Batches columns
    to avoid excessive queries.
    """
    from sqlalchemy import text

    if not column_names:
        return {}

    qi = quote_fn if quote_fn else lambda x: x
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    result: dict[str, list[str]] = {col: [] for col in column_names}

    # Batch columns (same batch size logic as stats queries)
    batch_size = 20  # Fewer than stats batches since we fetch raw rows
    batches = [column_names[i:i + batch_size] for i in range(0, len(column_names), batch_size)]

    for batch in batches:
        select_cols = ", ".join(qi(c) for c in batch)

        if sample_clause and sample_clause.startswith("WHERE"):
            query = f"SELECT {select_cols} FROM {qualified} {sample_clause} LIMIT {limit}"
        elif sample_clause:
            query = f"SELECT {select_cols} FROM {qualified} {sample_clause} LIMIT {limit}"
        else:
            query = f"SELECT {select_cols} FROM {qualified} LIMIT {limit}"

        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()

        for row in rows:
            mapping = row._mapping
            for col in batch:
                val = mapping.get(col)
                if val is not None:
                    result[col].append(str(val))

    return result
