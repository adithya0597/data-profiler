"""Stats worker: type-aware aggregate dispatch, single SQL pass per table."""

from __future__ import annotations

import logging
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text, Engine

from data_profiler.adapters.base import BaseAdapter
from data_profiler.config import ProfilerConfig
from data_profiler.enrichment.anomaly import apply_anomaly_rules
from data_profiler.enrichment.patterns import detect_patterns, fetch_string_sample
from data_profiler.workers.schema_worker import (
    ColumnSchema,
    TableSchema,
    discover_schema,
    get_row_count,
)

logger = logging.getLogger(__name__)


@dataclass
class ColumnProfile:
    """Profile for a single column."""
    name: str
    engine_type: str
    canonical_type: str
    comment: Optional[str]
    nullable: bool
    null_count: int = 0
    null_rate: float = 0.0
    min: Any = None
    max: Any = None
    mean: Optional[float] = None
    sum: Optional[float] = None
    stddev: Optional[float] = None
    variance: Optional[float] = None
    median: Optional[float] = None
    p5: Optional[float] = None
    p25: Optional[float] = None
    p75: Optional[float] = None
    p95: Optional[float] = None
    iqr: Optional[float] = None
    range: Optional[float] = None
    cv: Optional[float] = None
    mad: Optional[float] = None  # median absolute deviation
    approx_distinct: int = 0
    distinct_mode: str = "approx"
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    avg_length: Optional[float] = None
    zero_count: Optional[int] = None
    negative_count: Optional[int] = None
    infinite_count: Optional[int] = None  # count of +inf/-inf values (float columns only)
    empty_count: Optional[int] = None     # count of exactly-empty strings ("")
    whitespace_count: Optional[int] = None  # count of whitespace-only strings
    leading_trailing_whitespace_count: Optional[int] = None  # strings with leading/trailing spaces
    unique_count: Optional[int] = None      # distinct values with exactly one occurrence
    uniqueness_ratio: Optional[float] = None  # unique_count / approx_distinct
    true_count: Optional[int] = None
    false_count: Optional[int] = None
    true_rate: Optional[float] = None
    false_rate: Optional[float] = None
    imbalance_ratio: Optional[float] = None  # max(true_rate,false_rate)/min for boolean
    distinct_ratio: Optional[float] = None   # approx_distinct / sampled_row_count
    pk_candidate: bool = False               # True if column is near-unique with no nulls
    box_plot: Optional[dict] = None          # {q1, median, q3, lower_fence, upper_fence, lower_whisker, upper_whisker}
    freshness_days: Optional[float] = None  # days since max date/datetime value
    date_range_days: Optional[int] = None  # days between min and max date
    granularity_guess: Optional[str] = None  # "daily", "weekly", "monthly", "yearly", "unknown"
    skewness: Optional[float] = None
    kurtosis: Optional[float] = None
    is_monotonic_increasing: Optional[bool] = None
    is_monotonic_decreasing: Optional[bool] = None
    kde: Optional[list[dict]] = None  # KDE curve [{x, y}, ...] via Silverman bandwidth
    histogram: Optional[list[dict]] = None
    cdf: Optional[list[dict]] = None  # cumulative distribution [{x, cumulative_pct}, ...]
    qq_plot: Optional[list[dict]] = None  # Q-Q plot [{theoretical, actual}, ...] (normality check)
    length_histogram: Optional[list[dict]] = None  # for strings: [{length, count}, ...]
    benford_digits: Optional[list[dict]] = None
    benford_pvalue: Optional[float] = None
    top_values: Optional[list[dict[str, Any]]] = None
    bottom_values: Optional[list[dict[str, Any]]] = None  # least frequent values
    patterns: list[str] = field(default_factory=list)
    pattern_scores: dict[str, float] = field(default_factory=dict)
    anomalies: list[str] = field(default_factory=list)


@dataclass
class ProfiledTable:
    """Profile for an entire table."""
    name: str
    comment: Optional[str]
    total_row_count: int = 0
    sampled_row_count: int = 0
    sample_size: int = 0
    full_scan: bool = False
    profiled_at: str = ""
    duration_seconds: float = 0.0
    columns: list[ColumnProfile] = field(default_factory=list)
    constraints: Optional[Any] = None  # TableConstraints from enrichment.constraints
    correlations: Optional[list[dict]] = None
    suggested_constraints: Optional[list[dict]] = None
    duplicate_row_count: int = 0
    duplicate_rate: float = 0.0
    row_completeness_min: Optional[float] = None  # min fraction of non-null cols per row
    row_completeness_max: Optional[float] = None
    row_completeness_mean: Optional[float] = None
    functional_dependencies: Optional[list[dict]] = None  # [{from, to}]
    quality_score: float = 0.0  # 0-100 scale, computed from anomalies/nulls/dupes
    error: Optional[str] = None


def compute_quality_score(table: "ProfiledTable") -> float:
    """Compute a 0-100 data quality score matching the dashboard JS formula."""
    if table.error:
        return 0.0
    if not table.columns:
        return 100.0
    score = 100.0
    # Anomaly penalty: 3 pts per anomaly, max 30
    total_anomalies = sum(len(c.anomalies) for c in table.columns)
    score -= min(30.0, total_anomalies * 3.0)
    # Null penalty: avg null rate * 40, max 20
    avg_null = sum(c.null_rate for c in table.columns) / len(table.columns)
    score -= min(20.0, avg_null * 40.0)
    # Duplicate penalty: duplicate_rate * 100, max 15
    if table.duplicate_rate > 0:
        score -= min(15.0, table.duplicate_rate * 100.0)
    return max(0.0, round(score, 1))


# Type-aware aggregate dispatch table.
# Each canonical type maps to a list of (sql_template, alias_suffix) tuples.
# Templates use {col} for column name. The alias is {col}_{suffix}.
AGGREGATE_MAP: dict[str, list[tuple[str, str]]] = {
    "integer": [
        ("COUNT({col})", "non_null"),
        ("MIN({col})", "min"),
        ("MAX({col})", "max"),
        ("AVG(CAST({col} AS DOUBLE))", "mean"),
        ("SUM(CAST({col} AS DOUBLE))", "sum"),
        ("SUM(CASE WHEN {col} = 0 THEN 1 ELSE 0 END)", "zero_count"),
        ("SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END)", "negative_count"),
        # stddev handled separately per adapter
    ],
    "float": [
        ("COUNT({col})", "non_null"),
        ("MIN({col})", "min"),
        ("MAX({col})", "max"),
        ("AVG(CAST({col} AS DOUBLE))", "mean"),
        ("SUM(CAST({col} AS DOUBLE))", "sum"),
        ("SUM(CASE WHEN {col} = 0 THEN 1 ELSE 0 END)", "zero_count"),
        ("SUM(CASE WHEN {col} < 0 THEN 1 ELSE 0 END)", "negative_count"),
        # Infinity detection: values beyond IEEE 754 double max (1.7976931e+308)
        ("SUM(CASE WHEN ABS(CAST({col} AS DOUBLE)) > 1.7976931348623157e+308 THEN 1 ELSE 0 END)", "infinite_count"),
    ],
    "string": [
        ("COUNT({col})", "non_null"),
        ("MIN({col})", "min"),
        ("MAX({col})", "max"),
        ("MIN(LENGTH({col}))", "min_length"),
        ("MAX(LENGTH({col}))", "max_length"),
        ("AVG(LENGTH({col}))", "avg_length"),
        # Empty strings: exactly zero-length
        ("SUM(CASE WHEN {col} = '' THEN 1 ELSE 0 END)", "empty_count"),
        # Whitespace-only strings: length > 0 but trimmed length = 0
        ("SUM(CASE WHEN LENGTH({col}) > 0 AND LENGTH(TRIM({col})) = 0 THEN 1 ELSE 0 END)", "whitespace_count"),
        # Leading/trailing whitespace: non-empty strings where col != TRIM(col)
        ("SUM(CASE WHEN {col} != TRIM({col}) AND {col} != '' THEN 1 ELSE 0 END)", "leading_trailing_whitespace_count"),
    ],
    "date": [
        ("COUNT({col})", "non_null"),
        ("MIN({col})", "min"),
        ("MAX({col})", "max"),
    ],
    "datetime": [
        ("COUNT({col})", "non_null"),
        ("MIN({col})", "min"),
        ("MAX({col})", "max"),
    ],
    "boolean": [
        ("COUNT({col})", "non_null"),
        ("SUM(CAST({col} AS INT))", "true_count"),
    ],
    "binary": [
        ("COUNT({col})", "non_null"),
    ],
    "unknown": [
        ("COUNT({col})", "non_null"),
    ],
}


def _quote_alias(alias: str) -> str:
    """Quote a SQL alias to handle column names with spaces or special characters."""
    escaped = alias.replace('"', '""')
    return f'"{escaped}"'


def _build_select_exprs(
    columns: list[ColumnSchema],
    adapter: BaseAdapter,
    config: ProfilerConfig,
    full_scan: bool = False,
) -> list[str]:
    """Build the list of SQL select expressions for a batch of columns."""
    qi = adapter.quote_identifier
    qa = _quote_alias
    exprs = ["COUNT(*) AS sampled_row_count"]

    for col in columns:
        ct = col.canonical_type
        aggs = AGGREGATE_MAP.get(ct, AGGREGATE_MAP["unknown"])
        qcol = qi(col.name)

        for template, suffix in aggs:
            sql = template.format(col=qcol)
            alias = f"{col.name}__{suffix}"
            exprs.append(f"{sql} AS {qa(alias)}")

        # STDDEV for numeric types
        if ct in ("integer", "float"):
            stddev_sql = adapter.stddev_sql(qcol, qa(f"{col.name}__stddev"))
            if stddev_sql:
                exprs.append(stddev_sql)

            # Skewness & kurtosis
            skew_sql = adapter.skewness_sql(qcol)
            if skew_sql:
                exprs.append(f"{skew_sql} AS {qa(f'{col.name}__skewness')}")
            kurt_sql = adapter.kurtosis_sql(qcol)
            if kurt_sql:
                exprs.append(f"{kurt_sql} AS {qa(f'{col.name}__kurtosis')}")

        # Percentiles for numeric types (engines that support it)
        if ct in ("integer", "float") and adapter.supports_percentiles():
            pct_exprs = adapter.percentile_sql(
                qcol,
                [0.05, 0.25, 0.5, 0.75, 0.95],
                [qa(f"{col.name}__p5"), qa(f"{col.name}__p25"), qa(f"{col.name}__median"),
                 qa(f"{col.name}__p75"), qa(f"{col.name}__p95")],
            )
            exprs.extend(pct_exprs)

        # Distinct count: on full scan, include in main query.
        # On sampled data, skip here — we run a separate full-table distinct query.
        if full_scan:
            if config.exact_distinct or adapter.distinct_mode() == "exact":
                exprs.append(f"COUNT(DISTINCT {qcol}) AS {qa(f'{col.name}__approx_distinct')}")
            else:
                exprs.append(adapter.approx_distinct_sql(qcol, qa(f"{col.name}__approx_distinct")))

    return exprs


def _build_distinct_exprs(
    columns: list[ColumnSchema],
    adapter: BaseAdapter,
    config: ProfilerConfig,
) -> list[str]:
    """Build SELECT expressions for approx_distinct on the full (unsampled) table."""
    qi = adapter.quote_identifier
    qa = _quote_alias
    exprs = []
    for col in columns:
        qcol = qi(col.name)
        if config.exact_distinct or adapter.distinct_mode() == "exact":
            exprs.append(f"COUNT(DISTINCT {qcol}) AS {qa(f'{col.name}__approx_distinct')}")
        else:
            exprs.append(adapter.approx_distinct_sql(qcol, qa(f"{col.name}__approx_distinct")))
    return exprs


def _compute_python_stddev(
    engine: Engine,
    table_name: str,
    column_name: str,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
) -> float | None:
    """Compute STDDEV in Python for engines that lack native support (SQLite)."""
    qi = adapter.quote_identifier if adapter else lambda x: x
    qcol = qi(column_name)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

    if sample_clause.startswith("WHERE"):
        query = f"SELECT CAST({qcol} AS REAL) FROM {qualified} {sample_clause} AND {qcol} IS NOT NULL"
    else:
        where = f"WHERE {qcol} IS NOT NULL"
        query = f"SELECT CAST({qcol} AS REAL) FROM {qualified} {sample_clause} {where}" if sample_clause else f"SELECT CAST({qcol} AS REAL) FROM {qualified} {where}"

    with engine.connect() as conn:
        rows = conn.execute(text(query)).fetchall()

    values = [r[0] for r in rows if r[0] is not None]
    if len(values) < 2:
        return None
    return statistics.stdev(values)


def _compute_mad(median: float | None, values: list[float]) -> float | None:
    """Compute median absolute deviation: median(|x_i - median|).

    MAD is a robust spread estimator (less sensitive to outliers than stddev).
    """
    if median is None or len(values) < 3:
        return None
    deviations = [abs(v - median) for v in values]
    return statistics.median(deviations)


def _compute_kde(values: list[float], n_points: int = 50) -> list[dict] | None:
    """Gaussian KDE at n_points evenly spaced between min and max.

    Bandwidth via Silverman's rule: h = 1.06 * sigma * n^(-1/5).
    Returns list of {x, y} dicts, or None if insufficient data.
    """
    if len(values) < 5:
        return None
    n = len(values)
    try:
        sigma = statistics.stdev(values)
    except statistics.StatisticsError:
        return None
    if sigma == 0.0:
        return None
    h = 1.06 * sigma * (n ** -0.2)  # Silverman's rule of thumb
    min_v, max_v = min(values), max(values)
    if min_v == max_v:
        return None
    inv_h_sqrt2pi = 1.0 / (h * math.sqrt(2 * math.pi))
    neg_inv_2h2 = -0.5 / (h * h)
    step = (max_v - min_v) / (n_points - 1)
    result = []
    for i in range(n_points):
        x = min_v + i * step
        density = sum(math.exp(neg_inv_2h2 * (x - xi) ** 2) for xi in values) * inv_h_sqrt2pi / n
        result.append({"x": round(x, 4), "y": round(density, 8)})
    return result


def _compute_monotonicity(
    engine: Engine,
    table_name: str,
    col_name: str,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
    limit: int = 10000,
) -> tuple[bool | None, bool | None]:
    """Check if a numeric column is monotonically increasing or decreasing.

    Returns (is_monotonic_increasing, is_monotonic_decreasing).
    Only checks up to `limit` rows (sampled or first N) for performance.
    Returns (None, None) on failure.
    """
    qi = adapter.quote_identifier if adapter else lambda x: x
    qcol = qi(col_name)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

    # Use ROWID ordering as a proxy for insertion order — sufficient for detecting
    # accidental monotonicity patterns (e.g., auto-increment IDs, timestamps).
    q = (
        f"SELECT {qcol} FROM {qualified} "
        f"WHERE {qcol} IS NOT NULL "
        f"LIMIT {limit}"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        return None, None

    values = [float(r[0]) for r in rows if r[0] is not None]
    if len(values) < 3:
        return None, None

    increasing = all(values[i] <= values[i + 1] for i in range(len(values) - 1))
    decreasing = all(values[i] >= values[i + 1] for i in range(len(values) - 1))
    return increasing, decreasing


import math


def _compute_histogram(
    engine: Engine,
    table_name: str,
    col_name: str,
    min_val: float,
    max_val: float,
    num_bins: int,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
) -> list[dict]:
    """Compute a histogram for a numeric column using width_bucket."""
    qi = adapter.quote_identifier if adapter else lambda x: x
    qcol = qi(col_name)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

    # Guard: min == max -> single bin
    if min_val == max_val:
        where = f"WHERE {qcol} IS NOT NULL"
        if sample_clause and sample_clause.startswith("WHERE"):
            where = f"{sample_clause} AND {qcol} IS NOT NULL"
        elif sample_clause:
            where = f"WHERE {qcol} IS NOT NULL"
        q = f"SELECT COUNT(*) FROM {qualified} {where}"
        with engine.connect() as conn:
            cnt = conn.execute(text(q)).scalar() or 0
        return [{"bin_start": min_val, "bin_end": max_val, "count": int(cnt)}]

    # Use math-based bucketing (works on all engines including DuckDB)
    bin_width = (max_val - min_val) / num_bins
    # LEAST/GREATEST clamp to [1, num_bins]
    bucket_expr = f"LEAST({num_bins}, GREATEST(1, CAST(FLOOR(({qcol} - {min_val}) / {bin_width}) AS INTEGER) + 1))"

    # DuckDB USING SAMPLE can't coexist with WHERE in the same SELECT.
    # Use a subquery when the sample clause is a table modifier (not a WHERE).
    if sample_clause and not sample_clause.startswith("WHERE"):
        from_clause = f"FROM (SELECT {qcol} FROM {qualified} {sample_clause}) _s"
    elif sample_clause:
        from_clause = f"FROM {qualified}"
    else:
        from_clause = f"FROM {qualified}"

    where = f"WHERE {qcol} IS NOT NULL"
    if sample_clause and sample_clause.startswith("WHERE"):
        where = f"{sample_clause} AND {qcol} IS NOT NULL"

    q = (
        f"SELECT {bucket_expr} AS bucket, "
        f"COUNT(*) AS cnt "
        f"{from_clause} {where} "
        f"GROUP BY bucket ORDER BY bucket"
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        logger.warning("Histogram query failed for %s.%s", table_name, col_name)
        return []

    bins = []
    for r in rows:
        bucket = int(r[0]) if r[0] is not None else 0
        cnt = int(r[1])
        if bucket < 1:
            bucket = 1
        if bucket > num_bins:
            bucket = num_bins
        bin_start = min_val + (bucket - 1) * bin_width
        bin_end = min_val + bucket * bin_width
        bins.append({"bin_start": round(bin_start, 4), "bin_end": round(bin_end, 4), "count": cnt})

    # Merge duplicate buckets (from clamping)
    merged_bins: list[dict] = []
    for b in bins:
        if merged_bins and merged_bins[-1]["bin_start"] == b["bin_start"]:
            merged_bins[-1]["count"] += b["count"]
        else:
            merged_bins.append(b)

    return merged_bins


def _chi2_pvalue(chi2: float, df: int) -> float:
    """Approximate chi-squared p-value using Wilson-Hilferty transformation."""
    if chi2 <= 0 or df <= 0:
        return 1.0
    z = (((chi2 / df) ** (1.0 / 3.0)) - (1.0 - 2.0 / (9.0 * df))) / math.sqrt(2.0 / (9.0 * df))
    # Standard normal CDF approximation
    p = 0.5 * math.erfc(z / math.sqrt(2.0))
    return max(0.0, min(1.0, p))


def _normal_ppf(p: float) -> float:
    """Rational approximation of standard normal inverse CDF (Abramowitz & Stegun 26.2.17)."""
    if p <= 0.0:
        return -8.0
    if p >= 1.0:
        return 8.0
    if p > 0.5:
        return -_normal_ppf(1.0 - p)
    t = math.sqrt(-2.0 * math.log(p))
    c0, c1, c2 = 2.515517, 0.802853, 0.010328
    d1, d2, d3 = 1.432788, 0.189269, 0.001308
    return -(t - (c0 + c1 * t + c2 * t ** 2) / (1.0 + d1 * t + d2 * t ** 2 + d3 * t ** 3))


def _compute_qq_plot(vals: list[float], mean: float, stddev: float, n_points: int = 50) -> list[dict]:
    """Compute Q-Q plot data comparing sample to normal distribution.

    Returns list of {theoretical, actual} pairs where theoretical is the expected
    value under N(mean, stddev) at that quantile and actual is the observed value.
    """
    if stddev <= 0 or len(vals) < 5:
        return []
    sorted_vals = sorted(vals)
    n = len(sorted_vals)
    # Subsample to n_points evenly spaced indices
    if n > n_points:
        indices = [int(round(i * (n - 1) / (n_points - 1))) for i in range(n_points)]
    else:
        indices = list(range(n))
    result = []
    for i in indices:
        p = (i + 0.5) / n
        theoretical = round(mean + stddev * _normal_ppf(p), 4)
        actual = round(sorted_vals[i], 4)
        result.append({"theoretical": theoretical, "actual": actual})
    return result


def _compute_length_histogram(
    engine: Engine,
    table_name: str,
    col_name: str,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
    max_distinct_lengths: int = 30,
) -> list[dict]:
    """Compute value-length frequency distribution for a string column.

    Returns [{length, count}, ...] sorted by length. Bucketed into bins when
    the number of distinct lengths exceeds max_distinct_lengths.
    """
    qi = adapter.quote_identifier if adapter else lambda x: x
    qcol = qi(col_name)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    if sample_clause and not sample_clause.startswith("WHERE"):
        from_clause = f"FROM {qualified} {sample_clause}"
        where = f"WHERE {qcol} IS NOT NULL"
    else:
        from_clause = f"FROM {qualified}"
        where = (f"{sample_clause} AND {qcol} IS NOT NULL" if sample_clause
                 else f"WHERE {qcol} IS NOT NULL")
    q = (
        f"SELECT LENGTH({qcol}) AS len, COUNT(*) AS cnt "
        f"{from_clause} {where} "
        f"GROUP BY len ORDER BY len"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        return []
    if not rows:
        return []
    raw = [(int(r[0]), int(r[1])) for r in rows if r[0] is not None]
    if len(raw) <= max_distinct_lengths:
        return [{"length": length, "count": cnt} for length, cnt in raw]
    # Bucket into max_distinct_lengths bins
    min_len = raw[0][0]
    max_len = raw[-1][0]
    bin_width = max(1, math.ceil((max_len - min_len + 1) / max_distinct_lengths))
    buckets: dict[int, int] = {}
    for length, cnt in raw:
        bucket_start = min_len + ((length - min_len) // bin_width) * bin_width
        buckets[bucket_start] = buckets.get(bucket_start, 0) + cnt
    return [{"length": k, "count": v} for k, v in sorted(buckets.items())]


def _compute_cdf(histogram: list[dict]) -> list[dict]:
    """Derive CDF from a histogram. Returns [{x, cumulative_pct}, ...]."""
    total = sum(b["count"] for b in histogram)
    if total == 0:
        return []
    cumulative = 0
    result = []
    for b in histogram:
        cumulative += b["count"]
        result.append({
            "x": b["bin_end"],
            "cumulative_pct": round(cumulative / total, 6),
        })
    return result


def _compute_benford(
    engine: Engine,
    table_name: str,
    col_name: str,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
) -> tuple[list[dict], float] | None:
    """Compute Benford's Law leading digit distribution and chi-squared p-value."""
    qi = adapter.quote_identifier if adapter else lambda x: x
    qcol = qi(col_name)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

    where = f"WHERE {qcol} > 0"
    if sample_clause and sample_clause.startswith("WHERE"):
        where = f"{sample_clause} AND {qcol} > 0"

    if sample_clause and not sample_clause.startswith("WHERE"):
        from_clause = f"FROM (SELECT {qcol} FROM {qualified} {sample_clause}) _s"
    else:
        from_clause = f"FROM {qualified}"

    # Extract leading digit, handling decimals < 1 by stripping leading zeros
    q = (
        f"SELECT CAST(SUBSTR(REGEXP_REPLACE(CAST(ABS({qcol}) AS VARCHAR), "
        f"'^0\\.0*', ''), 1, 1) AS INTEGER) AS leading_digit, "
        f"COUNT(*) AS cnt "
        f"{from_clause} {where} "
        f"GROUP BY leading_digit ORDER BY leading_digit"
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        logger.warning("Benford query failed for %s.%s", table_name, col_name)
        return None

    if not rows:
        return None

    # Build observed counts
    observed = {}
    total = 0
    for r in rows:
        digit = int(r[0]) if r[0] is not None else 0
        cnt = int(r[1])
        if 1 <= digit <= 9:
            observed[digit] = observed.get(digit, 0) + cnt
            total += cnt

    if total < 100:
        return None

    # Expected Benford distribution
    expected = {d: math.log10(1 + 1 / d) for d in range(1, 10)}

    # Build results and chi-squared statistic
    digits = []
    chi2 = 0.0
    for d in range(1, 10):
        obs_rate = observed.get(d, 0) / total
        exp_rate = expected[d]
        exp_count = exp_rate * total
        obs_count = observed.get(d, 0)
        if exp_count > 0:
            chi2 += (obs_count - exp_count) ** 2 / exp_count
        digits.append({"digit": d, "observed": round(obs_rate, 4), "expected": round(exp_rate, 4)})

    pvalue = _chi2_pvalue(chi2, 8)
    return digits, round(pvalue, 6)


def _compute_correlations(
    engine: Engine,
    adapter: BaseAdapter,
    table_name: str,
    numeric_cols: list[str],
    sample_clause: str,
    schema: str | None,
    max_columns: int = 20,
) -> list[dict]:
    """Compute Pearson and Spearman correlation matrix for numeric columns."""
    if len(numeric_cols) < 2:
        return []

    qi = adapter.quote_identifier
    cols = numeric_cols[:max_columns]
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    from_clause = f"FROM {qualified} {sample_clause}" if sample_clause else f"FROM {qualified}"

    results = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            corr_sql = adapter.correlation_sql(qi(cols[i]), qi(cols[j]))
            if corr_sql is None:
                continue

            # Pearson
            pearson = None
            q = f"SELECT {corr_sql} AS r {from_clause}"
            try:
                with engine.connect() as conn:
                    r = conn.execute(text(q)).scalar()
                if r is not None:
                    pearson = round(float(r), 4)
            except Exception:
                logger.warning("Pearson query failed for %s.%s vs %s", table_name, cols[i], cols[j])

            # Spearman (Pearson on ranks via CTE)
            spearman = None
            c1, c2 = qi(cols[i]), qi(cols[j])
            spearman_q = (
                f"WITH _ranks AS ("
                f"SELECT RANK() OVER (ORDER BY {c1}) AS r1,"
                f" RANK() OVER (ORDER BY {c2}) AS r2"
                f" {from_clause}"
                f" WHERE {c1} IS NOT NULL AND {c2} IS NOT NULL"
                f") SELECT {adapter.correlation_sql('r1', 'r2')} FROM _ranks"
            )
            try:
                with engine.connect() as conn:
                    s = conn.execute(text(spearman_q)).scalar()
                if s is not None:
                    spearman = round(float(s), 4)
            except Exception:
                pass  # Spearman is best-effort; don't warn on every pair

            if pearson is not None or spearman is not None:
                entry: dict = {"col1": cols[i], "col2": cols[j]}
                if pearson is not None:
                    entry["pearson"] = pearson
                if spearman is not None:
                    entry["spearman"] = spearman
                results.append(entry)

    return results


def _compute_cramers_v(
    engine: Engine,
    table_name: str,
    col1: str,
    col2: str,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
) -> float | None:
    """Compute Cramér's V for two categorical columns."""
    qi = adapter.quote_identifier if adapter else lambda x: x
    qc1, qc2 = qi(col1), qi(col2)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)
    if sample_clause and not sample_clause.startswith("WHERE"):
        from_clause = f"FROM (SELECT {qc1}, {qc2} FROM {qualified} {sample_clause}) _s"
    else:
        from_clause = f"FROM {qualified}"

    where = f"WHERE {qc1} IS NOT NULL AND {qc2} IS NOT NULL"
    if sample_clause and sample_clause.startswith("WHERE"):
        where = f"{sample_clause} AND {qc1} IS NOT NULL AND {qc2} IS NOT NULL"

    q = (
        f"SELECT {qc1}, {qc2}, COUNT(*) AS cnt "
        f"{from_clause} {where} "
        f"GROUP BY {qc1}, {qc2}"
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        logger.warning("Cramér's V query failed for %s.%s vs %s", table_name, col1, col2)
        return None

    if not rows:
        return None

    # Build contingency table
    from collections import defaultdict
    row_totals: dict[str, int] = defaultdict(int)
    col_totals: dict[str, int] = defaultdict(int)
    cells: dict[tuple, int] = {}
    n = 0
    for r in rows:
        r1, r2, cnt = str(r[0]), str(r[1]), int(r[2])
        row_totals[r1] += cnt
        col_totals[r2] += cnt
        cells[(r1, r2)] = cnt
        n += cnt

    if n == 0:
        return None

    k1 = len(row_totals)
    k2 = len(col_totals)
    if min(k1 - 1, k2 - 1) == 0:
        return 0.0

    # Chi-squared statistic
    chi2 = 0.0
    for (r1, r2), observed in cells.items():
        expected = row_totals[r1] * col_totals[r2] / n
        if expected > 0:
            chi2 += (observed - expected) ** 2 / expected

    v = math.sqrt(chi2 / (n * min(k1 - 1, k2 - 1)))
    return round(v, 4)


def _compute_nmi(
    engine: Engine,
    table_name: str,
    col1: str, min1: float, max1: float,
    col2: str, min2: float, max2: float,
    sample_clause: str,
    schema: str | None,
    adapter: BaseAdapter | None = None,
    n_bins: int = 10,
) -> float | None:
    """Normalized Mutual Information for two numeric columns via histogram binning.

    Discretizes each column into n_bins uniform bins, builds the joint frequency
    table in SQL, then computes NMI = MI / sqrt(H(X) * H(Y)) in Python.
    NMI = 0 means independent; NMI = 1 means perfectly predictable.
    """
    if min1 >= max1 or min2 >= max2:
        return None

    qi = adapter.quote_identifier if adapter else lambda x: x
    qc1, qc2 = qi(col1), qi(col2)
    qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

    w1 = (max1 - min1) / n_bins
    w2 = (max2 - min2) / n_bins
    b1_expr = f"LEAST({n_bins}, GREATEST(1, CAST(FLOOR(({qc1} - {min1}) / {w1}) AS INT) + 1))"
    b2_expr = f"LEAST({n_bins}, GREATEST(1, CAST(FLOOR(({qc2} - {min2}) / {w2}) AS INT) + 1))"

    if sample_clause and not sample_clause.startswith("WHERE"):
        from_clause = f"FROM (SELECT {qc1}, {qc2} FROM {qualified} {sample_clause}) _s"
        where = f"WHERE {qc1} IS NOT NULL AND {qc2} IS NOT NULL"
    else:
        from_clause = f"FROM {qualified}"
        where = f"WHERE {qc1} IS NOT NULL AND {qc2} IS NOT NULL"
        if sample_clause:
            where = f"{sample_clause} AND {qc1} IS NOT NULL AND {qc2} IS NOT NULL"

    q = (
        f"SELECT {b1_expr} AS b1, {b2_expr} AS b2, COUNT(*) AS cnt "
        f"{from_clause} {where} GROUP BY b1, b2"
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(q)).fetchall()
    except Exception:
        return None

    if not rows:
        return None

    from collections import defaultdict
    joint: dict[tuple, int] = {}
    marginal1: dict[int, int] = defaultdict(int)
    marginal2: dict[int, int] = defaultdict(int)
    n = 0
    for r in rows:
        b1, b2, cnt = int(r[0]), int(r[1]), int(r[2])
        joint[(b1, b2)] = cnt
        marginal1[b1] += cnt
        marginal2[b2] += cnt
        n += cnt

    if n == 0:
        return None

    # MI = Σ p(x,y) log(p(x,y) / (p(x)·p(y)))
    mi = 0.0
    for (b1, b2), cnt in joint.items():
        pxy = cnt / n
        px = marginal1[b1] / n
        py = marginal2[b2] / n
        if pxy > 0 and px > 0 and py > 0:
            mi += pxy * math.log(pxy / (px * py))

    # H(X) and H(Y)
    hx = -sum((c / n) * math.log(c / n) for c in marginal1.values() if c > 0)
    hy = -sum((c / n) * math.log(c / n) for c in marginal2.values() if c > 0)

    if hx == 0.0 or hy == 0.0:
        return 0.0

    nmi = mi / math.sqrt(hx * hy)
    return round(max(0.0, min(1.0, nmi)), 4)


_PII_PATTERNS = ["email", "ssn", "credit_card", "phone_us"]


def _mask_pii_values(
    top_values: list[dict[str, Any]],
    detected_patterns: list[str],
) -> list[dict[str, Any]]:
    """Redact top_values if the column matches a PII pattern."""
    if not any(p in detected_patterns for p in _PII_PATTERNS):
        return top_values
    return [{"value": "[REDACTED]", "count": tv["count"]} for tv in top_values]


def profile_table(
    adapter: BaseAdapter,
    table_schema: TableSchema,
    config: ProfilerConfig,
) -> ProfiledTable:
    """Profile a single table: schema + stats in a single SQL pass (or batched)."""
    start = time.time()
    engine = adapter.get_engine()
    schema = config.schema_name
    table_name = table_schema.name

    result = ProfiledTable(
        name=table_name,
        comment=table_schema.comment,
        sample_size=config.sample_size,
        profiled_at=datetime.now(timezone.utc).isoformat(),
    )

    try:
        # Get total row count (separate query, no sampling)
        total_rows = get_row_count(engine, table_name, schema)
        result.total_row_count = total_rows

        if config.stats_depth == "fast":
            # Schema-only mode: no aggregate queries
            result.sampled_row_count = total_rows
            result.full_scan = True
            for col in table_schema.columns:
                result.columns.append(ColumnProfile(
                    name=col.name,
                    engine_type=col.engine_type,
                    canonical_type=col.canonical_type,
                    comment=col.comment,
                    nullable=col.nullable,
                    distinct_mode=adapter.distinct_mode(),
                ))
            result.duration_seconds = time.time() - start
            return result

        # Determine sampling
        full_scan = config.sample_size == 0 or config.sample_size >= total_rows
        if full_scan and config.sample_size > 0:
            logger.info(
                "Table %s has %d rows, below sample_size %d — scanning full table.",
                table_name, total_rows, config.sample_size,
            )
        result.full_scan = full_scan
        sample_sql = adapter.sample_clause(table_name, config.sample_size, total_rows)

        # Batch columns to avoid expression limits
        all_columns = table_schema.columns
        batch_size = config.column_batch_size
        batches = [all_columns[i:i + batch_size] for i in range(0, len(all_columns), batch_size)]

        qi = adapter.quote_identifier
        qualified = f"{qi(schema)}.{qi(table_name)}" if schema else qi(table_name)

        # Collect all batch results before writing anything
        batch_results: list[dict[str, Any]] = []
        sampled_row_count = 0

        for batch in batches:
            exprs = _build_select_exprs(batch, adapter, config, full_scan=full_scan)

            # Build FROM clause with sampling
            if sample_sql and not sample_sql.startswith("WHERE"):
                from_clause = f"FROM {qualified} {sample_sql}"
            elif sample_sql:
                from_clause = f"FROM {qualified} {sample_sql}"
            else:
                from_clause = f"FROM {qualified}"

            query = f"SELECT {', '.join(exprs)} {from_clause}"
            logger.debug("Query for %s: %s", table_name, query[:200])

            with engine.connect() as conn:
                row = conn.execute(text(query)).fetchone()

            if row is None:
                raise RuntimeError(f"No result from stats query for {table_name}")

            row_dict = dict(row._mapping)
            batch_results.append(row_dict)
            sampled_row_count = row_dict.get("sampled_row_count", 0)

        result.sampled_row_count = sampled_row_count

        # Run approx_distinct on full table (no sampling) when sampling is active.
        # This fixes the critical bug where distinct counts are bounded by sample size.
        distinct_results: dict[str, Any] = {}
        if not full_scan:
            for batch in batches:
                d_exprs = _build_distinct_exprs(batch, adapter, config)
                if d_exprs:
                    d_query = f"SELECT {', '.join(d_exprs)} FROM {qualified}"
                    logger.debug("Distinct query for %s: %s", table_name, d_query[:200])
                    with engine.connect() as conn:
                        d_row = conn.execute(text(d_query)).fetchone()
                    if d_row:
                        distinct_results.update(dict(d_row._mapping))

        # Merge batch results and build column profiles
        merged: dict[str, Any] = {}
        for br in batch_results:
            merged.update(br)
        # Distinct results come from the full-table query (or main query if full scan)
        merged.update(distinct_results)

        # Fetch top-5 and bottom-5 frequent values per column (separate query per column)
        top_values_map: dict[str, list[dict[str, Any]]] = {}
        bottom_values_map: dict[str, list[dict[str, Any]]] = {}
        for col in all_columns:
            try:
                qcol = qi(col.name)
                top_query = (
                    f"SELECT {qcol} AS val, COUNT(*) AS freq "
                    f"FROM {qualified} "
                    f"WHERE {qcol} IS NOT NULL "
                    f"GROUP BY {qcol} ORDER BY freq DESC LIMIT 5"
                )
                with engine.connect() as conn:
                    rows = conn.execute(text(top_query)).fetchall()
                if rows:
                    top_values_map[col.name] = [
                        {"value": str(r[0]), "count": int(r[1])} for r in rows
                    ]
                # Bottom-5: rarest values (only meaningful when there's enough cardinality)
                if len(rows) >= 5:
                    bottom_query = (
                        f"SELECT {qcol} AS val, COUNT(*) AS freq "
                        f"FROM {qualified} "
                        f"WHERE {qcol} IS NOT NULL "
                        f"GROUP BY {qcol} ORDER BY freq ASC LIMIT 5"
                    )
                    with engine.connect() as conn:
                        bottom_rows = conn.execute(text(bottom_query)).fetchall()
                    if bottom_rows:
                        bottom_values_map[col.name] = [
                            {"value": str(r[0]), "count": int(r[1])} for r in bottom_rows
                        ]
                # Unique count: values appearing exactly once (Deequ-style Uniqueness metric)
                try:
                    unique_q = (
                        f"SELECT COUNT(*) FROM ("
                        f"SELECT {qcol} FROM {qualified} "
                        f"WHERE {qcol} IS NOT NULL "
                        f"GROUP BY {qcol} HAVING COUNT(*) = 1"
                        f") _u"
                    )
                    with engine.connect() as conn:
                        uc = conn.execute(text(unique_q)).scalar()
                    if uc is not None:
                        top_values_map[f"__unique_count__{col.name}"] = int(uc)
                except Exception:
                    pass
            except Exception:
                logger.warning("Top-N query failed for %s.%s", table_name, col.name)

        # Pattern detection on string columns
        pattern_map: dict[str, tuple[list[str], dict[str, float]]] = {}
        if config.enable_patterns:
            string_cols = [c.name for c in all_columns if c.canonical_type == "string"]
            if string_cols:
                string_samples = fetch_string_sample(
                    engine, table_name, string_cols, sample_sql, schema,
                    quote_fn=qi,
                )
                for col_name, values in string_samples.items():
                    if values:
                        pattern_map[col_name] = detect_patterns(values)

        for col in all_columns:
            non_null = merged.get(f"{col.name}__non_null", 0) or 0
            null_count = sampled_row_count - non_null
            null_rate = null_count / sampled_row_count if sampled_row_count > 0 else 0.0

            cp = ColumnProfile(
                name=col.name,
                engine_type=col.engine_type,
                canonical_type=col.canonical_type,
                comment=col.comment,
                nullable=col.nullable,
                null_count=null_count,
                null_rate=null_rate,
                approx_distinct=merged.get(f"{col.name}__approx_distinct", 0) or 0,
                distinct_mode=adapter.distinct_mode(),
                top_values=top_values_map.get(col.name),
                bottom_values=bottom_values_map.get(col.name),
            )

            # Type-specific fields
            if col.canonical_type in ("integer", "float"):
                cp.min = merged.get(f"{col.name}__min")
                cp.max = merged.get(f"{col.name}__max")
                cp.mean = merged.get(f"{col.name}__mean")
                sum_val = merged.get(f"{col.name}__sum")
                if sum_val is not None:
                    cp.sum = float(sum_val)
                cp.zero_count = merged.get(f"{col.name}__zero_count")
                if cp.zero_count is not None:
                    cp.zero_count = int(cp.zero_count)
                cp.negative_count = merged.get(f"{col.name}__negative_count")
                if cp.negative_count is not None:
                    cp.negative_count = int(cp.negative_count)
                if col.canonical_type == "float":
                    ic = merged.get(f"{col.name}__infinite_count")
                    cp.infinite_count = int(ic) if ic is not None else None
                stddev_val = merged.get(f"{col.name}__stddev")
                if stddev_val is not None:
                    cp.stddev = float(stddev_val)
                    cp.variance = cp.stddev ** 2
                elif not adapter.supports_native_stddev() and col.canonical_type in ("integer", "float"):
                    # Python fallback for SQLite
                    cp.stddev = _compute_python_stddev(
                        engine, table_name, col.name, sample_sql, schema, adapter,
                    )
                    if cp.stddev is not None:
                        cp.variance = cp.stddev ** 2

                # Skewness & kurtosis
                skew_val = merged.get(f"{col.name}__skewness")
                if skew_val is not None:
                    cp.skewness = float(skew_val)
                kurt_val = merged.get(f"{col.name}__kurtosis")
                if kurt_val is not None:
                    cp.kurtosis = float(kurt_val)

                # Percentiles (p5, p25, p50/median, p75, p95)
                for attr, key in [
                    ("p5", f"{col.name}__p5"),
                    ("p25", f"{col.name}__p25"),
                    ("median", f"{col.name}__median"),
                    ("p75", f"{col.name}__p75"),
                    ("p95", f"{col.name}__p95"),
                ]:
                    raw = merged.get(key)
                    setattr(cp, attr, float(raw) if raw is not None else None)
                # Derived: IQR, range, CV (coefficient of variation)
                if cp.p25 is not None and cp.p75 is not None:
                    cp.iqr = round(cp.p75 - cp.p25, 6)
                if cp.min is not None and cp.max is not None:
                    try:
                        cp.range = round(float(cp.max) - float(cp.min), 6)
                    except (TypeError, ValueError):
                        pass
                if cp.stddev is not None and cp.mean not in (None, 0.0):
                    cp.cv = round(abs(cp.stddev / cp.mean), 6)
                # Box plot: Tukey fences (k=1.5) — whiskers clipped to actual data range
                if cp.iqr is not None and cp.p25 is not None and cp.p75 is not None:
                    lower_fence = round(cp.p25 - 1.5 * cp.iqr, 6)
                    upper_fence = round(cp.p75 + 1.5 * cp.iqr, 6)
                    cp.box_plot = {
                        "q1": cp.p25,
                        "median": cp.median,
                        "q3": cp.p75,
                        "lower_fence": lower_fence,
                        "upper_fence": upper_fence,
                    }

                # Fetch sample values for MAD + KDE (one query, both computations)
                if config.stats_depth == "full":
                    try:
                        qi_inner = adapter.quote_identifier
                        qcol_inner = qi_inner(col.name)
                        where_clause = f"WHERE {qcol_inner} IS NOT NULL"
                        if sample_sql and sample_sql.startswith("WHERE"):
                            where_clause = f"{sample_sql} AND {qcol_inner} IS NOT NULL"
                        val_q = (
                            f"SELECT CAST({qcol_inner} AS DOUBLE) "
                            f"FROM {qualified} {where_clause} LIMIT 10000"
                        )
                        with engine.connect() as conn:
                            vals = [float(r[0]) for r in conn.execute(text(val_q)).fetchall() if r[0] is not None]
                        if cp.median is not None:
                            mad = _compute_mad(cp.median, vals)
                            cp.mad = round(mad, 6) if mad is not None else None
                        if len(vals) >= 5:
                            cp.kde = _compute_kde(vals)
                        if len(vals) >= 5 and cp.mean is not None and cp.stddev is not None:
                            cp.qq_plot = _compute_qq_plot(vals, cp.mean, cp.stddev)
                    except Exception:
                        pass  # MAD/KDE/QQ are best-effort

                # Monotonicity detection — check if values appear in sorted order
                if config.stats_depth == "full" and cp.approx_distinct > 1:
                    try:
                        mono_inc, mono_dec = _compute_monotonicity(
                            engine, table_name, col.name, sample_sql, schema, adapter,
                        )
                        if mono_inc is not None:
                            cp.is_monotonic_increasing = mono_inc
                            cp.is_monotonic_decreasing = mono_dec
                    except Exception:
                        pass

                # Cast numeric values
                if cp.min is not None:
                    try:
                        cp.min = float(cp.min) if col.canonical_type == "float" else int(cp.min)
                    except (ValueError, TypeError):
                        pass
                if cp.max is not None:
                    try:
                        cp.max = float(cp.max) if col.canonical_type == "float" else int(cp.max)
                    except (ValueError, TypeError):
                        pass
                if cp.mean is not None:
                    cp.mean = float(cp.mean)

            elif col.canonical_type == "string":
                cp.min = merged.get(f"{col.name}__min")
                cp.max = merged.get(f"{col.name}__max")
                cp.min_length = merged.get(f"{col.name}__min_length")
                if cp.min_length is not None:
                    cp.min_length = int(cp.min_length)
                cp.max_length = merged.get(f"{col.name}__max_length")
                if cp.max_length is not None:
                    cp.max_length = int(cp.max_length)
                cp.avg_length = merged.get(f"{col.name}__avg_length")
                if cp.avg_length is not None:
                    cp.avg_length = float(cp.avg_length)
                ec = merged.get(f"{col.name}__empty_count")
                cp.empty_count = int(ec) if ec is not None else None
                wc = merged.get(f"{col.name}__whitespace_count")
                cp.whitespace_count = int(wc) if wc is not None else None
                ltwc = merged.get(f"{col.name}__leading_trailing_whitespace_count")
                cp.leading_trailing_whitespace_count = int(ltwc) if ltwc is not None else None

            elif col.canonical_type in ("date", "datetime"):
                raw_min = merged.get(f"{col.name}__min")
                raw_max = merged.get(f"{col.name}__max")
                cp.min = str(raw_min) if raw_min is not None else None
                cp.max = str(raw_max) if raw_max is not None else None
                # Freshness + date_range_days + granularity_guess
                if raw_max is not None and raw_min is not None:
                    try:
                        from datetime import date as _date
                        max_str = str(raw_max)[:10]
                        min_str = str(raw_min)[:10]
                        max_date = _date.fromisoformat(max_str)
                        min_date = _date.fromisoformat(min_str)
                        cp.freshness_days = (datetime.now(timezone.utc).date() - max_date).days
                        range_days = (max_date - min_date).days
                        cp.date_range_days = range_days
                        # Granularity inference: compare distinct count to expected count per cadence
                        if range_days > 0 and cp.approx_distinct > 1:
                            ratio_daily = cp.approx_distinct / range_days
                            ratio_weekly = cp.approx_distinct / (range_days / 7.0)
                            ratio_monthly = cp.approx_distinct / (range_days / 30.0)
                            ratio_yearly = cp.approx_distinct / (range_days / 365.0)
                            if 0.7 <= ratio_daily <= 1.3 and range_days >= 7:
                                cp.granularity_guess = "daily"
                            elif 0.7 <= ratio_weekly <= 1.3 and range_days >= 28:
                                cp.granularity_guess = "weekly"
                            elif 0.7 <= ratio_monthly <= 1.3 and range_days >= 60:
                                cp.granularity_guess = "monthly"
                            elif 0.5 <= ratio_yearly <= 1.5 and range_days >= 365:
                                cp.granularity_guess = "yearly"
                            else:
                                cp.granularity_guess = "unknown"
                        else:
                            cp.granularity_guess = "unknown"
                    except (ValueError, TypeError, AttributeError):
                        pass
                elif raw_max is not None:
                    try:
                        from datetime import date as _date
                        max_str = str(raw_max)[:10]
                        max_date = _date.fromisoformat(max_str)
                        cp.freshness_days = (datetime.now(timezone.utc).date() - max_date).days
                    except (ValueError, TypeError, AttributeError):
                        pass

            elif col.canonical_type == "boolean":
                true_count = merged.get(f"{col.name}__true_count")
                cp.true_count = int(true_count) if true_count is not None else None
                if cp.true_count is not None and non_null > 0:
                    cp.false_count = non_null - cp.true_count
                    cp.true_rate = round(cp.true_count / non_null, 6)
                    cp.false_rate = round(cp.false_count / non_null, 6)
                    if cp.true_rate > 0 and cp.false_rate > 0:
                        cp.imbalance_ratio = round(
                            max(cp.true_rate, cp.false_rate) / min(cp.true_rate, cp.false_rate), 4
                        )

            # Set detected patterns
            if col.name in pattern_map:
                cp.patterns, cp.pattern_scores = pattern_map[col.name]

            # Mask PII in top_values
            if cp.top_values and cp.patterns:
                cp.top_values = _mask_pii_values(cp.top_values, cp.patterns)

            # Unique count (singletons) and uniqueness_ratio
            uc = top_values_map.get(f"__unique_count__{col.name}")
            if uc is not None:
                cp.unique_count = uc
                if cp.approx_distinct and cp.approx_distinct > 0:
                    cp.uniqueness_ratio = round(cp.unique_count / cp.approx_distinct, 6)

            # Derived cross-type fields: distinct_ratio and pk_candidate
            if sampled_row_count > 0:
                cp.distinct_ratio = round(cp.approx_distinct / sampled_row_count, 6)
            if cp.null_count == 0 and sampled_row_count > 0 and cp.approx_distinct >= sampled_row_count * 0.99:
                cp.pk_candidate = True

            # Apply anomaly rules (use total_rows for distinct ratio when distinct was computed on full table)
            effective_row_count = total_rows if not full_scan else sampled_row_count
            cp.anomalies = apply_anomaly_rules(cp, effective_row_count)

            result.columns.append(cp)

        # Row-level completeness: fraction of non-null columns per row
        if config.stats_depth == "full" and all_columns:
            try:
                col_null_exprs = " + ".join(
                    f"CASE WHEN {qi(c.name)} IS NOT NULL THEN 1 ELSE 0 END"
                    for c in all_columns
                )
                completeness_expr = f"({col_null_exprs}) * 1.0 / {len(all_columns)}"
                if sample_sql and not sample_sql.startswith("WHERE"):
                    rc_from = f"FROM {qualified} {sample_sql}"
                elif sample_sql:
                    rc_from = f"FROM {qualified} {sample_sql}"
                else:
                    rc_from = f"FROM {qualified}"
                rc_query = (
                    f"SELECT MIN({completeness_expr}), MAX({completeness_expr}),"
                    f" AVG({completeness_expr}) {rc_from}"
                )
                with engine.connect() as conn:
                    rc_row = conn.execute(text(rc_query)).fetchone()
                if rc_row and rc_row[0] is not None:
                    result.row_completeness_min = round(float(rc_row[0]), 4)
                    result.row_completeness_max = round(float(rc_row[1]), 4)
                    result.row_completeness_mean = round(float(rc_row[2]), 4)
            except Exception:
                logger.warning("Row completeness failed for %s", table_name)

        # Functional dependency detection: A→B if each value of A maps to exactly 1 value of B
        # Only check low-cardinality columns (2–50 distinct values) to keep O(n^2) manageable.
        if config.stats_depth == "full" and len(result.columns) >= 2:
            try:
                fd_cols = [
                    cp for cp in result.columns
                    if 1 < cp.approx_distinct <= 50
                    and cp.canonical_type in ("string", "integer", "boolean")
                ][:8]  # cap at 8 columns → max 56 pair queries
                fds = []
                for col_a in fd_cols:
                    for col_b in fd_cols:
                        if col_a.name == col_b.name:
                            continue
                        try:
                            fd_q = (
                                f"SELECT MAX(distinct_b) FROM ("
                                f"SELECT {qi(col_a.name)},"
                                f" COUNT(DISTINCT {qi(col_b.name)}) AS distinct_b"
                                f" FROM {qualified}"
                                f" WHERE {qi(col_a.name)} IS NOT NULL"
                                f" AND {qi(col_b.name)} IS NOT NULL"
                                f" GROUP BY {qi(col_a.name)}"
                                f") AS _fd"
                            )
                            with engine.connect() as conn:
                                max_d = conn.execute(text(fd_q)).scalar()
                            if max_d is not None and int(max_d) == 1:
                                fds.append({"from": col_a.name, "to": col_b.name})
                        except Exception:
                            pass
                if fds:
                    result.functional_dependencies = fds
            except Exception:
                logger.warning("Functional dependency detection failed for %s", table_name)

        # Duplicate row detection
        if config.detect_duplicates and 0 < len(all_columns) <= config.duplicate_column_limit:
            try:
                col_list = ", ".join(qi(c.name) for c in all_columns)
                dup_query = (
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT {col_list} FROM {qualified} "
                    f"GROUP BY {col_list} HAVING COUNT(*) > 1"
                    f") AS dup_groups"
                )
                # Count total extra rows (sum of (count-1) per duplicate group)
                dup_sum_query = (
                    f"SELECT COALESCE(SUM(cnt - 1), 0) FROM ("
                    f"SELECT COUNT(*) AS cnt FROM {qualified} "
                    f"GROUP BY {col_list} HAVING COUNT(*) > 1"
                    f") AS dup_groups"
                )
                with engine.connect() as conn:
                    dup_count = conn.execute(text(dup_sum_query)).scalar() or 0
                result.duplicate_row_count = int(dup_count)
                result.duplicate_rate = dup_count / total_rows if total_rows > 0 else 0.0
            except Exception as e:
                logger.warning("Duplicate detection failed for %s: %s", table_name, e)

        # Post-profiling: Histogram for numeric columns
        if config.enable_histogram:
            hist_start = time.time()
            for cp in result.columns:
                if cp.canonical_type in ("integer", "float") and cp.min is not None and cp.max is not None:
                    try:
                        cp.histogram = _compute_histogram(
                            engine, table_name, cp.name,
                            float(cp.min), float(cp.max),
                            config.histogram_bins, sample_sql, schema, adapter,
                        )
                        if cp.histogram:
                            cp.cdf = _compute_cdf(cp.histogram)
                    except Exception:
                        logger.warning("Histogram failed for %s.%s", table_name, cp.name)
                elif cp.canonical_type == "string":
                    try:
                        cp.length_histogram = _compute_length_histogram(
                            engine, table_name, cp.name, sample_sql, schema, adapter,
                        )
                    except Exception:
                        logger.warning("Length histogram failed for %s.%s", table_name, cp.name)
            logger.debug("Histograms for %s: %.2fs", table_name, time.time() - hist_start)

        # Post-profiling: Benford's Law analysis
        if config.enable_benford:
            benford_start = time.time()
            for cp in result.columns:
                if (cp.canonical_type in ("integer", "float")
                        and cp.approx_distinct > 100
                        and cp.min is not None):
                    try:
                        result_benford = _compute_benford(
                            engine, table_name, cp.name, sample_sql, schema, adapter,
                        )
                        if result_benford:
                            cp.benford_digits, cp.benford_pvalue = result_benford
                    except Exception:
                        logger.warning("Benford failed for %s.%s", table_name, cp.name)
            logger.debug("Benford for %s: %.2fs", table_name, time.time() - benford_start)

        # Post-profiling: Correlation analysis
        if config.enable_correlation and adapter.correlation_sql("a", "b") is not None:
            corr_start = time.time()
            numeric_cols = [
                cp.name for cp in result.columns
                if cp.canonical_type in ("integer", "float")
            ]
            if len(numeric_cols) >= 2:
                try:
                    pearson = _compute_correlations(
                        engine, adapter, table_name, numeric_cols,
                        sample_sql, schema, config.correlation_max_columns,
                    )
                    # NMI for numeric pairs (uses same min/max from column profiles)
                    col_minmax = {
                        cp.name: (float(cp.min), float(cp.max))
                        for cp in result.columns
                        if cp.canonical_type in ("integer", "float")
                        and cp.min is not None and cp.max is not None
                        and float(cp.min) < float(cp.max)
                    }
                    for entry in pearson:
                        c1, c2 = entry["col1"], entry["col2"]
                        if c1 in col_minmax and c2 in col_minmax:
                            try:
                                nmi = _compute_nmi(
                                    engine, table_name,
                                    c1, *col_minmax[c1],
                                    c2, *col_minmax[c2],
                                    sample_sql, schema, adapter,
                                )
                                if nmi is not None:
                                    entry["nmi"] = nmi
                            except Exception:
                                pass  # NMI is best-effort
                    # Cramér's V for low-cardinality string pairs
                    cramers = []
                    cat_cols = [
                        cp.name for cp in result.columns
                        if cp.canonical_type == "string" and 1 < cp.approx_distinct < 50
                    ]
                    for i in range(min(len(cat_cols), 5)):
                        for j in range(i + 1, min(len(cat_cols), 5)):
                            v = _compute_cramers_v(
                                engine, table_name, cat_cols[i], cat_cols[j],
                                sample_sql, schema, adapter,
                            )
                            if v is not None:
                                cramers.append({
                                    "col1": cat_cols[i], "col2": cat_cols[j],
                                    "cramers_v": v,
                                })
                    result.correlations = pearson + cramers if (pearson or cramers) else None
                except Exception:
                    logger.warning("Correlation failed for %s", table_name)
            logger.debug("Correlations for %s: %.2fs", table_name, time.time() - corr_start)

    except Exception as e:
        logger.error("Error profiling table %s: %s", table_name, e, exc_info=True)
        result.error = str(e)

    result.duration_seconds = time.time() - start
    return result
