"""Rule-based anomaly detection for column profiles."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_profiler.config import AnomalyThresholds
    from data_profiler.workers.stats_worker import ColumnProfile

logger = logging.getLogger(__name__)


def _safe_ratio(a: int | float, b: int | float) -> float:
    """Avoids ZeroDivisionError for empty samples."""
    return a / b if b > 0 else 0.0


def _is_future_date(profile: "ColumnProfile") -> bool:
    """Check if max date/datetime value is in the future."""
    if profile.canonical_type not in ("date", "datetime") or profile.max is None:
        return False
    try:
        max_str = str(profile.max)[:10]  # YYYY-MM-DD
        max_date = date.fromisoformat(max_str)
        return max_date > date.today()
    except (ValueError, TypeError):
        return False


# HLL accuracy note: APPROX_COUNT_DISTINCT uses HyperLogLog with ~2-3% typical error
# on Snowflake/Databricks/DuckDB. The "all_unique" rule uses a 0.99 threshold (not 1.0)
# to account for HLL undercount. The "single_value" rule checks == 1, which HLL reports
# accurately (HLL error is proportional to cardinality; very low cardinality is exact).
# SQLite uses exact COUNT(DISTINCT) so no HLL error applies there.


def _iqr_has_outliers(s: "ColumnProfile") -> bool:
    """True if min or max lies beyond 1.5×IQR from the quartiles (Tukey fences)."""
    if s.p25 is None or s.p75 is None or s.min is None or s.max is None:
        return False
    try:
        iqr = float(s.p75) - float(s.p25)
        if iqr == 0.0:
            return False
        lower = float(s.p25) - 1.5 * iqr
        upper = float(s.p75) + 1.5 * iqr
        return float(s.min) < lower or float(s.max) > upper
    except (TypeError, ValueError):
        return False


def _zscore_has_outliers(s: "ColumnProfile", threshold: float = 3.0) -> bool:
    """True if min or max is more than `threshold` stddevs from the mean."""
    if s.mean is None or s.stddev is None or s.stddev == 0.0:
        return False
    if s.min is None and s.max is None:
        return False
    try:
        z_min = abs((float(s.mean) - float(s.min)) / s.stddev) if s.min is not None else 0.0
        z_max = abs((float(s.max) - float(s.mean)) / s.stddev) if s.max is not None else 0.0
        return max(z_min, z_max) > threshold
    except (TypeError, ValueError):
        return False


def _build_anomaly_rules(
    high_null_rate: float = 0.5,
    near_constant_rate: float = 0.95,
    high_cardinality_distinct: int = 10_000,
    all_unique_ratio: float = 0.99,
    all_unique_hll_guard: float = 1.05,
    skewness_threshold: float = 2.0,
) -> list[tuple[str, object]]:
    """Build anomaly rules with configurable thresholds."""
    return [
        ("high_null_rate", lambda s, n: s.null_rate > high_null_rate),
        ("single_value", lambda s, n: s.approx_distinct == 1),
        ("all_unique", lambda s, n: n > 1
                                    and _safe_ratio(s.approx_distinct, n) > all_unique_ratio
                                    and s.approx_distinct <= n * all_unique_hll_guard),
        ("empty_string_dominant",
            lambda s, n: s.canonical_type == "string"
                         and s.max_length is not None
                         and s.max_length == 0),
        ("zero_variance",
            lambda s, n: s.canonical_type in ("integer", "float")
                         and s.stddev is not None
                         and s.stddev == 0.0
                         and s.approx_distinct > 1),
        ("near_constant",
            lambda s, n: s.top_values is not None
                         and len(s.top_values) > 0
                         and n > 1
                         and _safe_ratio(s.top_values[0]["count"], n) > near_constant_rate),
        ("high_cardinality",
            lambda s, n: s.canonical_type == "string"
                         and s.approx_distinct > high_cardinality_distinct),
        ("has_negatives",
            lambda s, n: s.negative_count is not None and s.negative_count > 0),
        ("future_dates", lambda s, n: _is_future_date(s)),
        ("benford_anomaly",
            lambda s, n: s.canonical_type in ("integer", "float")
                         and getattr(s, "benford_pvalue", None) is not None
                         and s.benford_pvalue < 0.01
                         and n > 1000),
        # IQR outlier: min or max beyond Tukey's 1.5×IQR fences (requires p25/p75)
        ("iqr_outliers",
            lambda s, n: s.canonical_type in ("integer", "float") and _iqr_has_outliers(s)),
        # Z-score outlier: |z| > 3.0 for min or max
        ("zscore_outliers",
            lambda s, n: s.canonical_type in ("integer", "float") and _zscore_has_outliers(s)),
        # Skewness alert: |skewness| > 2.0 (highly skewed distribution)
        ("high_skewness",
            lambda s, n: s.canonical_type in ("integer", "float")
                         and getattr(s, "skewness", None) is not None
                         and abs(s.skewness) > skewness_threshold),
        # Boolean imbalance: minority class < 1% of non-null rows
        ("boolean_imbalance",
            lambda s, n: s.canonical_type == "boolean"
                         and s.true_count is not None
                         and n > 0
                         and (_safe_ratio(s.true_count, n) < 0.01
                              or _safe_ratio(s.true_count, n) > 0.99)),
        # Stale data: date/datetime column hasn't been updated in > 90 days
        ("stale_data",
            lambda s, n: s.canonical_type in ("date", "datetime")
                         and getattr(s, "freshness_days", None) is not None
                         and s.freshness_days > 90),
        # Modified z-score outlier (Iglewicz & Hoaglin, 1993): |0.6745*(x-median)/MAD| > 3.5
        # More robust than z-score because it uses MAD instead of mean/stddev.
        ("mad_outliers",
            lambda s, n: s.canonical_type in ("integer", "float")
                         and getattr(s, "mad", None) is not None
                         and s.mad > 0
                         and s.median is not None
                         and s.min is not None
                         and s.max is not None
                         and max(
                             abs(0.6745 * (float(s.min) - s.median) / s.mad),
                             abs(0.6745 * (float(s.max) - s.median) / s.mad),
                         ) > 3.5),
        # Suspicious uniform length: all non-empty strings have exactly the same length
        # Indicates structured IDs, codes, or hash values that may warrant a CHECK constraint.
        ("suspicious_uniform_length",
            lambda s, n: s.canonical_type == "string"
                         and s.min_length is not None
                         and s.max_length is not None
                         and s.min_length == s.max_length
                         and s.max_length > 0
                         and n > 1),
        # Low cardinality numeric: integer/float with very few distinct values
        # Suggests a miscoded categorical (e.g., status=1/2/3 stored as integer).
        ("low_cardinality_numeric",
            lambda s, n: s.canonical_type in ("integer", "float")
                         and s.approx_distinct is not None
                         and s.approx_distinct < 10
                         and n >= 100),
        # Leading/trailing whitespace: string values with leading or trailing spaces
        ("has_leading_trailing_whitespace",
            lambda s, n: s.canonical_type == "string"
                         and getattr(s, "leading_trailing_whitespace_count", None) is not None
                         and s.leading_trailing_whitespace_count > 0),
    ]


# Default rules with standard thresholds (backward compatible)
ANOMALY_RULES: list[tuple[str, object]] = _build_anomaly_rules()


def apply_anomaly_rules(
    profile: ColumnProfile,
    row_count: int = 0,
    thresholds: AnomalyThresholds | None = None,
) -> list[str]:
    """Apply all anomaly rules to a column profile, return list of triggered rule names.

    row_count should be total_rows when approx_distinct was computed on the full table,
    or sampled_row_count when running in full-scan mode.
    """
    if thresholds is not None:
        rules = _build_anomaly_rules(
            high_null_rate=thresholds.high_null_rate,
            near_constant_rate=thresholds.near_constant_rate,
            high_cardinality_distinct=thresholds.high_cardinality_distinct,
            all_unique_ratio=thresholds.all_unique_ratio,
            all_unique_hll_guard=thresholds.all_unique_hll_guard,
        )
    else:
        rules = ANOMALY_RULES

    triggered = []
    for name, predicate in rules:
        try:
            if predicate(profile, row_count):
                triggered.append(name)
        except Exception:
            logger.debug("Anomaly rule '%s' failed", name, exc_info=True)
    return triggered
