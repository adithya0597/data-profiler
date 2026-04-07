"""Statistical merging for incremental profiling (Welford's parallel algorithm)."""

from __future__ import annotations

import math
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


def merge_profiles(prior: "ProfiledTable", delta: "ProfiledTable") -> "ProfiledTable":
    """Merge a delta profile into a prior profile for append-only tables.

    Uses Welford's parallel algorithm for mean/variance/stddev.
    """
    from data_profiler.workers.stats_worker import ProfiledTable

    prior_n = prior.total_row_count
    delta_n = delta.total_row_count
    total_n = prior_n + delta_n

    # Match columns by name
    prior_cols = {c.name: c for c in prior.columns}
    delta_cols = {c.name: c for c in delta.columns}
    all_col_names = list(dict.fromkeys(
        [c.name for c in delta.columns] + [c.name for c in prior.columns]
    ))

    merged_columns = []
    for name in all_col_names:
        if name in delta_cols and name in prior_cols:
            merged_columns.append(_merge_column(prior_cols[name], delta_cols[name], prior_n, delta_n))
        elif name in delta_cols:
            # New column — use delta's values as-is
            merged_columns.append(delta_cols[name])
        # Dropped columns (in prior only) are omitted

    return ProfiledTable(
        name=delta.name,
        comment=delta.comment or prior.comment,
        total_row_count=total_n,
        sampled_row_count=prior.sampled_row_count + delta.sampled_row_count,
        sample_size=delta.sample_size,
        full_scan=delta.full_scan,
        profiled_at=max(prior.profiled_at, delta.profiled_at) if prior.profiled_at and delta.profiled_at else delta.profiled_at,
        duration_seconds=prior.duration_seconds + delta.duration_seconds,
        columns=merged_columns,
        constraints=delta.constraints or prior.constraints,
        correlations=delta.correlations,
        suggested_constraints=delta.suggested_constraints,
        duplicate_row_count=prior.duplicate_row_count + delta.duplicate_row_count,
        duplicate_rate=(prior.duplicate_row_count + delta.duplicate_row_count) / max(1, total_n),
        functional_dependencies=delta.functional_dependencies,
        quality_score=delta.quality_score,
    )


def _merge_column(prior: "ColumnProfile", delta: "ColumnProfile", prior_n: int, delta_n: int) -> "ColumnProfile":
    """Merge two column profiles using Welford's parallel algorithm."""
    from data_profiler.workers.stats_worker import ColumnProfile

    total_n = prior_n + delta_n

    # Additive counts
    null_count = prior.null_count + delta.null_count
    null_rate = null_count / max(1, total_n)

    # Min/max
    merged_min = _safe_min(prior.min, delta.min)
    merged_max = _safe_max(prior.max, delta.max)

    # Weighted mean (Welford's parallel combine)
    merged_mean = None
    merged_variance = None
    merged_stddev = None
    if prior.mean is not None and delta.mean is not None and total_n > 0:
        merged_mean = (prior_n * prior.mean + delta_n * delta.mean) / total_n
        # Welford's parallel algorithm for variance
        d = delta.mean - prior.mean
        prior_var = prior.variance if prior.variance is not None else 0.0
        delta_var = delta.variance if delta.variance is not None else 0.0
        m2_prior = prior_var * prior_n
        m2_delta = delta_var * delta_n
        m2_combined = m2_prior + m2_delta + (d * d * prior_n * delta_n / total_n)
        merged_variance = m2_combined / total_n if total_n > 0 else 0.0
        merged_stddev = math.sqrt(merged_variance) if merged_variance >= 0 else 0.0
    elif delta.mean is not None:
        merged_mean = delta.mean
        merged_variance = delta.variance
        merged_stddev = delta.stddev

    # Additive sums
    merged_sum = _safe_add(prior.sum, delta.sum)
    merged_zero = _safe_add(prior.zero_count, delta.zero_count)
    merged_neg = _safe_add(prior.negative_count, delta.negative_count)

    # Conservative distinct estimate
    approx_distinct = max(prior.approx_distinct, delta.approx_distinct)

    # Union patterns and anomalies
    patterns = list(dict.fromkeys(prior.patterns + delta.patterns))
    anomalies = list(dict.fromkeys(prior.anomalies + delta.anomalies))
    pattern_scores = {**prior.pattern_scores, **delta.pattern_scores}

    return ColumnProfile(
        name=delta.name,
        engine_type=delta.engine_type,
        canonical_type=delta.canonical_type,
        comment=delta.comment or prior.comment,
        nullable=delta.nullable,
        null_count=null_count,
        null_rate=null_rate,
        min=merged_min,
        max=merged_max,
        mean=merged_mean,
        sum=merged_sum,
        stddev=merged_stddev,
        variance=merged_variance,
        median=delta.median,  # not mergeable
        p5=delta.p5,
        p25=delta.p25,
        p75=delta.p75,
        p95=delta.p95,
        iqr=delta.iqr,
        range=delta.range,
        cv=delta.cv,
        mad=delta.mad,
        approx_distinct=approx_distinct,
        distinct_mode=delta.distinct_mode,
        min_length=_safe_min(prior.min_length, delta.min_length),
        max_length=_safe_max(prior.max_length, delta.max_length),
        avg_length=delta.avg_length,
        zero_count=merged_zero,
        negative_count=merged_neg,
        infinite_count=_safe_add(prior.infinite_count, delta.infinite_count),
        empty_count=_safe_add(prior.empty_count, delta.empty_count),
        whitespace_count=_safe_add(prior.whitespace_count, delta.whitespace_count),
        leading_trailing_whitespace_count=_safe_add(
            prior.leading_trailing_whitespace_count,
            delta.leading_trailing_whitespace_count,
        ),
        unique_count=delta.unique_count,
        uniqueness_ratio=delta.uniqueness_ratio,
        true_count=_safe_add(prior.true_count, delta.true_count),
        false_count=_safe_add(prior.false_count, delta.false_count),
        true_rate=delta.true_rate,
        false_rate=delta.false_rate,
        imbalance_ratio=delta.imbalance_ratio,
        distinct_ratio=delta.distinct_ratio,
        pk_candidate=delta.pk_candidate,
        box_plot=delta.box_plot,
        freshness_days=delta.freshness_days,
        date_range_days=delta.date_range_days,
        granularity_guess=delta.granularity_guess,
        skewness=delta.skewness,
        kurtosis=delta.kurtosis,
        is_monotonic_increasing=delta.is_monotonic_increasing,
        is_monotonic_decreasing=delta.is_monotonic_decreasing,
        kde=delta.kde,
        histogram=delta.histogram,
        cdf=delta.cdf,
        qq_plot=delta.qq_plot,
        length_histogram=delta.length_histogram,
        benford_digits=delta.benford_digits,
        benford_pvalue=delta.benford_pvalue,
        top_values=delta.top_values,
        bottom_values=delta.bottom_values,
        patterns=patterns,
        pattern_scores=pattern_scores,
        anomalies=anomalies,
    )


def _safe_min(a: Any, b: Any) -> Any:
    if a is None:
        return b
    if b is None:
        return a
    return min(a, b)


def _safe_max(a: Any, b: Any) -> Any:
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)


def _safe_add(a: Any, b: Any) -> Any:
    if a is None and b is None:
        return None
    return (a or 0) + (b or 0)
