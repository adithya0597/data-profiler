"""Unit tests for anomaly detection rules."""

import pytest
from data_profiler.config import AnomalyThresholds
from data_profiler.enrichment.anomaly import apply_anomaly_rules, _build_anomaly_rules
from data_profiler.workers.stats_worker import ColumnProfile


def _make_col(**kwargs) -> ColumnProfile:
    defaults = dict(
        name="test_col",
        engine_type="INTEGER",
        canonical_type="integer",
        comment=None,
        nullable=True,
        null_count=0,
        null_rate=0.0,
        min=0,
        max=100,
        mean=50.0,
        stddev=10.0,
        variance=100.0,
        median=50.0,
        p25=25.0,
        p75=75.0,
        approx_distinct=50,
        distinct_mode="approx",
        max_length=None,
        avg_length=None,
        zero_count=0,
        negative_count=0,
        true_count=None,
        top_values=None,
        anomalies=[],
    )
    defaults.update(kwargs)
    return ColumnProfile(**defaults)


class TestHighNullRate:
    def test_triggers_above_50pct(self):
        col = _make_col(null_rate=0.6, null_count=60)
        assert "high_null_rate" in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_below_50pct(self):
        col = _make_col(null_rate=0.3, null_count=30)
        assert "high_null_rate" not in apply_anomaly_rules(col, 100)

    def test_boundary_at_50pct(self):
        col = _make_col(null_rate=0.5, null_count=50)
        assert "high_null_rate" not in apply_anomaly_rules(col, 100)


class TestSingleValue:
    def test_triggers_on_one_distinct(self):
        col = _make_col(approx_distinct=1)
        assert "single_value" in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_on_multiple_distinct(self):
        col = _make_col(approx_distinct=2)
        assert "single_value" not in apply_anomaly_rules(col, 100)


class TestAllUnique:
    def test_triggers_when_distinct_equals_row_count(self):
        col = _make_col(approx_distinct=1000)
        assert "all_unique" in apply_anomaly_rules(col, 1000)

    def test_does_not_trigger_when_low_ratio(self):
        col = _make_col(approx_distinct=50)
        assert "all_unique" not in apply_anomaly_rules(col, 1000)

    def test_does_not_trigger_on_single_row(self):
        col = _make_col(approx_distinct=1)
        assert "all_unique" not in apply_anomaly_rules(col, 1)

    def test_handles_hll_undercount(self):
        # HLL might report 990 for truly 1000 distinct values
        col = _make_col(approx_distinct=995)
        assert "all_unique" in apply_anomaly_rules(col, 1000)


class TestEmptyStringDominant:
    def test_triggers_on_zero_max_length_string(self):
        col = _make_col(canonical_type="string", max_length=0)
        assert "empty_string_dominant" in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_on_non_string(self):
        col = _make_col(canonical_type="integer", max_length=0)
        assert "empty_string_dominant" not in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_when_max_length_none(self):
        col = _make_col(canonical_type="string", max_length=None)
        assert "empty_string_dominant" not in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_when_max_length_positive(self):
        col = _make_col(canonical_type="string", max_length=10)
        assert "empty_string_dominant" not in apply_anomaly_rules(col, 100)


class TestZeroVariance:
    def test_triggers_on_zero_stddev_multiple_distinct(self):
        col = _make_col(stddev=0.0, approx_distinct=5)
        assert "zero_variance" in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_on_single_distinct(self):
        col = _make_col(stddev=0.0, approx_distinct=1)
        assert "zero_variance" not in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_on_nonzero_stddev(self):
        col = _make_col(stddev=5.0, approx_distinct=50)
        assert "zero_variance" not in apply_anomaly_rules(col, 100)


class TestNearConstant:
    def test_triggers_when_top_value_dominates(self):
        col = _make_col(top_values=[{"value": "A", "count": 960}])
        assert "near_constant" in apply_anomaly_rules(col, 1000)

    def test_does_not_trigger_when_top_value_below_threshold(self):
        col = _make_col(top_values=[{"value": "A", "count": 500}])
        assert "near_constant" not in apply_anomaly_rules(col, 1000)

    def test_does_not_trigger_without_top_values(self):
        col = _make_col(top_values=None)
        assert "near_constant" not in apply_anomaly_rules(col, 1000)


class TestHighCardinality:
    def test_triggers_on_high_distinct_string(self):
        col = _make_col(canonical_type="string", approx_distinct=15000, max_length=20)
        assert "high_cardinality" in apply_anomaly_rules(col, 100000)

    def test_does_not_trigger_on_integer(self):
        col = _make_col(canonical_type="integer", approx_distinct=15000)
        assert "high_cardinality" not in apply_anomaly_rules(col, 100000)

    def test_does_not_trigger_on_low_distinct_string(self):
        col = _make_col(canonical_type="string", approx_distinct=100, max_length=10)
        assert "high_cardinality" not in apply_anomaly_rules(col, 1000)


class TestHasNegatives:
    def test_triggers_with_negative_values(self):
        col = _make_col(negative_count=5)
        assert "has_negatives" in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_with_zero_negatives(self):
        col = _make_col(negative_count=0)
        assert "has_negatives" not in apply_anomaly_rules(col, 100)

    def test_does_not_trigger_with_none(self):
        col = _make_col(negative_count=None)
        assert "has_negatives" not in apply_anomaly_rules(col, 100)


class TestConfigurableThresholds:
    """B4: Configurable anomaly thresholds via AnomalyThresholds model."""

    def test_custom_high_null_threshold(self):
        """Custom threshold changes when high_null_rate triggers."""
        col = _make_col(null_rate=0.3, null_count=30)
        # Default threshold 0.5 -> should NOT trigger
        assert "high_null_rate" not in apply_anomaly_rules(col, 100)
        # Custom threshold 0.2 -> SHOULD trigger
        thresholds = AnomalyThresholds(high_null_rate=0.2)
        assert "high_null_rate" in apply_anomaly_rules(col, 100, thresholds=thresholds)

    def test_custom_near_constant_threshold(self):
        col = _make_col(top_values=[{"value": "A", "count": 850}])
        # Default 0.95 -> should NOT trigger
        assert "near_constant" not in apply_anomaly_rules(col, 1000)
        # Custom 0.8 -> SHOULD trigger
        thresholds = AnomalyThresholds(near_constant_rate=0.8)
        assert "near_constant" in apply_anomaly_rules(col, 1000, thresholds=thresholds)

    def test_custom_high_cardinality_threshold(self):
        col = _make_col(canonical_type="string", approx_distinct=5000, max_length=20)
        # Default 10000 -> should NOT trigger
        assert "high_cardinality" not in apply_anomaly_rules(col, 100000)
        # Custom 3000 -> SHOULD trigger
        thresholds = AnomalyThresholds(high_cardinality_distinct=3000)
        assert "high_cardinality" in apply_anomaly_rules(col, 100000, thresholds=thresholds)

    def test_default_thresholds_unchanged(self):
        """Default AnomalyThresholds should match original hardcoded values."""
        t = AnomalyThresholds()
        assert t.high_null_rate == 0.5
        assert t.near_constant_rate == 0.95
        assert t.high_cardinality_distinct == 10_000
        assert t.all_unique_ratio == 0.99
        assert t.all_unique_hll_guard == 1.05

    def test_none_thresholds_uses_defaults(self):
        """Passing thresholds=None uses the default module-level rules."""
        col = _make_col(null_rate=0.6, null_count=60)
        result_none = apply_anomaly_rules(col, 100, thresholds=None)
        result_default = apply_anomaly_rules(col, 100, thresholds=AnomalyThresholds())
        assert result_none == result_default

    def test_build_anomaly_rules_returns_correct_count(self):
        """Factory function returns all 19 rules."""
        rules = _build_anomaly_rules()
        assert len(rules) == 19

    def test_pydantic_validation_rejects_invalid(self):
        """Invalid threshold values are rejected by Pydantic validation."""
        with pytest.raises(Exception):
            AnomalyThresholds(high_null_rate=2.0)  # Must be <= 1.0
        with pytest.raises(Exception):
            AnomalyThresholds(high_cardinality_distinct=0)  # Must be >= 1


class TestBenfordAnomaly:
    """Feature 4: Benford's Law anomaly rule."""

    def test_benford_anomaly_fires_on_low_pvalue(self):
        """benford_anomaly triggers when p-value < 0.01 and row count > 1000."""
        col = _make_col(
            canonical_type="integer",
            benford_pvalue=0.001,
        )
        result = apply_anomaly_rules(col, 5000)
        assert "benford_anomaly" in result

    def test_benford_anomaly_does_not_fire_on_high_pvalue(self):
        col = _make_col(
            canonical_type="integer",
            benford_pvalue=0.5,
        )
        result = apply_anomaly_rules(col, 5000)
        assert "benford_anomaly" not in result

    def test_benford_anomaly_does_not_fire_on_small_table(self):
        """Needs > 1000 rows to trigger."""
        col = _make_col(
            canonical_type="integer",
            benford_pvalue=0.001,
        )
        result = apply_anomaly_rules(col, 500)
        assert "benford_anomaly" not in result

    def test_benford_anomaly_does_not_fire_without_pvalue(self):
        col = _make_col(canonical_type="integer")
        result = apply_anomaly_rules(col, 5000)
        assert "benford_anomaly" not in result


class TestIQROutlierRule:
    """Feature 5: IQR outlier detection (Tukey's fences)."""

    def test_iqr_outlier_fires_when_max_beyond_fence(self):
        # p25=25, p75=75, iqr=50, upper fence=75+75=150 — max=1000 exceeds it
        col = _make_col(p25=25.0, p75=75.0, min=0, max=1000)
        assert "iqr_outliers" in apply_anomaly_rules(col, 100)

    def test_iqr_outlier_fires_when_min_below_fence(self):
        # p25=25, p75=75, iqr=50, lower fence=25-75=-50 — min=-500 below it
        col = _make_col(p25=25.0, p75=75.0, min=-500, max=100)
        assert "iqr_outliers" in apply_anomaly_rules(col, 100)

    def test_iqr_outlier_does_not_fire_within_fences(self):
        # p25=25, p75=75, iqr=50, fences=[-50, 150] — min=0, max=100 are inside
        col = _make_col(p25=25.0, p75=75.0, min=0, max=100)
        assert "iqr_outliers" not in apply_anomaly_rules(col, 100)

    def test_iqr_outlier_skipped_when_missing_percentiles(self):
        col = _make_col(p25=None, p75=None, min=0, max=1000)
        assert "iqr_outliers" not in apply_anomaly_rules(col, 100)

    def test_iqr_outlier_skipped_when_iqr_is_zero(self):
        col = _make_col(p25=50.0, p75=50.0, min=50, max=50)
        assert "iqr_outliers" not in apply_anomaly_rules(col, 100)


class TestZScoreOutlierRule:
    """Feature 6: Z-score outlier detection (|z| > 3.0)."""

    def test_zscore_outlier_fires_on_extreme_max(self):
        # mean=50, stddev=10, max=1000 → z=95 > 3
        col = _make_col(mean=50.0, stddev=10.0, min=0, max=1000)
        assert "zscore_outliers" in apply_anomaly_rules(col, 100)

    def test_zscore_outlier_fires_on_extreme_min(self):
        # mean=50, stddev=10, min=-500 → z=55 > 3
        col = _make_col(mean=50.0, stddev=10.0, min=-500, max=100)
        assert "zscore_outliers" in apply_anomaly_rules(col, 100)

    def test_zscore_outlier_does_not_fire_within_threshold(self):
        # mean=50, stddev=10, min=25, max=75 → max z=2.5 < 3
        col = _make_col(mean=50.0, stddev=10.0, min=25, max=75)
        assert "zscore_outliers" not in apply_anomaly_rules(col, 100)

    def test_zscore_outlier_skipped_when_stddev_zero(self):
        col = _make_col(mean=50.0, stddev=0.0, min=50, max=50)
        assert "zscore_outliers" not in apply_anomaly_rules(col, 100)


class TestHighSkewnessRule:
    """Feature 7: Skewness alert (|skewness| > 2.0)."""

    def test_high_skewness_fires_on_positive_skew(self):
        col = _make_col(skewness=3.5)
        assert "high_skewness" in apply_anomaly_rules(col, 100)

    def test_high_skewness_fires_on_negative_skew(self):
        col = _make_col(skewness=-2.5)
        assert "high_skewness" in apply_anomaly_rules(col, 100)

    def test_high_skewness_does_not_fire_within_threshold(self):
        col = _make_col(skewness=1.0)
        assert "high_skewness" not in apply_anomaly_rules(col, 100)

    def test_high_skewness_skipped_when_none(self):
        col = _make_col(skewness=None)
        assert "high_skewness" not in apply_anomaly_rules(col, 100)

    def test_high_skewness_skipped_for_non_numeric(self):
        col = _make_col(canonical_type="string", skewness=5.0)
        assert "high_skewness" not in apply_anomaly_rules(col, 100)


class TestStaleDateRule:
    """Stale data rule: date/datetime not updated in > 90 days."""

    def test_fires_on_old_date(self):
        col = _make_col(canonical_type="date", freshness_days=200)
        assert "stale_data" in apply_anomaly_rules(col, 100)

    def test_does_not_fire_on_recent_date(self):
        col = _make_col(canonical_type="date", freshness_days=10)
        assert "stale_data" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_on_non_date(self):
        col = _make_col(canonical_type="integer", freshness_days=200)
        assert "stale_data" not in apply_anomaly_rules(col, 100)

    def test_skipped_when_freshness_none(self):
        col = _make_col(canonical_type="date")
        assert "stale_data" not in apply_anomaly_rules(col, 100)


class TestMADOutlierRule:
    """Feature 9: MAD-based outlier detection (modified z-score, Iglewicz & Hoaglin)."""

    def test_fires_on_extreme_min(self):
        # median=50, mad=5, min=-1000 -> modified_z = 0.6745*(50-(-1000))/5 = 141 >> 3.5
        col = _make_col(median=50.0, mad=5.0, min=-1000, max=60)
        assert "mad_outliers" in apply_anomaly_rules(col, 100)

    def test_fires_on_extreme_max(self):
        col = _make_col(median=50.0, mad=5.0, min=40, max=1000)
        assert "mad_outliers" in apply_anomaly_rules(col, 100)

    def test_does_not_fire_within_bounds(self):
        # median=50, mad=5, min=45, max=55 -> modified_z = 0.6745*(5)/5 = 0.67 < 3.5
        col = _make_col(median=50.0, mad=5.0, min=45, max=55)
        assert "mad_outliers" not in apply_anomaly_rules(col, 100)

    def test_skipped_when_mad_zero(self):
        col = _make_col(median=50.0, mad=0.0, min=0, max=1000)
        assert "mad_outliers" not in apply_anomaly_rules(col, 100)

    def test_skipped_when_mad_none(self):
        col = _make_col(median=50.0, min=0, max=1000)
        assert "mad_outliers" not in apply_anomaly_rules(col, 100)


class TestBooleanImbalanceRule:
    """Feature 8: Boolean imbalance detection (minority class < 1%)."""

    def test_fires_when_almost_all_true(self):
        # 99.5% true → false class < 1%
        col = _make_col(canonical_type="boolean", true_count=995)
        assert "boolean_imbalance" in apply_anomaly_rules(col, 1000)

    def test_fires_when_almost_all_false(self):
        # 0.5% true → true class < 1%
        col = _make_col(canonical_type="boolean", true_count=5)
        assert "boolean_imbalance" in apply_anomaly_rules(col, 1000)

    def test_does_not_fire_on_balanced(self):
        col = _make_col(canonical_type="boolean", true_count=500)
        assert "boolean_imbalance" not in apply_anomaly_rules(col, 1000)

    def test_skipped_when_true_count_none(self):
        col = _make_col(canonical_type="boolean", true_count=None)
        assert "boolean_imbalance" not in apply_anomaly_rules(col, 1000)

    def test_skipped_for_non_boolean(self):
        col = _make_col(canonical_type="integer", true_count=5)
        assert "boolean_imbalance" not in apply_anomaly_rules(col, 1000)


class TestSuspiciousUniformLengthRule:
    """Suspicious uniform length: all strings have the same non-zero length (structured IDs/codes)."""

    def test_fires_when_all_same_length(self):
        # ZIP codes: all 5 chars → suspicious
        col = _make_col(canonical_type="string", min_length=5, max_length=5)
        assert "suspicious_uniform_length" in apply_anomaly_rules(col, 100)

    def test_fires_for_fixed_hash_length(self):
        # MD5 hashes: all 32 chars
        col = _make_col(canonical_type="string", min_length=32, max_length=32)
        assert "suspicious_uniform_length" in apply_anomaly_rules(col, 1000)

    def test_does_not_fire_when_lengths_vary(self):
        # Normal text: 1–100 chars
        col = _make_col(canonical_type="string", min_length=1, max_length=100)
        assert "suspicious_uniform_length" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_when_all_empty(self):
        # empty_string_dominant handles this; uniform_length should NOT fire for length=0
        col = _make_col(canonical_type="string", min_length=0, max_length=0)
        assert "suspicious_uniform_length" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_for_non_string(self):
        col = _make_col(canonical_type="integer", min_length=5, max_length=5)
        assert "suspicious_uniform_length" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_when_lengths_none(self):
        col = _make_col(canonical_type="string", min_length=None, max_length=None)
        assert "suspicious_uniform_length" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_for_single_row(self):
        # n=1 → trivially all same length, not suspicious
        col = _make_col(canonical_type="string", min_length=5, max_length=5)
        assert "suspicious_uniform_length" not in apply_anomaly_rules(col, 1)


class TestLowCardinalityNumericRule:
    """Low cardinality numeric: integer/float with < 10 distinct values."""

    def test_fires_on_status_column(self):
        # Status column with values 1/2/3 → low cardinality
        col = _make_col(canonical_type="integer", approx_distinct=3)
        assert "low_cardinality_numeric" in apply_anomaly_rules(col, 1000)

    def test_fires_on_exactly_9_distinct(self):
        col = _make_col(canonical_type="float", approx_distinct=9)
        assert "low_cardinality_numeric" in apply_anomaly_rules(col, 500)

    def test_does_not_fire_on_10_distinct(self):
        col = _make_col(canonical_type="integer", approx_distinct=10)
        assert "low_cardinality_numeric" not in apply_anomaly_rules(col, 1000)

    def test_does_not_fire_for_string(self):
        col = _make_col(canonical_type="string", approx_distinct=3)
        assert "low_cardinality_numeric" not in apply_anomaly_rules(col, 1000)

    def test_does_not_fire_on_small_table(self):
        # n=50 < 100 → not enough data to call it low cardinality
        col = _make_col(canonical_type="integer", approx_distinct=3)
        assert "low_cardinality_numeric" not in apply_anomaly_rules(col, 50)


class TestLeadingTrailingWhitespaceRule:
    """Leading/trailing whitespace detection."""

    def test_fires_when_whitespace_present(self):
        col = _make_col(canonical_type="string", leading_trailing_whitespace_count=5)
        assert "has_leading_trailing_whitespace" in apply_anomaly_rules(col, 100)

    def test_does_not_fire_when_zero(self):
        col = _make_col(canonical_type="string", leading_trailing_whitespace_count=0)
        assert "has_leading_trailing_whitespace" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_when_none(self):
        col = _make_col(canonical_type="string")
        assert "has_leading_trailing_whitespace" not in apply_anomaly_rules(col, 100)

    def test_does_not_fire_for_non_string(self):
        col = _make_col(canonical_type="integer", leading_trailing_whitespace_count=5)
        assert "has_leading_trailing_whitespace" not in apply_anomaly_rules(col, 100)
