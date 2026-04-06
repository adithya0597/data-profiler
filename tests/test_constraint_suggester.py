"""Unit tests for constraint suggestion engine."""

from data_profiler.enrichment.constraint_suggester import suggest_constraints, SuggestedConstraint
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


def _make_col(**kwargs) -> ColumnProfile:
    defaults = dict(
        name="test_col", engine_type="INTEGER", canonical_type="integer",
        comment=None, nullable=True, null_count=0, null_rate=0.0,
        min=0, max=100, mean=50.0, approx_distinct=50, anomalies=[],
    )
    defaults.update(kwargs)
    return ColumnProfile(**defaults)


def _make_table(name="test_table", columns=None, total_row_count=1000) -> ProfiledTable:
    return ProfiledTable(
        name=name, comment=None,
        total_row_count=total_row_count, sampled_row_count=total_row_count,
        columns=columns or [],
    )


class TestNotNullSuggestion:
    def test_suggests_not_null_for_zero_null_rate(self):
        col = _make_col(name="id", null_rate=0.0, null_count=0, nullable=True)
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        not_null = [s for s in suggestions if s["constraint_type"] == "NOT NULL"]
        assert len(not_null) >= 1
        assert not_null[0]["column"] == "id"
        assert not_null[0]["confidence"] > 0.0

    def test_no_not_null_when_has_nulls(self):
        col = _make_col(name="name", null_rate=0.1, null_count=100, nullable=True)
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        not_null = [s for s in suggestions if s["constraint_type"] == "NOT NULL"]
        assert len(not_null) == 0

    def test_no_not_null_when_not_nullable(self):
        """Already NOT NULL columns should not get suggestions."""
        col = _make_col(name="id", null_rate=0.0, null_count=0, nullable=False)
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        not_null = [s for s in suggestions if s["constraint_type"] == "NOT NULL"]
        assert len(not_null) == 0


class TestUniqueSuggestion:
    def test_suggests_unique_when_all_unique(self):
        col = _make_col(name="email", anomalies=["all_unique"], approx_distinct=1000)
        table = _make_table(columns=[col], total_row_count=1000)
        suggestions = suggest_constraints(table)
        unique = [s for s in suggestions if s["constraint_type"] == "UNIQUE"]
        assert len(unique) == 1
        assert "UNIQUE" in unique[0]["expression"]

    def test_no_unique_without_anomaly(self):
        col = _make_col(name="status", anomalies=[], approx_distinct=5)
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        unique = [s for s in suggestions if s["constraint_type"] == "UNIQUE"]
        assert len(unique) == 0


class TestCheckSuggestion:
    def test_suggests_check_nonneg(self):
        col = _make_col(
            name="amount", canonical_type="integer",
            min=0, max=1000, negative_count=0,
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        checks = [s for s in suggestions if s["constraint_type"] == "CHECK"]
        assert len(checks) >= 1
        assert ">= 0" in checks[0]["expression"]

    def test_no_check_when_has_negatives(self):
        col = _make_col(
            name="balance", canonical_type="integer",
            min=-500, max=1000, negative_count=10,
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        checks = [s for s in suggestions if s["constraint_type"] == "CHECK"]
        assert len(checks) == 0

    def test_no_check_for_string(self):
        col = _make_col(
            name="label", canonical_type="string",
            min=None, max=None, negative_count=None,
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        checks = [s for s in suggestions if s["constraint_type"] == "CHECK"]
        assert len(checks) == 0


class TestEmptyTable:
    def test_no_suggestions_for_empty_table(self):
        col = _make_col(name="id", null_rate=0.0, null_count=0, nullable=True)
        table = _make_table(columns=[col], total_row_count=0)
        suggestions = suggest_constraints(table)
        assert len(suggestions) == 0


class TestSuggestedConstraintFields:
    def test_output_format(self):
        col = _make_col(name="pk", null_rate=0.0, null_count=0, nullable=True,
                        anomalies=["all_unique"], approx_distinct=1000,
                        negative_count=0, min=1, max=1000)
        table = _make_table(columns=[col], total_row_count=1000)
        suggestions = suggest_constraints(table)
        assert len(suggestions) > 0
        for s in suggestions:
            assert "table" in s
            assert "column" in s
            assert "constraint_type" in s
            assert "expression" in s
            assert "confidence" in s
            assert "evidence" in s
            assert isinstance(s["confidence"], float)
