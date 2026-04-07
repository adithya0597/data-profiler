"""Unit tests for constraint suggestion engine."""

from data_profiler.enrichment.constraint_suggester import (
    suggest_constraints, suggest_fk_constraints, SuggestedConstraint,
)
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable
from data_profiler.workers.relationship_worker import Relationship


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


class TestEnumSuggestion:
    def test_suggests_enum_for_low_cardinality_string(self):
        col = _make_col(
            name="status", canonical_type="string",
            approx_distinct=3, min=None, max=None, negative_count=None,
            top_values=[
                {"value": "active", "count": 500},
                {"value": "inactive", "count": 300},
                {"value": "pending", "count": 200},
            ],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s["expression"]]
        assert len(enums) == 1
        assert "IN (" in enums[0]["expression"]
        assert "'active'" in enums[0]["expression"]
        assert "'inactive'" in enums[0]["expression"]
        assert "'pending'" in enums[0]["expression"]
        assert enums[0]["constraint_type"] == "CHECK"

    def test_no_enum_for_high_cardinality(self):
        col = _make_col(
            name="city", canonical_type="string",
            approx_distinct=50, min=None, max=None, negative_count=None,
            top_values=[{"value": f"city_{i}", "count": 20} for i in range(5)],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s.get("expression", "")]
        assert len(enums) == 0

    def test_no_enum_for_numeric(self):
        col = _make_col(
            name="flag", canonical_type="integer",
            approx_distinct=2, min=0, max=1, negative_count=0,
            top_values=[{"value": "0", "count": 600}, {"value": "1", "count": 400}],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s.get("expression", "")]
        assert len(enums) == 0

    def test_no_enum_when_redacted(self):
        col = _make_col(
            name="ssn", canonical_type="string",
            approx_distinct=3, min=None, max=None, negative_count=None,
            top_values=[
                {"value": "[REDACTED]", "count": 500},
                {"value": "[REDACTED]", "count": 300},
                {"value": "[REDACTED]", "count": 200},
            ],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s.get("expression", "")]
        assert len(enums) == 0

    def test_no_enum_for_single_value(self):
        """approx_distinct must be > 1."""
        col = _make_col(
            name="constant", canonical_type="string",
            approx_distinct=1, min=None, max=None, negative_count=None,
            top_values=[{"value": "always", "count": 1000}],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s.get("expression", "")]
        assert len(enums) == 0

    def test_enum_escapes_single_quotes(self):
        col = _make_col(
            name="label", canonical_type="string",
            approx_distinct=2, min=None, max=None, negative_count=None,
            top_values=[
                {"value": "it's", "count": 500},
                {"value": "they're", "count": 500},
            ],
        )
        table = _make_table(columns=[col])
        suggestions = suggest_constraints(table)
        enums = [s for s in suggestions if "enum" in s.get("expression", "")]
        assert len(enums) == 1
        assert "'it''s'" in enums[0]["expression"]

    def test_enum_confidence_scales_with_row_count(self):
        col = _make_col(
            name="status", canonical_type="string",
            approx_distinct=3, min=None, max=None, negative_count=None,
            top_values=[
                {"value": "a", "count": 4},
                {"value": "b", "count": 3},
                {"value": "c", "count": 3},
            ],
        )
        # 10 rows → low confidence
        table_small = _make_table(columns=[col], total_row_count=10)
        small_enum = [s for s in suggest_constraints(table_small) if "enum" in s.get("expression", "")]
        assert small_enum[0]["confidence"] == 0.70

        # 50 rows → medium
        table_med = _make_table(columns=[col], total_row_count=50)
        med_enum = [s for s in suggest_constraints(table_med) if "enum" in s.get("expression", "")]
        assert med_enum[0]["confidence"] == 0.80

        # 1000 rows → high
        table_big = _make_table(columns=[col], total_row_count=1000)
        big_enum = [s for s in suggest_constraints(table_big) if "enum" in s.get("expression", "")]
        assert big_enum[0]["confidence"] == 0.90


class TestFKSuggestion:
    def test_suggests_fk_from_declared_relationship(self):
        rels = [Relationship(
            source_table="orders", source_columns=["customer_id"],
            target_table="customers", target_columns=["id"],
            relationship_type="declared_fk", confidence=1.0,
        )]
        suggestions = suggest_fk_constraints(rels)
        assert len(suggestions) == 1
        assert suggestions[0]["constraint_type"] == "FK"
        assert "FOREIGN KEY" in suggestions[0]["expression"]
        assert "REFERENCES" in suggestions[0]["expression"]
        assert suggestions[0]["confidence"] == 1.0

    def test_suggests_fk_from_inferred_relationship(self):
        rels = [Relationship(
            source_table="line_items", source_columns=["order_id"],
            target_table="orders", target_columns=["id"],
            relationship_type="inferred", confidence=0.85,
        )]
        suggestions = suggest_fk_constraints(rels)
        assert len(suggestions) == 1
        assert suggestions[0]["confidence"] == 0.85

    def test_composite_fk(self):
        rels = [Relationship(
            source_table="line_items", source_columns=["order_id", "product_id"],
            target_table="order_products", target_columns=["order_id", "product_id"],
            relationship_type="declared_fk", confidence=1.0,
        )]
        suggestions = suggest_fk_constraints(rels)
        assert len(suggestions) == 1
        expr = suggestions[0]["expression"]
        assert '"order_id", "product_id"' in expr or '"order_id"' in expr

    def test_empty_relationships(self):
        suggestions = suggest_fk_constraints([])
        assert suggestions == []

    def test_fk_uses_quote_fn(self):
        rels = [Relationship(
            source_table="orders", source_columns=["cust_id"],
            target_table="customers", target_columns=["id"],
            relationship_type="declared_fk", confidence=1.0,
        )]
        suggestions = suggest_fk_constraints(rels, quote_fn=lambda n: f"`{n}`")
        assert "`orders`" in suggestions[0]["expression"]
        assert "`customers`" in suggestions[0]["expression"]


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
