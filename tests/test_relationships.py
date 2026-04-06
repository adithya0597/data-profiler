"""Unit tests for cross-table relationship discovery."""

import pytest
from data_profiler.enrichment.constraints import TableConstraints
from data_profiler.workers.relationship_worker import (
    Relationship,
    discover_relationships,
    relationships_to_dict,
)
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


def _make_table(name, columns=None, constraints=None):
    return ProfiledTable(
        name=name,
        comment=None,
        total_row_count=1000,
        sampled_row_count=1000,
        columns=columns or [],
        constraints=constraints,
    )


def _make_col(name, canonical_type="integer", anomalies=None):
    return ColumnProfile(
        name=name,
        engine_type="INTEGER",
        canonical_type=canonical_type,
        comment=None,
        nullable=True,
        anomalies=anomalies or [],
    )


class TestDeclaredFKs:
    def test_collects_declared_fk(self):
        constraints = TableConstraints(
            foreign_keys=[{
                "constrained_columns": ["customer_sk"],
                "referred_table": "customer",
                "referred_columns": ["c_customer_sk"],
                "name": "fk_cust",
            }]
        )
        tables = [
            _make_table("store_sales", constraints=constraints),
            _make_table("customer"),
        ]
        rels = discover_relationships(tables, include_inferred=False)
        assert len(rels) == 1
        assert rels[0].source_table == "store_sales"
        assert rels[0].target_table == "customer"
        assert rels[0].relationship_type == "declared_fk"

    def test_multiple_fks_from_one_table(self):
        constraints = TableConstraints(
            foreign_keys=[
                {"constrained_columns": ["a"], "referred_table": "t1", "referred_columns": ["id"], "name": "fk1"},
                {"constrained_columns": ["b"], "referred_table": "t2", "referred_columns": ["id"], "name": "fk2"},
            ]
        )
        tables = [_make_table("source", constraints=constraints)]
        rels = discover_relationships(tables, include_inferred=False)
        assert len(rels) == 2

    def test_no_constraints_returns_empty(self):
        tables = [_make_table("t1"), _make_table("t2")]
        rels = discover_relationships(tables, include_inferred=False)
        assert rels == []


class TestInferredRelationships:
    def test_infers_relationship_from_matching_column(self):
        """Two tables share 'customer_id', one side is all_unique -> inferred FK."""
        tables = [
            _make_table("orders", columns=[
                _make_col("customer_id", anomalies=[]),
            ]),
            _make_table("customer", columns=[
                _make_col("customer_id", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        inferred = [r for r in rels if r.relationship_type == "inferred"]
        assert len(inferred) == 1
        assert inferred[0].source_table == "orders"
        assert inferred[0].target_table == "customer"

    def test_no_inference_without_unique(self):
        """Both sides non-unique -> no inferred relationship."""
        tables = [
            _make_table("t1", columns=[_make_col("shared_col")]),
            _make_table("t2", columns=[_make_col("shared_col")]),
        ]
        rels = discover_relationships(tables)
        assert len(rels) == 0

    def test_no_inference_type_mismatch(self):
        """Same name but different types -> no inferred relationship."""
        tables = [
            _make_table("t1", columns=[_make_col("id", "integer", ["all_unique"])]),
            _make_table("t2", columns=[_make_col("id", "string")]),
        ]
        rels = discover_relationships(tables)
        inferred = [r for r in rels if r.relationship_type == "inferred"]
        assert len(inferred) == 0

    def test_skips_already_declared_fk(self):
        """Don't duplicate a declared FK as an inferred one."""
        constraints = TableConstraints(
            foreign_keys=[{
                "constrained_columns": ["id"],
                "referred_table": "t2",
                "referred_columns": ["id"],
                "name": "fk_id",
            }]
        )
        tables = [
            _make_table("t1", columns=[_make_col("id")], constraints=constraints),
            _make_table("t2", columns=[_make_col("id", anomalies=["all_unique"])]),
        ]
        rels = discover_relationships(tables)
        assert len(rels) == 1
        assert rels[0].relationship_type == "declared_fk"


class TestConfidenceField:
    """B2: Inferred relationships get confidence scores."""

    def test_declared_fk_has_confidence_1(self):
        constraints = TableConstraints(
            foreign_keys=[{
                "constrained_columns": ["customer_sk"],
                "referred_table": "customer",
                "referred_columns": ["c_customer_sk"],
                "name": "fk_cust",
            }]
        )
        tables = [
            _make_table("store_sales", constraints=constraints),
            _make_table("customer"),
        ]
        rels = discover_relationships(tables, include_inferred=False)
        assert rels[0].confidence == 1.0

    def test_inferred_has_heuristic_confidence(self):
        """Without engine, inferred relationships get 0.5 (heuristic)."""
        tables = [
            _make_table("orders", columns=[_make_col("customer_id")]),
            _make_table("customer", columns=[
                _make_col("customer_id", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables, engine=None)
        inferred = [r for r in rels if r.relationship_type == "inferred"]
        assert len(inferred) == 1
        assert inferred[0].confidence == 0.5
        assert inferred[0].overlap_count is None

    def test_confidence_in_serialization(self):
        rels = [
            Relationship(
                source_table="a", source_columns=["id"],
                target_table="b", target_columns=["id"],
                relationship_type="inferred", confidence=0.5,
            ),
        ]
        d = relationships_to_dict(rels)
        assert d["relationships"][0]["confidence"] == 0.5


class TestSerialization:
    def test_relationships_to_dict(self):
        rels = [
            Relationship(
                source_table="orders",
                source_columns=["customer_id"],
                target_table="customer",
                target_columns=["c_customer_sk"],
                relationship_type="declared_fk",
            ),
        ]
        d = relationships_to_dict(rels)
        assert d["_relationships"] is True
        assert d["count"] == 1
        assert d["relationships"][0]["source_table"] == "orders"

    def test_empty_relationships(self):
        d = relationships_to_dict([])
        assert d["count"] == 0
        assert d["relationships"] == []
