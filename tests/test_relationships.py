"""Unit tests for cross-table relationship discovery."""

import pytest
from data_profiler.enrichment.constraints import TableConstraints
from data_profiler.workers.relationship_worker import (
    Relationship,
    _extract_entity_hint,
    _discover_semantic_fks,
    _discover_composite_fks,
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
        """Two tables share 'customer_id', one side is all_unique -> FK discovered.
        May be discovered as semantic_fk (naming pattern) or inferred (exact name match)."""
        tables = [
            _make_table("orders", columns=[
                _make_col("customer_id", anomalies=[]),
            ]),
            _make_table("customer", columns=[
                _make_col("customer_id", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        fk_rels = [r for r in rels if r.relationship_type in ("inferred", "semantic_fk")]
        assert len(fk_rels) == 1
        assert fk_rels[0].source_table == "orders"
        assert fk_rels[0].target_table == "customer"

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
        """Without engine, discovered relationships get heuristic confidence.
        customer_id→customer is caught by semantic FK (0.6) or exact match (0.5)."""
        tables = [
            _make_table("orders", columns=[_make_col("customer_id")]),
            _make_table("customer", columns=[
                _make_col("customer_id", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables, engine=None)
        fk_rels = [r for r in rels if r.relationship_type in ("inferred", "semantic_fk")]
        assert len(fk_rels) == 1
        assert fk_rels[0].confidence in (0.5, 0.6)
        assert fk_rels[0].overlap_count is None

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


# ---- Feature 6: Semantic FK Discovery ----

class TestEntityHintExtraction:
    """Unit tests for _extract_entity_hint."""

    def test_sk_suffix(self):
        assert _extract_entity_hint("ss_customer_sk", "store_sales") == "customer"

    def test_id_suffix(self):
        assert _extract_entity_hint("ss_item_id", "store_sales") == "item"

    def test_key_suffix(self):
        assert _extract_entity_hint("ws_ship_key", "web_sales") == "ship"

    def test_multi_word_entity(self):
        assert _extract_entity_hint("sr_returned_date_sk", "store_returns") == "returned_date"

    def test_no_suffix_returns_none(self):
        assert _extract_entity_hint("quantity", "store_sales") is None

    def test_no_prefix_uses_full_stem(self):
        # Column with no underscore prefix: "customer_sk" → stem "customer", no prefix to strip
        # Since split gives ["customer"] with len < 2, falls through to stem
        assert _extract_entity_hint("customer_sk", "customer") == "customer"


class TestSemanticFKs:
    """Feature 6: Discover FK relationships via naming conventions."""

    def test_tpcds_pattern_customer_sk(self):
        """ss_customer_sk in store_sales → c_customer_sk in customer."""
        tables = [
            _make_table("store_sales", columns=[
                _make_col("ss_customer_sk"),
                _make_col("ss_quantity"),
            ]),
            _make_table("customer", columns=[
                _make_col("c_customer_sk", anomalies=["all_unique"]),
                _make_col("c_first_name", canonical_type="string"),
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 1
        assert semantic[0].source_table == "store_sales"
        assert semantic[0].source_columns == ["ss_customer_sk"]
        assert semantic[0].target_table == "customer"
        assert semantic[0].target_columns == ["c_customer_sk"]
        assert semantic[0].confidence == 0.6

    def test_dim_table_suffix(self):
        """ss_sold_date_sk in store_sales → d_date_sk in date_dim."""
        # Entity hint: "sold_date" from "ss_sold_date_sk" — won't match "date_dim"
        # This tests that we need the entity to match the table name
        tables = [
            _make_table("store_sales", columns=[
                _make_col("ss_date_sk"),  # entity = "date"
            ]),
            _make_table("date_dim", columns=[
                _make_col("date_sk", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 1
        assert semantic[0].target_table == "date_dim"

    def test_no_match_without_unique_target(self):
        """Semantic FK not emitted if target column is not all_unique."""
        tables = [
            _make_table("store_sales", columns=[
                _make_col("ss_customer_sk"),
            ]),
            _make_table("customer", columns=[
                _make_col("c_customer_sk", anomalies=[]),  # NOT unique
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 0

    def test_type_mismatch_blocks_semantic_fk(self):
        """Semantic FK not emitted if types don't match."""
        tables = [
            _make_table("store_sales", columns=[
                _make_col("ss_customer_sk", canonical_type="string"),
            ]),
            _make_table("customer", columns=[
                _make_col("c_customer_sk", canonical_type="integer", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 0

    def test_no_self_reference(self):
        """Table doesn't generate semantic FK to itself."""
        tables = [
            _make_table("customer", columns=[
                _make_col("c_customer_sk", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 0

    def test_does_not_duplicate_declared_fk(self):
        """Semantic FK skipped if already declared."""
        constraints = TableConstraints(
            foreign_keys=[{
                "constrained_columns": ["ss_customer_sk"],
                "referred_table": "customer",
                "referred_columns": ["c_customer_sk"],
                "name": "fk_cust",
            }]
        )
        tables = [
            _make_table("store_sales", columns=[
                _make_col("ss_customer_sk"),
            ], constraints=constraints),
            _make_table("customer", columns=[
                _make_col("c_customer_sk", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        assert sum(1 for r in rels if r.source_columns == ["ss_customer_sk"]) == 1

    def test_id_suffix_pattern(self):
        """Column ending in _id also triggers semantic FK."""
        tables = [
            _make_table("orders", columns=[
                _make_col("o_customer_id"),
            ]),
            _make_table("customer", columns=[
                _make_col("customer_id", anomalies=["all_unique"]),
            ]),
        ]
        rels = discover_relationships(tables)
        semantic = [r for r in rels if r.relationship_type == "semantic_fk"]
        assert len(semantic) == 1


# ---- Feature 7: Composite Key Relationship Detection ----

class TestCompositeKeys:
    """Feature 7: Detect composite key relationships from declared multi-column PKs."""

    def test_composite_pk_matched_in_other_table(self):
        """inventory has (inv_item_sk, inv_warehouse_sk) PK. Another table has both columns."""
        pk_constraints = TableConstraints(
            primary_key=["inv_item_sk", "inv_warehouse_sk"],
        )
        tables = [
            _make_table("inventory", columns=[
                _make_col("inv_item_sk"),
                _make_col("inv_warehouse_sk"),
                _make_col("inv_quantity"),
            ], constraints=pk_constraints),
            _make_table("inventory_snapshot", columns=[
                _make_col("inv_item_sk"),
                _make_col("inv_warehouse_sk"),
                _make_col("snapshot_date", canonical_type="date"),
            ]),
        ]
        rels = discover_relationships(tables)
        composite = [r for r in rels if r.relationship_type == "inferred_composite"]
        assert len(composite) == 1
        assert composite[0].source_table == "inventory_snapshot"
        assert composite[0].target_table == "inventory"
        assert set(composite[0].source_columns) == {"inv_item_sk", "inv_warehouse_sk"}

    def test_no_composite_when_columns_missing(self):
        """If the other table is missing one PK column, no composite FK."""
        pk_constraints = TableConstraints(
            primary_key=["col_a", "col_b"],
        )
        tables = [
            _make_table("pk_table", columns=[
                _make_col("col_a"),
                _make_col("col_b"),
            ], constraints=pk_constraints),
            _make_table("other", columns=[
                _make_col("col_a"),
                # col_b missing
            ]),
        ]
        rels = discover_relationships(tables)
        composite = [r for r in rels if r.relationship_type == "inferred_composite"]
        assert len(composite) == 0

    def test_no_composite_when_types_mismatch(self):
        pk_constraints = TableConstraints(
            primary_key=["col_a", "col_b"],
        )
        tables = [
            _make_table("pk_table", columns=[
                _make_col("col_a", "integer"),
                _make_col("col_b", "integer"),
            ], constraints=pk_constraints),
            _make_table("other", columns=[
                _make_col("col_a", "integer"),
                _make_col("col_b", "string"),  # type mismatch
            ]),
        ]
        rels = discover_relationships(tables)
        composite = [r for r in rels if r.relationship_type == "inferred_composite"]
        assert len(composite) == 0

    def test_single_pk_not_treated_as_composite(self):
        """Single-column PKs are handled by Phase 2, not composite detection."""
        pk_constraints = TableConstraints(primary_key=["id"])
        tables = [
            _make_table("pk_table", columns=[
                _make_col("id", anomalies=["all_unique"]),
            ], constraints=pk_constraints),
            _make_table("other", columns=[_make_col("id")]),
        ]
        rels = discover_relationships(tables)
        composite = [r for r in rels if r.relationship_type == "inferred_composite"]
        assert len(composite) == 0

    def test_no_self_composite(self):
        """Table doesn't match its own composite PK."""
        pk_constraints = TableConstraints(
            primary_key=["a", "b"],
        )
        tables = [
            _make_table("t", columns=[
                _make_col("a"),
                _make_col("b"),
            ], constraints=pk_constraints),
        ]
        rels = discover_relationships(tables)
        composite = [r for r in rels if r.relationship_type == "inferred_composite"]
        assert len(composite) == 0
