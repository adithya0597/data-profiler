"""Unit tests for property graph export (GraphBuilder, JSON-LD, GraphML)."""

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from data_profiler.persistence.graph_model import (
    GraphBuilder,
    GraphNode,
    PropertyGraph,
    _make_urn,
)
from data_profiler.persistence.jsonld_serializer import JSONLDSerializer
from data_profiler.persistence.graphml_serializer import GraphMLSerializer
from data_profiler.persistence.serializers import create_serializer
from data_profiler.workers.stats_worker import ColumnProfile, ProfiledTable


def _make_col(name, canonical_type="integer", **kwargs):
    defaults = dict(
        engine_type="INTEGER", comment=None, nullable=True, anomalies=[],
    )
    defaults.update(kwargs)
    return ColumnProfile(name=name, canonical_type=canonical_type, **defaults)


def _make_table(name, columns=None, **kwargs):
    defaults = dict(
        comment=None, total_row_count=1000, sampled_row_count=1000,
        columns=columns or [],
    )
    defaults.update(kwargs)
    return ProfiledTable(name=name, **defaults)


# ---------------------------------------------------------------------------
# GraphBuilder
# ---------------------------------------------------------------------------

class TestGraphBuilder:
    def test_empty_graph_has_db_and_schema_nodes(self):
        builder = GraphBuilder(database="mydb", schema="public", engine="duckdb")
        graph = builder.build()
        assert len(graph.nodes) == 2
        labels = {n.label for n in graph.nodes}
        assert labels == {"Database", "Schema"}
        assert len(graph.edges) == 1
        assert graph.edges[0].label == "HAS_SCHEMA"

    def test_single_table_nodes_and_edges(self):
        builder = GraphBuilder(database="db", schema="s")
        col = _make_col("id", approx_distinct=100, null_rate=0.0)
        table = _make_table("users", columns=[col], profiled_at="2026-01-01T00:00:00Z")
        builder.add_table(table)
        graph = builder.build()

        # 2 (db+schema) + 1 table + 1 column = 4 nodes
        assert len(graph.nodes) == 4
        labels = [n.label for n in graph.nodes]
        assert labels.count("Database") == 1
        assert labels.count("Schema") == 1
        assert labels.count("Table") == 1
        assert labels.count("Column") == 1

        # 1 HAS_SCHEMA + 1 HAS_TABLE + 1 HAS_COLUMN = 3 edges
        edge_labels = [e.label for e in graph.edges]
        assert edge_labels.count("HAS_SCHEMA") == 1
        assert edge_labels.count("HAS_TABLE") == 1
        assert edge_labels.count("HAS_COLUMN") == 1

    def test_multiple_columns(self):
        builder = GraphBuilder(database="db", schema="s")
        cols = [_make_col("id"), _make_col("name", "string", engine_type="VARCHAR")]
        table = _make_table("t", columns=cols)
        builder.add_table(table)
        graph = builder.build()

        col_nodes = [n for n in graph.nodes if n.label == "Column"]
        assert len(col_nodes) == 2
        has_col_edges = [e for e in graph.edges if e.label == "HAS_COLUMN"]
        assert len(has_col_edges) == 2

    def test_error_table_skipped(self):
        builder = GraphBuilder(database="db", schema="s")
        table = _make_table("bad", error="timeout")
        builder.add_table(table)
        graph = builder.build()

        # Only db + schema nodes, no table nodes
        assert len(graph.nodes) == 2
        assert all(n.label != "Table" for n in graph.nodes)

    def test_functional_dep_edges(self):
        builder = GraphBuilder(database="db", schema="s")
        cols = [_make_col("zip"), _make_col("city", "string", engine_type="VARCHAR")]
        table = _make_table("addr", columns=cols,
                           functional_dependencies=[{"from": "zip", "to": "city"}])
        builder.add_table(table)
        graph = builder.build()

        fd_edges = [e for e in graph.edges if e.label == "FUNCTIONAL_DEP"]
        assert len(fd_edges) == 1
        assert "zip" in fd_edges[0].source
        assert "city" in fd_edges[0].target

    def test_correlation_edges_threshold(self):
        builder = GraphBuilder(database="db", schema="s")
        cols = [_make_col("a"), _make_col("b"), _make_col("c")]
        table = _make_table("t", columns=cols, correlations=[
            {"column_a": "a", "column_b": "b", "value": 0.9},   # above threshold
            {"column_a": "a", "column_b": "c", "value": 0.3},   # below threshold
        ])
        builder.add_table(table)
        graph = builder.build()

        sim_edges = [e for e in graph.edges if e.label == "SIMILAR_TO"]
        assert len(sim_edges) == 1
        assert sim_edges[0].properties["correlation"] == 0.9

    def test_declared_fk_edge(self):
        builder = GraphBuilder(database="db", schema="s")
        cols1 = [_make_col("user_id")]
        cols2 = [_make_col("id")]
        builder.add_table(_make_table("orders", columns=cols1))
        builder.add_table(_make_table("users", columns=cols2))
        builder.add_relationships([{
            "source_table": "orders", "source_columns": ["user_id"],
            "target_table": "users", "target_columns": ["id"],
            "relationship_type": "declared_fk", "confidence": 1.0,
        }])
        graph = builder.build()

        fk_edges = [e for e in graph.edges if e.label == "FK_DECLARED"]
        assert len(fk_edges) == 1
        assert "user_id" in fk_edges[0].source
        assert "id" in fk_edges[0].target

    def test_semantic_fk_edge(self):
        builder = GraphBuilder(database="db", schema="s")
        builder.add_table(_make_table("sales", columns=[_make_col("customer_sk")]))
        builder.add_table(_make_table("customer", columns=[_make_col("customer_sk")]))
        builder.add_relationships([{
            "source_table": "sales", "source_columns": ["customer_sk"],
            "target_table": "customer", "target_columns": ["customer_sk"],
            "relationship_type": "semantic_fk", "confidence": 0.8,
        }])
        graph = builder.build()

        sem_edges = [e for e in graph.edges if e.label == "FK_SEMANTIC"]
        assert len(sem_edges) == 1

    def test_inferred_fk_edge(self):
        builder = GraphBuilder(database="db", schema="s")
        builder.add_table(_make_table("a", columns=[_make_col("col")]))
        builder.add_table(_make_table("b", columns=[_make_col("col")]))
        builder.add_relationships([{
            "source_table": "a", "source_columns": ["col"],
            "target_table": "b", "target_columns": ["col"],
            "relationship_type": "inferred", "confidence": 0.5,
        }])
        graph = builder.build()

        inf_edges = [e for e in graph.edges if e.label == "FK_INFERRED"]
        assert len(inf_edges) == 1

    def test_composite_fk_edge(self):
        builder = GraphBuilder(database="db", schema="s")
        builder.add_table(_make_table("fact", columns=[_make_col("a"), _make_col("b")]))
        builder.add_table(_make_table("dim", columns=[_make_col("a"), _make_col("b")]))
        builder.add_relationships([{
            "source_table": "fact", "source_columns": ["a", "b"],
            "target_table": "dim", "target_columns": ["a", "b"],
            "relationship_type": "inferred_composite", "confidence": 0.6,
        }])
        graph = builder.build()

        comp_edges = [e for e in graph.edges if e.label == "FK_COMPOSITE"]
        assert len(comp_edges) == 1
        # Composite edges connect tables, not columns
        assert "fact" in comp_edges[0].source
        assert "dim" in comp_edges[0].target
        assert comp_edges[0].properties["columns"] == ["a", "b"]

    def test_node_deduplication(self):
        builder = GraphBuilder(database="db", schema="s")
        col = _make_col("id")
        builder.add_table(_make_table("t", columns=[col]))
        # Adding same table again should not create duplicate nodes
        builder.add_table(_make_table("t", columns=[col]))
        graph = builder.build()

        table_nodes = [n for n in graph.nodes if n.label == "Table"]
        assert len(table_nodes) == 1

    def test_urn_format(self):
        urn = _make_urn("MyDB", "Public", "Users", "Email Address")
        assert urn == "urn:profiler:mydb:public:users:email_address"

    def test_urn_none_parts(self):
        urn = _make_urn(None, "public")
        assert urn == "urn:profiler:_:public"


# ---------------------------------------------------------------------------
# JSON-LD Serializer
# ---------------------------------------------------------------------------

class TestJSONLDSerializer:
    def test_roundtrip(self, tmp_path):
        path = str(tmp_path / "graph.jsonld")
        s = JSONLDSerializer(path)
        s.write_header({"database": "testdb", "schema": "public", "engine": "duckdb"})
        s.flush(_make_table("t1", columns=[_make_col("id")]))
        s.close()

        data = json.loads(Path(path).read_text())
        assert "@context" in data
        assert "@graph" in data
        assert len(data["@graph"]) > 0

    def test_context_has_required_keys(self, tmp_path):
        path = str(tmp_path / "graph.jsonld")
        s = JSONLDSerializer(path)
        s.write_header({"database": "db", "schema": "s", "engine": "duckdb"})
        s.close()

        data = json.loads(Path(path).read_text())
        ctx = data["@context"]
        assert "schema" in ctx
        assert "profiler" in ctx
        for label in ["Database", "Schema", "Table", "Column"]:
            assert label in ctx

    def test_graph_size(self, tmp_path):
        path = str(tmp_path / "graph.jsonld")
        s = JSONLDSerializer(path)
        s.write_header({"database": "db", "schema": "s"})
        s.flush(_make_table("t", columns=[_make_col("a"), _make_col("b")]))
        s.close()

        data = json.loads(Path(path).read_text())
        graph = data["@graph"]
        # 2 nodes (db+schema) + 1 table + 2 columns = 5 nodes
        # + 1 HAS_SCHEMA + 1 HAS_TABLE + 2 HAS_COLUMN = 4 edges
        # = 9 total items
        assert len(graph) == 9

    def test_edge_references_valid_node_ids(self, tmp_path):
        path = str(tmp_path / "graph.jsonld")
        s = JSONLDSerializer(path)
        s.write_header({"database": "db", "schema": "s"})
        s.flush(_make_table("t", columns=[_make_col("id")]))
        s.close()

        data = json.loads(Path(path).read_text())
        graph = data["@graph"]
        node_ids = {item["@id"] for item in graph if "@type" in item and "source" not in item}
        edges = [item for item in graph if "source" in item]
        for edge in edges:
            assert edge["source"] in node_ids, f"Edge source {edge['source']} not a known node"
            assert edge["target"] in node_ids, f"Edge target {edge['target']} not a known node"

    def test_relationships_in_trailer(self, tmp_path):
        path = str(tmp_path / "graph.jsonld")
        s = JSONLDSerializer(path)
        s.write_header({"database": "db", "schema": "s"})
        s.flush(_make_table("orders", columns=[_make_col("user_id")]))
        s.flush(_make_table("users", columns=[_make_col("id")]))
        s.write_trailer({
            "_relationships": True,
            "count": 1,
            "relationships": [{
                "source_table": "orders", "source_columns": ["user_id"],
                "target_table": "users", "target_columns": ["id"],
                "relationship_type": "declared_fk", "confidence": 1.0,
            }],
        })
        s.close()

        data = json.loads(Path(path).read_text())
        fk_items = [i for i in data["@graph"] if i.get("@type") == "FK_DECLARED"]
        assert len(fk_items) == 1


# ---------------------------------------------------------------------------
# GraphML Serializer
# ---------------------------------------------------------------------------

class TestGraphMLSerializer:
    def test_roundtrip_valid_xml(self, tmp_path):
        path = str(tmp_path / "graph.graphml")
        s = GraphMLSerializer(path)
        s.write_header({"database": "db", "schema": "s", "engine": "duckdb"})
        s.flush(_make_table("t", columns=[_make_col("id")]))
        s.close()

        # Should parse without error
        tree = ET.parse(path)
        root = tree.getroot()
        assert "graphml" in root.tag

    def test_key_declarations(self, tmp_path):
        path = str(tmp_path / "graph.graphml")
        s = GraphMLSerializer(path)
        s.write_header({"database": "db", "schema": "s"})
        s.flush(_make_table("t", columns=[_make_col("id")]))
        s.close()

        tree = ET.parse(path)
        root = tree.getroot()
        ns = {"g": "http://graphml.graphdrawing.org/xmlns"}
        keys = root.findall("g:key", ns)
        assert len(keys) > 0
        # Should have node keys and edge keys
        node_keys = [k for k in keys if k.get("for") == "node"]
        edge_keys = [k for k in keys if k.get("for") == "edge"]
        assert len(node_keys) > 0
        assert len(edge_keys) > 0

    def test_node_and_edge_counts(self, tmp_path):
        path = str(tmp_path / "graph.graphml")
        s = GraphMLSerializer(path)
        s.write_header({"database": "db", "schema": "s"})
        s.flush(_make_table("t", columns=[_make_col("a"), _make_col("b")]))
        s.close()

        tree = ET.parse(path)
        ns = {"g": "http://graphml.graphdrawing.org/xmlns"}
        graph_el = tree.getroot().find("g:graph", ns)
        nodes = graph_el.findall("g:node", ns)
        edges = graph_el.findall("g:edge", ns)
        # 2 (db+schema) + 1 table + 2 columns = 5 nodes
        assert len(nodes) == 5
        # 1 HAS_SCHEMA + 1 HAS_TABLE + 2 HAS_COLUMN = 4 edges
        assert len(edges) == 4

    def test_xml_special_char_escaping(self, tmp_path):
        path = str(tmp_path / "graph.graphml")
        s = GraphMLSerializer(path)
        s.write_header({"database": "db<>", "schema": "s&s"})
        s.flush(_make_table("table & <stuff>", columns=[_make_col("col\"name")]))
        s.close()

        # Should still parse as valid XML
        tree = ET.parse(path)
        assert tree.getroot() is not None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class TestSerializerFactory:
    def test_create_jsonld(self, tmp_path):
        s = create_serializer("jsonld", str(tmp_path / "out.jsonld"))
        assert isinstance(s, JSONLDSerializer)

    def test_create_graphml(self, tmp_path):
        s = create_serializer("graphml", str(tmp_path / "out.graphml"))
        assert isinstance(s, GraphMLSerializer)
