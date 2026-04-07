"""Property graph model for knowledge graph export (JSON-LD, GraphML)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

from data_profiler.persistence.serializers import _clean_profile

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable


def _make_urn(*parts: str | None) -> str:
    """Build a stable URN from name parts, lowercased and whitespace-stripped."""
    clean = [p.lower().replace(" ", "_") if p else "_" for p in parts]
    return "urn:profiler:" + ":".join(clean)


@dataclass
class GraphNode:
    """A node in the property graph."""
    id: str
    label: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphEdge:
    """A directed edge in the property graph."""
    id: str
    source: str
    target: str
    label: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class PropertyGraph:
    """A collection of nodes and edges."""
    nodes: list[GraphNode] = field(default_factory=list)
    edges: list[GraphEdge] = field(default_factory=list)


_REL_TYPE_TO_EDGE_LABEL = {
    "declared_fk": "FK_DECLARED",
    "semantic_fk": "FK_SEMANTIC",
    "inferred": "FK_INFERRED",
    "inferred_composite": "FK_COMPOSITE",
}


class GraphBuilder:
    """Incrementally builds a property graph from profiled tables and relationships.

    Usage: call add_table() per table, then add_relationships() once,
    then build() to get the final PropertyGraph.
    """

    def __init__(self, database: str | None, schema: str | None, engine: str | None = None):
        self._database = database or "default"
        self._schema = schema or "default"
        self._engine = engine or ""
        self._nodes: dict[str, GraphNode] = {}
        self._edges: list[GraphEdge] = []
        self._edge_count = 0

        # Database node
        db_urn = _make_urn(self._database)
        self._nodes[db_urn] = GraphNode(
            id=db_urn, label="Database",
            properties={"name": self._database, "engine": self._engine},
        )

        # Schema node
        schema_urn = _make_urn(self._database, self._schema)
        self._nodes[schema_urn] = GraphNode(
            id=schema_urn, label="Schema",
            properties={"name": self._schema, "database": self._database},
        )

        self._add_edge(db_urn, schema_urn, "HAS_SCHEMA")

    def _add_edge(self, source: str, target: str, label: str, **props: Any) -> None:
        self._edge_count += 1
        self._edges.append(GraphEdge(
            id=f"e{self._edge_count}",
            source=source, target=target, label=label,
            properties=props,
        ))

    def add_table(self, profile: "ProfiledTable") -> None:
        """Add a profiled table as Table + Column nodes with HAS_TABLE/HAS_COLUMN edges."""
        if profile.error:
            return

        data = _clean_profile(profile)
        schema_urn = _make_urn(self._database, self._schema)
        table_urn = _make_urn(self._database, self._schema, profile.name)

        self._nodes[table_urn] = GraphNode(
            id=table_urn, label="Table",
            properties={
                "name": profile.name,
                "schema": self._schema,
                "row_count": profile.total_row_count,
                "quality_score": profile.quality_score,
                "profiled_at": profile.profiled_at,
            },
        )
        self._add_edge(schema_urn, table_urn, "HAS_TABLE")

        for col in data.get("columns", []):
            col_urn = _make_urn(self._database, self._schema, profile.name, col["name"])
            self._nodes[col_urn] = GraphNode(
                id=col_urn, label="Column",
                properties={
                    "name": col["name"],
                    "table": profile.name,
                    "canonical_type": col["canonical_type"],
                    "null_rate": col.get("null_rate", 0.0),
                    "approx_distinct": col.get("approx_distinct", 0),
                    "mean": col.get("mean"),
                    "min": col.get("min"),
                    "max": col.get("max"),
                    "patterns": col.get("patterns", []),
                    "anomalies": col.get("anomalies", []),
                },
            )
            self._add_edge(table_urn, col_urn, "HAS_COLUMN")

        # Functional dependency edges
        if profile.functional_dependencies:
            for fd in profile.functional_dependencies:
                from_col = fd.get("from") or fd.get("determinant")
                to_col = fd.get("to") or fd.get("dependent")
                if from_col and to_col:
                    from_urn = _make_urn(self._database, self._schema, profile.name, from_col)
                    to_urn = _make_urn(self._database, self._schema, profile.name, to_col)
                    if from_urn in self._nodes and to_urn in self._nodes:
                        self._add_edge(from_urn, to_urn, "FUNCTIONAL_DEP")

        # Correlation edges (only strong correlations > 0.7)
        if profile.correlations:
            for corr in profile.correlations:
                col_a = corr.get("column_a") or corr.get("col1")
                col_b = corr.get("column_b") or corr.get("col2")
                value = corr.get("value") or corr.get("correlation", 0)
                if col_a and col_b and abs(value) > 0.7:
                    a_urn = _make_urn(self._database, self._schema, profile.name, col_a)
                    b_urn = _make_urn(self._database, self._schema, profile.name, col_b)
                    if a_urn in self._nodes and b_urn in self._nodes:
                        self._add_edge(a_urn, b_urn, "SIMILAR_TO", correlation=value)

    def add_relationships(self, relationships: list[dict[str, Any]]) -> None:
        """Add FK/inferred relationship edges."""
        for rel in relationships:
            rel_type = rel.get("relationship_type", "inferred")
            edge_label = _REL_TYPE_TO_EDGE_LABEL.get(rel_type, "FK_INFERRED")

            src_table = rel.get("source_table", "")
            tgt_table = rel.get("target_table", "")
            src_cols = rel.get("source_columns", [])
            tgt_cols = rel.get("target_columns", [])
            confidence = rel.get("confidence", 1.0)

            if rel_type == "inferred_composite":
                # Composite: Table-to-Table edge with columns as property
                src_urn = _make_urn(self._database, self._schema, src_table)
                tgt_urn = _make_urn(self._database, self._schema, tgt_table)
                self._add_edge(src_urn, tgt_urn, edge_label,
                               columns=src_cols, confidence=confidence)
            else:
                # Single-column: Column-to-Column edges
                for sc, tc in zip(src_cols, tgt_cols):
                    src_urn = _make_urn(self._database, self._schema, src_table, sc)
                    tgt_urn = _make_urn(self._database, self._schema, tgt_table, tc)
                    self._add_edge(src_urn, tgt_urn, edge_label, confidence=confidence)

    def build(self) -> PropertyGraph:
        """Return the assembled property graph."""
        return PropertyGraph(
            nodes=list(self._nodes.values()),
            edges=list(self._edges),
        )
