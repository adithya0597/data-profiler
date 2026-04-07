"""JSON-LD property graph serializer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TYPE_CHECKING

from data_profiler.persistence.graph_model import GraphBuilder

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable


_JSONLD_CONTEXT = {
    "schema": "http://schema.org/",
    "profiler": "urn:profiler:ontology:",
    "name": "schema:name",
    "Database": "profiler:Database",
    "Schema": "profiler:Schema",
    "Table": "profiler:Table",
    "Column": "profiler:Column",
    "HAS_SCHEMA": "profiler:hasSchema",
    "HAS_TABLE": "profiler:hasTable",
    "HAS_COLUMN": "profiler:hasColumn",
    "FK_DECLARED": "profiler:fkDeclared",
    "FK_SEMANTIC": "profiler:fkSemantic",
    "FK_INFERRED": "profiler:fkInferred",
    "FK_COMPOSITE": "profiler:fkComposite",
    "FUNCTIONAL_DEP": "profiler:functionalDep",
    "SIMILAR_TO": "profiler:similarTo",
}


class JSONLDSerializer:
    """Buffered JSON-LD: builds property graph incrementally, writes at close()."""

    def __init__(self, path: str):
        self.path = path
        self._builder: GraphBuilder | None = None

    def write_header(self, header: dict[str, Any]) -> None:
        self._builder = GraphBuilder(
            database=header.get("database"),
            schema=header.get("schema"),
            engine=header.get("engine"),
        )

    def flush(self, profile: "ProfiledTable") -> None:
        if self._builder is not None:
            self._builder.add_table(profile)

    def write_trailer(self, data: dict[str, Any]) -> None:
        if self._builder is not None and data.get("_relationships"):
            self._builder.add_relationships(data.get("relationships", []))

    def close(self) -> None:
        if self._builder is None:
            return
        graph = self._builder.build()

        graph_items: list[dict[str, Any]] = []
        for node in graph.nodes:
            item: dict[str, Any] = {
                "@id": node.id,
                "@type": node.label,
            }
            for k, v in node.properties.items():
                if v is not None:
                    item[k] = v
            graph_items.append(item)

        for edge in graph.edges:
            item = {
                "@id": edge.id,
                "@type": edge.label,
                "source": edge.source,
                "target": edge.target,
            }
            for k, v in edge.properties.items():
                if v is not None:
                    item[k] = v
            graph_items.append(item)

        document = {
            "@context": _JSONLD_CONTEXT,
            "@graph": graph_items,
        }

        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        Path(self.path).write_text(json.dumps(document, indent=2, default=str))
