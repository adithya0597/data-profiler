"""GraphML property graph serializer."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, TYPE_CHECKING

from data_profiler.persistence.graph_model import GraphBuilder

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable


class GraphMLSerializer:
    """Buffered GraphML: builds property graph incrementally, writes XML at close()."""

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

        root = ET.Element("graphml", attrib={
            "xmlns": "http://graphml.graphdrawing.org/xmlns",
        })

        # Collect all property keys from nodes and edges
        node_keys: dict[str, str] = {"label": "string"}
        edge_keys: dict[str, str] = {"label": "string"}

        for node in graph.nodes:
            for k, v in node.properties.items():
                node_keys[k] = _infer_type(v)

        for edge in graph.edges:
            for k, v in edge.properties.items():
                edge_keys[k] = _infer_type(v)

        # Declare <key> elements
        for key_name, key_type in sorted(node_keys.items()):
            ET.SubElement(root, "key", attrib={
                "id": f"n_{key_name}", "for": "node",
                "attr.name": key_name, "attr.type": key_type,
            })
        for key_name, key_type in sorted(edge_keys.items()):
            ET.SubElement(root, "key", attrib={
                "id": f"e_{key_name}", "for": "edge",
                "attr.name": key_name, "attr.type": key_type,
            })

        # Graph element
        graph_el = ET.SubElement(root, "graph", id="G", edgedefault="directed")

        # Nodes
        for node in graph.nodes:
            node_el = ET.SubElement(graph_el, "node", id=node.id)
            _add_data(node_el, "n_label", node.label)
            for k, v in node.properties.items():
                if v is not None:
                    _add_data(node_el, f"n_{k}", v)

        # Edges
        for edge in graph.edges:
            edge_el = ET.SubElement(graph_el, "edge", attrib={
                "id": edge.id, "source": edge.source, "target": edge.target,
            })
            _add_data(edge_el, "e_label", edge.label)
            for k, v in edge.properties.items():
                if v is not None:
                    _add_data(edge_el, f"e_{k}", v)

        tree = ET.ElementTree(root)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        ET.indent(tree, space="  ")
        tree.write(self.path, xml_declaration=True, encoding="unicode")


def _infer_type(value: Any) -> str:
    """Map Python types to GraphML attr.type values."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    return "string"


def _add_data(parent: ET.Element, key: str, value: Any) -> None:
    """Add a <data> child element with the given key and value."""
    data_el = ET.SubElement(parent, "data", key=key)
    if isinstance(value, (list, dict)):
        data_el.text = json.dumps(value, default=str)
    else:
        data_el.text = str(value)
