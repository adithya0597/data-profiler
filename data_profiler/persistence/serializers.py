"""Streaming serializers: NDJSON, YAML, Parquet, CSV."""

from __future__ import annotations

import csv
import io
import json
import threading
from dataclasses import asdict
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from data_profiler.workers.stats_worker import ProfiledTable


def _clean_profile(profile: ProfiledTable) -> dict[str, Any]:
    """Convert ProfiledTable to a JSON-serializable dict."""
    d = asdict(profile)
    # Convert any non-serializable types
    for col in d.get("columns", []):
        for key in ("min", "max"):
            val = col.get(key)
            if val is not None and not isinstance(val, (str, int, float, bool)):
                col[key] = str(val)
    return d


class NDJSONSerializer:
    """Streaming NDJSON: one JSON line per table, flushed immediately."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._file = open(path, "w")

    def write_header(self, header: dict[str, Any]) -> None:
        with self._lock:
            self._file.write(json.dumps(header) + "\n")
            self._file.flush()

    def flush(self, profile: ProfiledTable) -> None:
        data = _clean_profile(profile)
        line = json.dumps(data, default=str)
        with self._lock:
            self._file.write(line + "\n")
            self._file.flush()

    def write_trailer(self, data: dict[str, Any]) -> None:
        """Write a trailer record (e.g., relationships) at end of stream."""
        with self._lock:
            self._file.write(json.dumps(data, default=str) + "\n")
            self._file.flush()

    def close(self) -> None:
        self._file.close()


class YAMLSerializer:
    """Streaming YAML: each table as a separate YAML document."""

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._file = open(path, "w")

    def write_header(self, header: dict[str, Any]) -> None:
        with self._lock:
            self._file.write("---\n")
            self._write_dict(header)
            self._file.flush()

    def flush(self, profile: ProfiledTable) -> None:
        if profile.error is not None:
            return  # Exclude errored tables from YAML
        data = _clean_profile(profile)
        with self._lock:
            self._file.write("---\n")
            self._write_dict(data)
            self._file.flush()

    def write_trailer(self, data: dict[str, Any]) -> None:
        """Write a trailer record as a YAML document."""
        with self._lock:
            self._file.write("---\n")
            self._write_dict(data)
            self._file.flush()

    def _write_dict(self, d: dict) -> None:
        """Simple YAML-like output without requiring PyYAML."""
        for key, value in d.items():
            if isinstance(value, list):
                self._file.write(f"{key}:\n")
                for item in value:
                    if isinstance(item, dict):
                        self._file.write("  -\n")
                        for k, v in item.items():
                            self._file.write(f"    {k}: {_yaml_value(v)}\n")
                    else:
                        self._file.write(f"  - {_yaml_value(item)}\n")
            elif isinstance(value, dict):
                self._file.write(f"{key}:\n")
                for k, v in value.items():
                    self._file.write(f"  {k}: {_yaml_value(v)}\n")
            else:
                self._file.write(f"{key}: {_yaml_value(value)}\n")

    def close(self) -> None:
        self._file.close()


class ParquetSerializer:
    """Buffered Parquet: collects all results, writes once at end."""

    def __init__(self, path: str):
        self.path = path
        self._buffer: list[dict[str, Any]] = []
        self._header: dict[str, Any] = {}
        self._trailers: list[dict[str, Any]] = []

    def write_header(self, header: dict[str, Any]) -> None:
        self._header = header

    def write_trailer(self, data: dict[str, Any]) -> None:
        """Store trailer data. Parquet can't mix schemas, so trailers are stored as metadata."""
        self._trailers.append(data)

    def flush(self, profile: ProfiledTable) -> None:
        if profile.error is not None:
            return  # Exclude errored tables from Parquet
        data = _clean_profile(profile)
        # Flatten columns into rows for Parquet
        for col in data.pop("columns", []):
            row = {
                "table_name": data["name"],
                "table_comment": data.get("comment"),
                "total_row_count": data["total_row_count"],
                "sampled_row_count": data["sampled_row_count"],
                "sample_size": data["sample_size"],
                "full_scan": data.get("full_scan", False),
                "profiled_at": data["profiled_at"],
                "duration_seconds": data.get("duration_seconds", 0),
                **col,
            }
            # Ensure min/max are strings for Parquet (mixed types not allowed)
            for key in ("min", "max"):
                if row.get(key) is not None:
                    row[key] = str(row[key])
            self._buffer.append(row)

    def close(self) -> None:
        if not self._buffer:
            return
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            raise ImportError("pyarrow is required for Parquet output: pip install data-profiler[parquet]")

        table = pa.Table.from_pylist(self._buffer)
        pq.write_table(table, self.path)


class CSVSerializer:
    """Flat CSV: one row per column, written streaming with header on first flush."""

    # Columns emitted in output — excludes nested fields (histogram, top_values, etc.)
    _SKIP_KEYS = {"histogram", "benford_digits", "top_values", "patterns", "pattern_scores",
                  "anomalies"}

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._file = open(path, "w", newline="")
        self._writer: csv.DictWriter | None = None
        self._header_written = False

    def write_header(self, header: dict[str, Any]) -> None:
        pass  # CSV header is written on first flush to capture column names

    def write_trailer(self, data: dict[str, Any]) -> None:
        pass  # Relationships not representable in flat CSV

    def flush(self, profile: "ProfiledTable") -> None:
        if profile.error is not None:
            return
        data = _clean_profile(profile)
        table_meta = {k: v for k, v in data.items() if k != "columns"}
        rows = []
        for col in data.get("columns", []):
            row = {f"table_{k}": v for k, v in table_meta.items()}
            row.update({k: v for k, v in col.items() if k not in self._SKIP_KEYS})
            rows.append(row)
        if not rows:
            return
        with self._lock:
            if not self._header_written:
                fieldnames = list(rows[0].keys())
                self._writer = csv.DictWriter(self._file, fieldnames=fieldnames,
                                              extrasaction="ignore")
                self._writer.writeheader()
                self._header_written = True
            for row in rows:
                self._writer.writerow(row)
            self._file.flush()

    def close(self) -> None:
        self._file.close()


def _yaml_value(v: Any) -> str:
    """Format a value for YAML output."""
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, str):
        if any(c in v for c in (":", "#", "'", '"', "\n", "{", "}", "[", "]")):
            return f'"{v}"'
        return v
    if isinstance(v, list):
        return json.dumps(v)
    return str(v)


def create_serializer(
    output_format: str, path: str,
) -> NDJSONSerializer | YAMLSerializer | ParquetSerializer | CSVSerializer:
    """Factory: create the appropriate serializer based on output format."""
    if output_format == "yaml":
        return YAMLSerializer(path)
    if output_format == "parquet":
        return ParquetSerializer(path)
    if output_format == "csv":
        return CSVSerializer(path)
    if output_format == "jsonld":
        from data_profiler.persistence.jsonld_serializer import JSONLDSerializer
        return JSONLDSerializer(path)
    if output_format == "graphml":
        from data_profiler.persistence.graphml_serializer import GraphMLSerializer
        return GraphMLSerializer(path)
    return NDJSONSerializer(path)
