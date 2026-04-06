"""Database engine adapters."""

from data_profiler.adapters.base import BaseAdapter
from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.adapters.sqlite import SQLiteAdapter

__all__ = ["BaseAdapter", "DuckDBAdapter", "SQLiteAdapter"]
