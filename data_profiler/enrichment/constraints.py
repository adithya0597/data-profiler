"""Constraint discovery: PK, FK, UNIQUE, CHECK via SQLAlchemy Inspector."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import inspect, Engine

logger = logging.getLogger(__name__)


@dataclass
class TableConstraints:
    """Discovered constraints for a table."""
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: list[dict[str, Any]] = field(default_factory=list)
    unique_constraints: list[dict[str, Any]] = field(default_factory=list)
    check_constraints: list[dict[str, Any]] = field(default_factory=list)


def discover_constraints(
    engine: Engine,
    table_name: str,
    schema: str | None = None,
) -> TableConstraints:
    """Discover PK, FK, UNIQUE, and CHECK constraints for a table.

    Uses SQLAlchemy Inspector methods. Gracefully handles engines that
    don't support certain constraint types (returns empty lists).
    """
    insp = inspect(engine)
    result = TableConstraints()

    # Primary key
    try:
        pk_info = insp.get_pk_constraint(table_name, schema=schema)
        result.primary_key = pk_info.get("constrained_columns", [])
    except (NotImplementedError, Exception) as e:
        logger.debug("PK discovery not supported for %s: %s", table_name, e)

    # Foreign keys
    try:
        fks = insp.get_foreign_keys(table_name, schema=schema)
        for fk in fks:
            result.foreign_keys.append({
                "constrained_columns": fk.get("constrained_columns", []),
                "referred_table": fk.get("referred_table", ""),
                "referred_columns": fk.get("referred_columns", []),
                "referred_schema": fk.get("referred_schema"),
                "name": fk.get("name"),
            })
    except (NotImplementedError, Exception) as e:
        logger.debug("FK discovery not supported for %s: %s", table_name, e)

    # Unique constraints
    try:
        uniques = insp.get_unique_constraints(table_name, schema=schema)
        for uc in uniques:
            result.unique_constraints.append({
                "name": uc.get("name"),
                "columns": uc.get("column_names", []),
            })
    except (NotImplementedError, Exception) as e:
        logger.debug("UNIQUE discovery not supported for %s: %s", table_name, e)

    # Check constraints
    try:
        checks = insp.get_check_constraints(table_name, schema=schema)
        for cc in checks:
            result.check_constraints.append({
                "name": cc.get("name"),
                "sqltext": str(cc.get("sqltext", "")),
            })
    except (NotImplementedError, Exception) as e:
        logger.debug("CHECK discovery not supported for %s: %s", table_name, e)

    return result
