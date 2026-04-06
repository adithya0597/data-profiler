"""Unit tests for portable type mapping."""

import pytest
from data_profiler.schema.portable_types import map_type


class TestMapType:
    """Test canonical type mapping for all engine type families."""

    @pytest.mark.parametrize("engine_type,expected", [
        # String types
        ("VARCHAR", "string"),
        ("VARCHAR(255)", "string"),
        ("TEXT", "string"),
        ("STRING", "string"),
        ("CHAR(10)", "string"),
        ("NVARCHAR(100)", "string"),

        # Integer types
        ("INT", "integer"),
        ("INTEGER", "integer"),
        ("BIGINT", "integer"),
        ("SMALLINT", "integer"),
        ("TINYINT", "integer"),
        ("HUGEINT", "integer"),
        ("NUMBER(38,0)", "integer"),  # Snowflake integer

        # Float types
        ("FLOAT", "float"),
        ("DOUBLE", "float"),
        ("DECIMAL", "float"),
        ("DECIMAL(18,2)", "float"),
        ("NUMERIC(10,4)", "float"),
        ("REAL", "float"),

        # Boolean
        ("BOOLEAN", "boolean"),
        ("BOOL", "boolean"),

        # Datetime
        ("TIMESTAMP", "datetime"),
        ("TIMESTAMP_LTZ", "datetime"),
        ("TIMESTAMP_NTZ", "datetime"),
        ("DATETIME", "datetime"),

        # Date
        ("DATE", "date"),

        # Binary
        ("BLOB", "binary"),
        ("BINARY", "binary"),
        ("VARBINARY", "binary"),
        ("BYTES", "binary"),

        # Unknown
        ("GEOMETRY", "unknown"),
        ("ARRAY", "unknown"),
        ("VARIANT", "unknown"),
    ])
    def test_type_mapping(self, engine_type: str, expected: str):
        assert map_type(engine_type) == expected

    def test_case_insensitive(self):
        assert map_type("varchar") == "string"
        assert map_type("Bigint") == "integer"
        assert map_type("boolean") == "boolean"

    def test_whitespace(self):
        assert map_type("  VARCHAR  ") == "string"
        assert map_type("INTEGER ") == "integer"

    def test_number_with_scale_zero_is_integer(self):
        """NUMBER(p,0) maps to integer (Snowflake convention)."""
        assert map_type("NUMBER(38,0)") == "integer"
        assert map_type("NUMERIC(18,0)") == "integer"

    def test_number_with_nonzero_scale_is_float(self):
        """NUMBER(p,s) with s>0 maps to float."""
        assert map_type("NUMBER(38,2)") == "float"
        assert map_type("NUMERIC(18,4)") == "float"
