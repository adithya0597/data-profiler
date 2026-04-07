"""Profiler configuration model."""

from __future__ import annotations

from pydantic import BaseModel, Field


class AnomalyThresholds(BaseModel):
    """Tunable thresholds for anomaly detection rules."""

    high_null_rate: float = Field(default=0.5, ge=0.0, le=1.0, description="Null rate above this triggers high_null_rate")
    near_constant_rate: float = Field(default=0.95, ge=0.5, le=1.0, description="Top-value frequency above this triggers near_constant")
    high_cardinality_distinct: int = Field(default=10_000, ge=1, description="Distinct count above this triggers high_cardinality")
    all_unique_ratio: float = Field(default=0.99, ge=0.9, le=1.0, description="Distinct/total ratio above this triggers all_unique")
    all_unique_hll_guard: float = Field(default=1.05, ge=1.0, le=1.2, description="HLL overcount guard: distinct must be <= rows * this")


class ProfilerConfig(BaseModel):
    """Configuration for a profiling run."""

    engine: str = Field(description="Database engine: snowflake, databricks, duckdb, sqlite")
    dsn: str = Field(description="SQLAlchemy-compatible connection string")
    database: str | None = Field(default=None, description="Database/catalog to profile")
    schema_name: str | None = Field(default=None, description="Schema filter (omit for all schemas)")
    sample_size: int = Field(default=10000, ge=0, description="Rows sampled per table. 0 = full scan")
    concurrency: int = Field(default=4, ge=1, description="Parallel table workers")
    stats_depth: str = Field(default="full", pattern="^(full|fast)$", description="full = all stats; fast = schema only")
    exact_distinct: bool = Field(default=False, description="Use COUNT(DISTINCT) instead of HLL")
    column_batch_size: int = Field(default=80, ge=1, description="Max columns per aggregate SELECT")
    output_format: str = Field(default="json", pattern="^(json|parquet|yaml|csv|html|jsonld|graphml)$")
    output: str | None = Field(default=None, description="Output file path")
    resume: str | None = Field(default=None, description="Run ID to resume")
    incremental: bool = Field(default=False, description="Only re-profile tables that changed since prior run")
    watermark_column: str | None = Field(default=None, description="Timestamp/sequence column for append-only delta detection")
    prior_run_id: str | None = Field(default=None, description="Run ID to compare against (auto-detected if omitted)")
    # Enterprise enrichment flags
    enable_patterns: bool = Field(default=True, description="Run regex pattern detection on string columns")
    discover_constraints: bool = Field(default=True, description="Discover PK/FK/UNIQUE/CHECK constraints via Inspector")
    discover_relationships: bool = Field(default=True, description="Map cross-table FK and inferred relationships")
    detect_duplicates: bool = Field(default=True, description="Count duplicate rows per table")
    duplicate_column_limit: int = Field(default=50, ge=1, description="Skip duplicate check for tables wider than this")
    enable_histogram: bool = Field(default=True, description="Compute histograms for numeric columns")
    histogram_bins: int = Field(default=10, ge=2, le=100, description="Number of histogram bins")
    enable_correlation: bool = Field(default=True, description="Compute Pearson/Cramér's V correlations")
    correlation_max_columns: int = Field(default=20, ge=2, description="Max numeric columns for correlation matrix")
    enable_benford: bool = Field(default=True, description="Run Benford's Law analysis on numeric columns")
    query_timeout: int = Field(default=300, ge=0, description="Statement timeout in seconds (0 = no limit)")
    anomaly_thresholds: AnomalyThresholds = Field(default_factory=AnomalyThresholds, description="Tunable anomaly detection thresholds")
