"""Orchestrator: discovers tables, profiles in parallel, writes output."""

from __future__ import annotations

import logging
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

from data_profiler.adapters.base import BaseAdapter
from data_profiler.adapters.duckdb import DuckDBAdapter
from data_profiler.adapters.sqlite import SQLiteAdapter
from data_profiler.config import ProfilerConfig
from data_profiler.persistence.checkpoint import CheckpointDB
from data_profiler.persistence.serializers import create_serializer
from data_profiler.workers.schema_worker import discover_schema, discover_tables
from data_profiler.enrichment.constraints import discover_constraints
from data_profiler.enrichment.constraint_suggester import suggest_constraints
from data_profiler.workers.relationship_worker import discover_relationships, relationships_to_dict
from data_profiler.workers.stats_worker import ProfiledTable, profile_table

logger = logging.getLogger(__name__)


def _create_adapter(config: ProfilerConfig) -> BaseAdapter:
    """Create the appropriate adapter based on engine config."""
    engine_map: dict[str, type[BaseAdapter]] = {
        "duckdb": DuckDBAdapter,
        "sqlite": SQLiteAdapter,
    }

    # Lazy imports for optional engines
    if config.engine == "snowflake":
        from data_profiler.adapters.snowflake import SnowflakeAdapter
        engine_map["snowflake"] = SnowflakeAdapter
    elif config.engine == "databricks":
        from data_profiler.adapters.databricks import DatabricksAdapter
        engine_map["databricks"] = DatabricksAdapter

    adapter_cls = engine_map.get(config.engine)
    if adapter_cls is None:
        raise ValueError(f"Unsupported engine: {config.engine}. Supported: {list(engine_map.keys())}")
    return adapter_cls(config.dsn)


def run_profiler(
    config: ProfilerConfig,
    progress_callback: Any | None = None,
) -> tuple[str, list[ProfiledTable]]:
    """Run the full profiling pipeline. Returns (run_id, results).

    progress_callback: optional callable(table_name, status, current, total)
    """
    # Setup
    adapter = _create_adapter(config)
    engine = adapter.get_engine()
    adapter.set_session_params(engine, config)
    run_id = config.resume or str(uuid.uuid4())[:8]

    # Auto-cap concurrency for DuckDB in-process
    concurrency = config.concurrency
    if adapter.engine_name == "duckdb" and concurrency > 1:
        logger.warning("DuckDB in-process mode: capping concurrency to 1")
        concurrency = 1

    # Checkpoint
    checkpoint = CheckpointDB()
    completed_tables = checkpoint.get_completed_tables(run_id) if config.resume else set()

    # Profile store — always active so every run stores snapshots for future incremental use
    profile_store = checkpoint.get_profile_store()
    prior_profiles: dict[str, ProfiledTable] = {}
    prior_metadata: dict[str, dict] = {}
    if config.incremental:
        prior_rid = config.prior_run_id or profile_store.get_latest_run_id(
            database=config.database, schema_name=config.schema_name,
        )
        if prior_rid:
            logger.info("Incremental mode: comparing against run %s", prior_rid)
            prior_profiles = profile_store.load_all_profiles(prior_rid)
            for tname in prior_profiles:
                meta = profile_store.get_prior_metadata(prior_rid, tname)
                if meta:
                    prior_metadata[tname] = meta
        else:
            logger.info("Incremental mode: no prior run found, full profiling")

    # Discover tables
    tables = discover_tables(engine, schema=config.schema_name)
    if not tables:
        from sqlalchemy.engine.url import make_url
        logger.warning("No tables found in %s", make_url(config.dsn).render_as_string(hide_password=True))
        return run_id, []

    # Filter already-completed tables on resume
    tables_to_profile = [t for t in tables if t not in completed_tables]
    total = len(tables)
    skipped = len(completed_tables)

    if skipped:
        logger.info("Resuming run %s: %d/%d tables already done", run_id, skipped, total)

    # Output setup
    output_ext = {"json": ".ndjson", "yaml": ".yaml", "parquet": ".parquet", "html": ".html",
                   "jsonld": ".jsonld", "graphml": ".graphml"}
    output_path = config.output or f"profiles/{run_id}{output_ext.get(config.output_format, '.ndjson')}"

    serializer = create_serializer(config.output_format, output_path)

    # Write run header
    header = {
        "_header": True,
        "run_id": run_id,
        "engine": config.engine,
        "database": config.database,
        "schema": config.schema_name,
        "profiled_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "sample_size": config.sample_size,
            "concurrency": concurrency,
            "stats_depth": config.stats_depth,
            "exact_distinct": config.exact_distinct,
            "column_batch_size": config.column_batch_size,
        },
    }
    serializer.write_header(header)

    # Profile tables
    results: list[ProfiledTable] = []
    error_count = 0
    done_count = skipped

    def _profile_one(table_name: str) -> ProfiledTable | None:
        # Incremental delta check
        if config.incremental and table_name in prior_metadata:
            from data_profiler.workers.delta_worker import check_delta, compute_column_hash
            tbl_schema = discover_schema(engine, table_name, config.schema_name)
            delta_result = check_delta(
                engine=engine,
                table_name=table_name,
                schema=config.schema_name,
                prior_metadata=prior_metadata.get(table_name),
                prior_profile=prior_profiles.get(table_name),
                watermark_column=config.watermark_column,
                current_columns=tbl_schema.columns,
                quote_fn=adapter.quote_identifier,
            )
            if not delta_result.needs_profiling:
                logger.info("Skipping unchanged table: %s", table_name)
                checkpoint.mark_skipped(run_id, table_name)
                return delta_result.prior_profile

            logger.info("Re-profiling %s (reason: %s)", table_name, delta_result.reason)

        checkpoint.mark_started(run_id, table_name)
        tbl_schema = discover_schema(engine, table_name, config.schema_name)
        result = profile_table(adapter, tbl_schema, config)
        # Constraint discovery (post-profiling enrichment)
        if config.discover_constraints and adapter.supports_constraints():
            try:
                result.constraints = discover_constraints(engine, table_name, config.schema_name)
            except Exception as e:
                logger.warning("Constraint discovery failed for %s: %s", table_name, e)
        return result

    def _handle_result(table_name: str, result: ProfiledTable) -> None:
        nonlocal error_count, done_count
        if result.error:
            checkpoint.mark_error(run_id, table_name, result.error)
            error_count += 1
        else:
            checkpoint.mark_done(run_id, table_name)

        # Store profile snapshot for future incremental runs
        if profile_store and not result.error:
            try:
                from data_profiler.workers.delta_worker import compute_column_hash
                from data_profiler.workers.schema_worker import ColumnSchema
                col_schemas = [
                    ColumnSchema(name=c.name, engine_type=c.engine_type, canonical_type=c.canonical_type,
                                 comment=c.comment, nullable=c.nullable)
                    for c in result.columns
                ]
                col_hash = compute_column_hash(col_schemas)
                wm_value = None
                if config.watermark_column:
                    wm_col = next((c for c in result.columns if c.name == config.watermark_column), None)
                    if wm_col and wm_col.max is not None:
                        wm_value = str(wm_col.max)
                profile_store.store_profile(
                    run_id=run_id, table_name=table_name, profile=result,
                    column_hash=col_hash, watermark_value=wm_value,
                    database=config.database, schema_name=config.schema_name,
                )
            except Exception as e:
                logger.warning("Failed to store profile snapshot for %s: %s", table_name, e)

        # Constraint suggestions (post-profiling enrichment)
        if not result.error:
            try:
                cs = suggest_constraints(result)
                if cs:
                    result.suggested_constraints = cs
            except Exception as e:
                logger.warning("Constraint suggestion failed for %s: %s", table_name, e)

        # Compute data quality score (after all enrichments)
        from data_profiler.workers.stats_worker import compute_quality_score
        result.quality_score = compute_quality_score(result)

        serializer.flush(result)
        results.append(result)
        done_count += 1

        if progress_callback:
            status = "error" if result.error else "done"
            progress_callback(table_name, status, done_count, total)

    if concurrency == 1:
        # Sequential: avoids thread-safety issues with in-process engines (DuckDB)
        for table_name in tables_to_profile:
            try:
                result = _profile_one(table_name)
            except Exception as e:
                logger.error("Unhandled error profiling %s: %s", table_name, e)
                checkpoint.mark_error(run_id, table_name, str(e))
                error_count += 1
                done_count += 1
                if progress_callback:
                    progress_callback(table_name, "error", done_count, total)
                continue
            _handle_result(table_name, result)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(_profile_one, t): t for t in tables_to_profile}
            for fut in as_completed(futures):
                table_name = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    logger.error("Unhandled error profiling %s: %s", table_name, e)
                    checkpoint.mark_error(run_id, table_name, str(e))
                    error_count += 1
                    done_count += 1
                    if progress_callback:
                        progress_callback(table_name, "error", done_count, total)
                    continue
                _handle_result(table_name, result)

    # Cross-table relationship discovery (post-profiling enrichment)
    if config.discover_relationships:
        try:
            rels = discover_relationships(results, engine=engine, config=config, quote_fn=adapter.quote_identifier)
            if rels:
                serializer.write_trailer(relationships_to_dict(rels))
                logger.info("Discovered %d relationships across %d tables", len(rels), len(results))
        except Exception as e:
            logger.warning("Relationship discovery failed: %s", e)

    serializer.close()
    checkpoint.close()

    return run_id, results
