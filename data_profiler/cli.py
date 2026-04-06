"""CLI interface: Rich progress bars, Click commands."""

from __future__ import annotations

import logging
import sys

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.table import Table

from data_profiler.config import ProfilerConfig
from data_profiler.run import run_profiler

console = Console(stderr=True)


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose: bool) -> None:
    """Multi-engine data profiler with portable type schema."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )


@cli.command()
@click.option("--engine", required=True, type=click.Choice(["snowflake", "databricks", "duckdb", "sqlite"]))
@click.option("--dsn", required=True, help="SQLAlchemy connection string")
@click.option("--database", default=None, help="Database/catalog to profile")
@click.option("--schema", "schema_name", default=None, help="Schema filter")
@click.option("--sample", default=10000, type=int, help="Rows sampled per table (0 = full scan)")
@click.option("--concurrency", default=4, type=int, help="Parallel table workers")
@click.option("--stats-depth", default="full", type=click.Choice(["full", "fast"]))
@click.option("--exact-distinct", is_flag=True, help="Use COUNT(DISTINCT) instead of HLL")
@click.option("--column-batch-size", default=80, type=int, help="Max columns per SELECT")
@click.option("--output-format", default="json", type=click.Choice(["json", "parquet", "yaml", "html"]))
@click.option("--output", "-o", default=None, help="Output file path")
@click.option("--resume", default=None, help="Run ID to resume")
def run(
    engine: str,
    dsn: str,
    database: str | None,
    schema_name: str | None,
    sample: int,
    concurrency: int,
    stats_depth: str,
    exact_distinct: bool,
    column_batch_size: int,
    output_format: str,
    output: str | None,
    resume: str | None,
) -> None:
    """Profile tables across database engines."""
    if sample > 0 and sample < 2:
        console.print("[yellow]Warning: sample size < 2 — mean and stddev will be NULL[/yellow]")

    config = ProfilerConfig(
        engine=engine,
        dsn=dsn,
        database=database,
        schema_name=schema_name,
        sample_size=sample,
        concurrency=concurrency,
        stats_depth=stats_depth,
        exact_distinct=exact_distinct,
        column_batch_size=column_batch_size,
        output_format=output_format,
        output=output,
        resume=resume,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:
        task_id: TaskID | None = None
        last_table = ""

        def on_progress(table_name: str, status: str, current: int, total: int) -> None:
            nonlocal task_id, last_table
            if task_id is None:
                desc = f"Profiling {total} tables on {engine}"
                task_id = progress.add_task(desc, total=total)
            mark = "[green]✓[/green]" if status == "done" else "[red]✗[/red]"
            last_table = f"{table_name} {mark}"
            progress.update(task_id, completed=current, description=f"Profiling [bold]{table_name}[/bold]")

        run_id, results = run_profiler(config, progress_callback=on_progress)

    # Summary table
    total_columns = sum(len(r.columns) for r in results)
    total_anomalies = sum(len(a) for r in results for c in r.columns for a in [c.anomalies])
    errors = sum(1 for r in results if r.error)
    total_duration = sum(r.duration_seconds for r in results)

    output_path = config.output or f"profiles/{run_id}.ndjson"

    summary = Table(title="Summary", show_header=False)
    summary.add_column("Key", style="bold")
    summary.add_column("Value")
    summary.add_row("Tables profiled", str(len(results)))
    summary.add_row("Columns total", f"{total_columns:,}")
    summary.add_row("Anomalies found", str(total_anomalies))
    if errors:
        summary.add_row("Errors", f"[red]{errors}[/red]")
    summary.add_row("Duration", f"{total_duration:.1f}s")
    summary.add_row("Output", output_path)
    summary.add_row("Run ID", f"{run_id} (resumable)")

    console.print()
    console.print(summary)


@cli.command()
@click.option("--engine", required=True, type=click.Choice(["snowflake", "databricks", "duckdb", "sqlite"]))
@click.option("--dsn", required=True, help="SQLAlchemy connection string")
@click.option("--database", default=None, help="Database/catalog name")
@click.option("--schema", "schema_name", default=None, help="Schema filter")
@click.option("--sample", default=10000, type=int, help="Rows sampled per table (0 = full scan)")
@click.option("--format", "export_format", default="openmetadata-json", type=click.Choice(["openmetadata-json"]))
@click.option("--output", "-o", default=None, help="Output file path")
def export(
    engine: str,
    dsn: str,
    database: str | None,
    schema_name: str | None,
    sample: int,
    export_format: str,
    output: str | None,
) -> None:
    """Export profiling results in catalog-compatible format."""
    from data_profiler.persistence.openmetadata import export_openmetadata
    from data_profiler.workers.relationship_worker import discover_relationships, relationships_to_dict

    config = ProfilerConfig(
        engine=engine,
        dsn=dsn,
        database=database,
        schema_name=schema_name,
        sample_size=sample,
        concurrency=1 if engine == "duckdb" else 4,
        output_format="json",  # internal format doesn't matter for export
    )

    console.print(f"[bold]Profiling for {export_format} export...[/bold]")
    run_id, results = run_profiler(config)

    rels = discover_relationships(results)
    rel_dicts = [
        {"source_table": r.source_table, "source_columns": r.source_columns,
         "target_table": r.target_table, "target_columns": r.target_columns,
         "relationship_type": r.relationship_type}
        for r in rels
    ]

    out_path = output or f"profiles/{run_id}_openmetadata.json"
    export_openmetadata(
        results=results,
        output_path=out_path,
        run_id=run_id,
        engine=engine,
        database=database,
        schema=schema_name,
        relationships=rel_dicts,
    )

    console.print(f"[green]Exported to {out_path}[/green]")


@cli.command()
@click.option("--engine", required=True, type=click.Choice(["snowflake", "databricks", "duckdb", "sqlite"]))
@click.option("--dsn", required=True, help="SQLAlchemy connection string")
@click.option("--database", default=None, help="Database/catalog name")
@click.option("--schema", "schema_name", default=None, help="Schema filter")
@click.option("--sample", default=10000, type=int, help="Rows sampled per table (0 = full scan)")
@click.option("--output", "-o", default=None, help="Output file path")
def dashboard(
    engine: str,
    dsn: str,
    database: str | None,
    schema_name: str | None,
    sample: int,
    output: str | None,
) -> None:
    """Generate interactive HTML dashboard."""
    from datetime import datetime, timezone
    from dataclasses import asdict
    from data_profiler.dashboard import generate_dashboard
    from data_profiler.workers.relationship_worker import discover_relationships

    config = ProfilerConfig(
        engine=engine,
        dsn=dsn,
        database=database,
        schema_name=schema_name,
        sample_size=sample,
        concurrency=1 if engine == "duckdb" else 4,
        output_format="json",
    )

    console.print("[bold]Profiling for dashboard...[/bold]")
    run_id, results = run_profiler(config)

    rels = discover_relationships(results)
    rel_dicts = [asdict(r) for r in rels] if rels else None

    out_path = output or f"profiles/{run_id}_dashboard.html"
    generate_dashboard(
        run_id=run_id,
        engine=engine,
        profiled_at=datetime.now(timezone.utc).isoformat(),
        results=results,
        output_path=out_path,
        relationships=rel_dicts,
    )

    console.print(f"[green]Dashboard saved to {out_path}[/green]")
    console.print(f"Open with: [bold]open {out_path}[/bold]")


if __name__ == "__main__":
    cli()
