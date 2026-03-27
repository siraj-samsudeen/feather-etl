"""feather CLI — thin wrapper over config, pipeline, state, and sources."""

from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(name="feather", help="feather-etl: config-driven ETL")


def _load_and_validate(config_path: Path):
    """Load config, validate, write validation JSON. Raises on failure."""
    from feather.config import load_config, write_validation_json

    try:
        cfg = load_config(config_path)
        write_validation_json(config_path, cfg)
        return cfg
    except (ValueError, FileNotFoundError) as e:
        if isinstance(e, FileNotFoundError):
            typer.echo(f"Config file not found: {config_path}", err=True)
        else:
            write_validation_json(config_path, None, errors=[str(e)])
            typer.echo(f"Validation failed: {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def init(project_name: str) -> None:
    """Scaffold a new client project."""
    from feather.init_wizard import scaffold_project

    project_path = Path(project_name).resolve()
    if project_path.exists():
        non_hidden = [f for f in project_path.iterdir() if not f.name.startswith(".")]
        if non_hidden:
            typer.echo(
                f"Directory '{project_name}' already exists and is not empty.",
                err=True,
            )
            raise typer.Exit(code=1)

    created = scaffold_project(project_path)
    typer.echo(f"Project scaffolded at {project_path}")
    for f in created:
        typer.echo(f"  {f}")


@app.command()
def validate(config: Path = typer.Option("feather.yaml", "--config")) -> None:
    """Validate config and write feather_validation.json."""
    cfg = _load_and_validate(config)
    typer.echo(f"Config valid: {len(cfg.tables)} table(s)")
    typer.echo(f"  Source: {cfg.source.type} ({cfg.source.path})")
    typer.echo(f"  Destination: {cfg.destination.path}")
    typer.echo(f"  State: {cfg.config_dir / 'feather_state.duckdb'}")
    for t in cfg.tables:
        typer.echo(f"  Table: {t.name} → {t.target_table} ({t.strategy})")


@app.command()
def discover(config: Path = typer.Option("feather.yaml", "--config")) -> None:
    """List tables and columns available in the configured source."""
    from feather.sources.registry import create_source

    cfg = _load_and_validate(config)
    source = create_source(cfg.source)

    if not source.check():
        typer.echo("Source connection failed.", err=True)
        raise typer.Exit(code=2)

    schemas = source.discover()
    typer.echo(f"Found {len(schemas)} table(s):\n")
    for s in schemas:
        typer.echo(f"  {s.name}")
        for col_name, col_type in s.columns:
            typer.echo(f"    {col_name}: {col_type}")
        typer.echo()


@app.command()
def setup(config: Path = typer.Option("feather.yaml", "--config")) -> None:
    """Preview and initialize state DB and schemas. Optional — feather run creates them automatically."""
    from feather.destinations.duckdb import DuckDBDestination
    from feather.state import StateManager

    cfg = _load_and_validate(config)

    state_path = cfg.config_dir / "feather_state.duckdb"
    sm = StateManager(state_path)
    sm.init_state()
    typer.echo(f"State DB initialized: {state_path}")

    dest = DuckDBDestination(path=cfg.destination.path)
    dest.setup_schemas()
    typer.echo(f"Schemas created in: {cfg.destination.path}")

    # Execute transforms (silver views, gold views/tables) if any exist
    from feather.transforms import (
        build_execution_order,
        discover_transforms,
        execute_transforms,
    )

    transforms = discover_transforms(cfg.config_dir)
    if transforms:
        ordered = build_execution_order(transforms)
        con = dest._connect()
        try:
            results = execute_transforms(con, ordered)
        finally:
            con.close()

        silver_views = sum(
            1 for r in results if r.schema == "silver" and r.status == "success"
        )
        gold_views = sum(
            1
            for r in results
            if r.schema == "gold" and r.type == "view" and r.status == "success"
        )
        gold_tables = sum(
            1
            for r in results
            if r.schema == "gold" and r.type == "table" and r.status == "success"
        )
        parts = []
        if silver_views:
            parts.append(f"{silver_views} silver view(s)")
        if gold_views:
            parts.append(f"{gold_views} gold view(s)")
        if gold_tables:
            parts.append(f"{gold_tables} gold table(s)")
        typer.echo(f"Transforms applied: {', '.join(parts)}")

        errors = [r for r in results if r.status == "error"]
        for r in errors:
            typer.echo(f"  Transform error: {r.schema}.{r.name} — {r.error}", err=True)


@app.command()
def run(config: Path = typer.Option("feather.yaml", "--config")) -> None:
    """Extract all configured tables."""
    from feather.pipeline import run_all

    cfg = _load_and_validate(config)
    results = run_all(cfg, config)

    for r in results:
        if r.status == "success":
            typer.echo(f"  {r.table_name}: {r.status} ({r.rows_loaded} rows)")
        elif r.status == "skipped":
            typer.echo(f"  {r.table_name}: skipped (unchanged)")
        else:
            typer.echo(f"  {r.table_name}: {r.status} — {r.error_message}")

    successes = sum(1 for r in results if r.status == "success")
    skipped = sum(1 for r in results if r.status == "skipped")
    failures = sum(1 for r in results if r.status == "failure")

    parts = [f"{successes}/{len(results)} tables extracted"]
    if skipped:
        parts.append(f"{skipped} skipped")
    typer.echo(f"\n{', '.join(parts)}.")

    if failures > 0:
        raise typer.Exit(code=1)


@app.command()
def status(config: Path = typer.Option("feather.yaml", "--config")) -> None:
    """Show last run status for all tables."""
    from feather.state import StateManager

    cfg = _load_and_validate(config)
    state_path = cfg.config_dir / "feather_state.duckdb"

    if not state_path.exists():
        typer.echo("No state DB found. Run 'feather setup' first.", err=True)
        raise typer.Exit(code=1)

    sm = StateManager(state_path)
    rows = sm.get_status()

    if not rows:
        typer.echo("No runs recorded yet.")
        return

    typer.echo(f"{'Table':<30} {'Status':<12} {'Rows':<10} {'Last Run'}")
    typer.echo("-" * 75)
    for row in rows:
        typer.echo(
            f"{row['table_name']:<30} {row['status']:<12} "
            f"{row.get('rows_loaded', '-'):<10} {row.get('ended_at', '-')}"
        )
        if row["status"] == "failure" and row.get("error_message"):
            error = str(row["error_message"])
            if len(error) > 80:
                error = error[:77] + "..."
            typer.echo(f"  Error: {error}")
