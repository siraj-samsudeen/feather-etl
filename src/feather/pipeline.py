"""Pipeline orchestrator for feather-etl."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.compute as pc

from feather.config import FeatherConfig, TableConfig
from feather.destinations.duckdb import DuckDBDestination
from feather.sources.registry import create_source
from feather.state import StateManager

import pyarrow as pa

logger = logging.getLogger(__name__)


def _resolve_target(table: TableConfig, mode: str) -> str:
    """Derive effective target from mode unless explicitly set."""
    if table.target_table:
        return table.target_table
    if mode == "prod":
        return f"silver.{table.name}"
    return f"bronze.{table.name}"


def _apply_column_map(data: pa.Table, column_map: dict[str, str]) -> pa.Table:
    """Select and rename columns per column_map. Returns new table with only mapped columns."""
    source_cols = list(column_map.keys())
    # Select only the mapped columns
    data = data.select(source_cols)
    # Rename: build new names in order
    new_names = [column_map[c] for c in data.column_names]
    return data.rename_columns(new_names)


@dataclass
class RunResult:
    table_name: str
    run_id: str
    status: str  # "success", "failure", "skipped"
    rows_loaded: int = 0
    error_message: str | None = None


def run_table(
    config: FeatherConfig,
    table: TableConfig,
    working_dir: Path,
) -> RunResult:
    """Run a single table through the pipeline: extract → load → update state."""
    now = datetime.now(timezone.utc)
    run_id = f"{table.name}_{now.isoformat()}"

    state_path = working_dir / "feather_state.duckdb"
    state = StateManager(state_path)
    state.init_state()

    dest = DuckDBDestination(path=config.destination.path)
    dest.setup_schemas()

    source = create_source(config.source)
    effective_target = _resolve_target(table, config.mode)

    # Prod mode with column_map: extract only mapped columns
    prod_columns = (
        list(table.column_map.keys())
        if config.mode == "prod" and table.column_map
        else None
    )

    # Change detection: check if source file changed since last run
    wm = state.read_watermark(table.name)
    change = source.detect_changes(table.source_table, last_state=wm)

    started_at = datetime.now(timezone.utc)

    if not change.changed:
        ended_at = datetime.now(timezone.utc)
        state.record_run(
            run_id=run_id,
            table_name=table.name,
            started_at=started_at,
            ended_at=ended_at,
            status="skipped",
        )
        # Touch scenario: mtime changed but hash identical — update watermark
        # so next run skips re-hashing
        if change.metadata:
            state.write_watermark(
                table.name,
                strategy=table.strategy,
                last_run_at=ended_at,
                last_file_mtime=change.metadata.get("file_mtime"),
                last_file_hash=change.metadata.get("file_hash"),
                last_checksum=change.metadata.get("checksum"),
                last_row_count=change.metadata.get("row_count"),
            )
        return RunResult(
            table_name=table.name,
            run_id=run_id,
            status="skipped",
        )

    try:
        is_incremental = table.strategy == "incremental"
        wm_last_value = wm.get("last_value") if wm else None
        watermark_before = str(wm_last_value) if wm_last_value else None

        if is_incremental and wm_last_value is not None:
            # Subsequent incremental run: apply overlap window
            overlap = config.defaults.overlap_window_minutes
            wm_dt = datetime.fromisoformat(str(wm_last_value))
            effective_wm = wm_dt - timedelta(minutes=overlap)
            effective_wm_str = effective_wm.isoformat()

            data = source.extract(
                table.source_table,
                columns=prod_columns,
                filter=table.filter,
                watermark_column=table.timestamp_column,
                watermark_value=effective_wm_str,
            )
            if config.mode == "test" and config.defaults.row_limit:
                data = data.slice(0, config.defaults.row_limit)
            if config.mode == "prod" and table.column_map:
                data = _apply_column_map(data, table.column_map)

            if data.num_rows == 0:
                ended_at = datetime.now(timezone.utc)
                state.record_run(
                    run_id=run_id,
                    table_name=table.name,
                    started_at=started_at,
                    ended_at=ended_at,
                    status="success",
                    rows_extracted=0,
                    rows_loaded=0,
                    watermark_before=watermark_before,
                    watermark_after=watermark_before,
                )
                # Update file mtime/hash but keep last_value unchanged
                state.write_watermark(
                    table.name,
                    strategy=table.strategy,
                    last_run_at=ended_at,
                    last_file_mtime=change.metadata.get("file_mtime"),
                    last_file_hash=change.metadata.get("file_hash"),
                    last_value=str(wm_last_value),
                )
                return RunResult(
                    table_name=table.name,
                    run_id=run_id,
                    status="success",
                    rows_loaded=0,
                )

            rows_loaded = dest.load_incremental(
                effective_target, data, run_id, table.timestamp_column
            )
        else:
            # Full strategy, or first incremental run (no watermark yet)
            if is_incremental and table.filter:
                data = source.extract(table.source_table, columns=prod_columns, filter=table.filter)
            else:
                data = source.extract(table.source_table, columns=prod_columns)
            if config.mode == "test" and config.defaults.row_limit:
                data = data.slice(0, config.defaults.row_limit)
            if config.mode == "prod" and table.column_map:
                data = _apply_column_map(data, table.column_map)
            rows_loaded = dest.load_full(effective_target, data, run_id)

        # Compute new watermark value for incremental tables
        new_last_value = None
        if is_incremental and data.num_rows > 0:
            max_ts = pc.max(data.column(table.timestamp_column)).as_py()
            new_last_value = max_ts.isoformat() if max_ts else None
        elif is_incremental and wm_last_value is not None:
            new_last_value = str(wm_last_value)

        watermark_after = new_last_value

        ended_at = datetime.now(timezone.utc)
        state.write_watermark(
            table.name,
            strategy=table.strategy,
            last_run_at=ended_at,
            last_file_mtime=change.metadata.get("file_mtime"),
            last_file_hash=change.metadata.get("file_hash"),
            last_value=new_last_value,
            last_checksum=change.metadata.get("checksum"),
            last_row_count=change.metadata.get("row_count"),
        )
        state.record_run(
            run_id=run_id,
            table_name=table.name,
            started_at=started_at,
            ended_at=ended_at,
            status="success",
            rows_extracted=rows_loaded,
            rows_loaded=rows_loaded,
            watermark_before=watermark_before,
            watermark_after=watermark_after,
        )
        return RunResult(
            table_name=table.name,
            run_id=run_id,
            status="success",
            rows_loaded=rows_loaded,
        )
    except Exception as e:
        ended_at = datetime.now(timezone.utc)
        error_msg = str(e)
        state.record_run(
            run_id=run_id,
            table_name=table.name,
            started_at=started_at,
            ended_at=ended_at,
            status="failure",
            error_message=error_msg,
        )
        return RunResult(
            table_name=table.name,
            run_id=run_id,
            status="failure",
            error_message=error_msg,
        )


def run_all(
    config: FeatherConfig,
    config_path: Path,
) -> list[RunResult]:
    """Run all configured tables.

    After extraction, rebuilds materialized gold transforms if any tables
    succeeded and transform SQL files exist.
    """
    working_dir = config.config_dir
    results: list[RunResult] = []
    for table in config.tables:
        result = run_table(config, table, working_dir)
        results.append(result)

    # Post-extraction transforms (mode-dependent)
    # Include skipped tables: transforms must run even when extraction is
    # skipped (e.g. mode switch dev→prod needs to rematerialise gold).
    any_ok = any(r.status in ("success", "skipped") for r in results)
    if any_ok:
        try:
            from feather.transforms import (
                build_execution_order,
                discover_transforms,
                execute_transforms,
                rebuild_materialized_gold,
            )

            transforms = discover_transforms(config.config_dir)
            if transforms:
                ordered = build_execution_order(transforms)
                dest = DuckDBDestination(path=config.destination.path)
                con = dest._connect()
                try:
                    if config.mode == "prod":
                        # Prod: create all as views first (gold needs silver),
                        # then rebuild gold as materialized tables
                        execute_transforms(con, ordered)
                        rebuild_results = rebuild_materialized_gold(con, ordered)
                        count = sum(1 for r in rebuild_results if r.status == "success")
                        if count:
                            logger.info("Rebuilt %d materialized gold table(s)", count)
                    else:
                        # Dev/test: create everything as views (force_views)
                        execute_transforms(con, ordered, force_views=True)
                finally:
                    con.close()
        except Exception as e:
            logger.error("Transform rebuild failed: %s", e)

    return results
