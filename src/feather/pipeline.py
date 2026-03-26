"""Pipeline orchestrator for feather-etl."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from feather.config import FeatherConfig, TableConfig, write_validation_json
from feather.destinations.duckdb import DuckDBDestination
from feather.sources.registry import create_source
from feather.state import StateManager

logger = logging.getLogger(__name__)


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

    started_at = datetime.now(timezone.utc)
    try:
        data = source.extract(table.source_table)
        rows_loaded = dest.load_full(table.target_table, data, run_id)

        ended_at = datetime.now(timezone.utc)
        state.write_watermark(table.name, strategy=table.strategy, last_run_at=ended_at)
        state.record_run(
            run_id=run_id,
            table_name=table.name,
            started_at=started_at,
            ended_at=ended_at,
            status="success",
            rows_extracted=rows_loaded,
            rows_loaded=rows_loaded,
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
        logger.error("Table %s failed: %s", table.name, error_msg)
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
    """Run all configured tables. Writes validation JSON before starting."""
    write_validation_json(config_path, config)

    working_dir = config.config_dir
    results: list[RunResult] = []
    for table in config.tables:
        result = run_table(config, table, working_dir)
        results.append(result)
    return results
