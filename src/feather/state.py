"""State management for feather-etl — watermarks, run history, schema versioning."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

import feather

SCHEMA_VERSION = 1


class StateManager:
    """Manages the feather_state.duckdb file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def _connect(self) -> duckdb.DuckDBPyConnection:
        is_new = not self.path.exists()
        con = duckdb.connect(str(self.path))
        if is_new and os.name != "nt":
            try:
                os.chmod(self.path, 0o600)
            except OSError:
                pass
        return con

    def init_state(self) -> None:
        """Create all state tables. Idempotent."""
        con = self._connect()

        con.execute("""
            CREATE TABLE IF NOT EXISTS _state_meta (
                schema_version INTEGER PRIMARY KEY,
                created_at TIMESTAMP,
                feather_version VARCHAR
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _watermarks (
                table_name VARCHAR PRIMARY KEY,
                strategy VARCHAR,
                last_value VARCHAR,
                last_checksum INTEGER,
                last_row_count INTEGER,
                last_file_mtime DOUBLE,
                last_file_hash VARCHAR,
                last_run_at TIMESTAMP,
                retry_count INTEGER DEFAULT 0,
                retry_after TIMESTAMP,
                boundary_hashes JSON
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _runs (
                run_id VARCHAR PRIMARY KEY,
                table_name VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                duration_sec DOUBLE,
                status VARCHAR,
                rows_extracted INTEGER,
                rows_loaded INTEGER,
                rows_skipped INTEGER,
                error_message VARCHAR,
                watermark_before VARCHAR,
                watermark_after VARCHAR,
                freshness_max_ts TIMESTAMP,
                schema_changes JSON
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _run_steps (
                run_id VARCHAR,
                step VARCHAR,
                message VARCHAR,
                started_at TIMESTAMP,
                ended_at TIMESTAMP
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _dq_results (
                run_id VARCHAR,
                table_name VARCHAR,
                check_type VARCHAR,
                column_name VARCHAR,
                result VARCHAR,
                details VARCHAR,
                checked_at TIMESTAMP
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS _schema_snapshots (
                table_name VARCHAR,
                column_name VARCHAR,
                data_type VARCHAR,
                snapshot_at TIMESTAMP,
                PRIMARY KEY (table_name, column_name)
            )
        """)

        # Check version and handle init vs downgrade protection
        row = con.execute("SELECT schema_version FROM _state_meta LIMIT 1").fetchone()
        if row is None:
            con.execute(
                "INSERT INTO _state_meta VALUES (?, ?, ?)",
                [SCHEMA_VERSION, datetime.now(timezone.utc), feather.__version__],
            )
        elif row[0] > SCHEMA_VERSION:
            con.close()
            raise RuntimeError(
                f"State DB schema version {row[0]} is newer than feather-etl "
                f"version {SCHEMA_VERSION}. Upgrade feather-etl."
            )

        con.close()

    def read_watermark(self, table_name: str) -> dict[str, object] | None:
        con = self._connect()
        row = con.execute(
            "SELECT * FROM _watermarks WHERE table_name = ?", [table_name]
        ).fetchone()
        if row is None:
            con.close()
            return None
        columns = [desc[0] for desc in con.description]
        con.close()
        return dict(zip(columns, row))

    def write_watermark(
        self,
        table_name: str,
        strategy: str,
        last_run_at: datetime | None = None,
        **kwargs: object,
    ) -> None:
        if last_run_at is None:
            last_run_at = datetime.now(timezone.utc)
        con = self._connect()
        existing = con.execute(
            "SELECT COUNT(*) FROM _watermarks WHERE table_name = ?", [table_name]
        ).fetchone()[0]
        if existing:
            con.execute(
                "UPDATE _watermarks SET strategy = ?, last_run_at = ? "
                "WHERE table_name = ?",
                [strategy, last_run_at, table_name],
            )
        else:
            con.execute(
                "INSERT INTO _watermarks (table_name, strategy, last_run_at) "
                "VALUES (?, ?, ?)",
                [table_name, strategy, last_run_at],
            )
        con.close()

    def record_run(
        self,
        run_id: str,
        table_name: str,
        started_at: datetime,
        ended_at: datetime,
        status: str,
        rows_extracted: int = 0,
        rows_loaded: int = 0,
        rows_skipped: int = 0,
        error_message: str | None = None,
        watermark_before: str | None = None,
        watermark_after: str | None = None,
        freshness_max_ts: datetime | None = None,
        schema_changes: str | None = None,
    ) -> None:
        duration = (ended_at - started_at).total_seconds()
        con = self._connect()
        con.execute(
            "INSERT INTO _runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                run_id,
                table_name,
                started_at,
                ended_at,
                duration,
                status,
                rows_extracted,
                rows_loaded,
                rows_skipped,
                error_message,
                watermark_before,
                watermark_after,
                freshness_max_ts,
                schema_changes,
            ],
        )
        con.close()

    def get_status(self) -> list[dict[str, object]]:
        """Return last run per table."""
        con = self._connect()
        rows = con.execute("""
            SELECT r.table_name, r.status, r.rows_loaded, r.ended_at, r.run_id
            FROM _runs r
            INNER JOIN (
                SELECT table_name, MAX(started_at) as max_started
                FROM _runs GROUP BY table_name
            ) latest ON r.table_name = latest.table_name
                AND r.started_at = latest.max_started
            ORDER BY r.table_name
        """).fetchall()
        columns = [desc[0] for desc in con.description]
        con.close()
        return [dict(zip(columns, row)) for row in rows]
