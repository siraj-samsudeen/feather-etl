"""Tests for feather.state module."""

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest


CURRENT_SCHEMA_VERSION = 1


class TestStateInit:
    def test_creates_all_tables(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()

        con = duckdb.connect(str(sm.path), read_only=True)
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        con.close()
        expected = {
            "_state_meta",
            "_watermarks",
            "_runs",
            "_run_steps",
            "_dq_results",
            "_schema_snapshots",
        }
        assert tables == expected

    def test_state_meta_version(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()

        con = duckdb.connect(str(sm.path), read_only=True)
        row = con.execute("SELECT schema_version FROM _state_meta").fetchone()
        con.close()
        assert row[0] == CURRENT_SCHEMA_VERSION

    def test_idempotent_init(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()
        sm.init_state()  # should not error or duplicate

        con = duckdb.connect(str(sm.path), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM _state_meta").fetchone()[0]
        con.close()
        assert count == 1


class TestWatermarks:
    def test_write_and_read(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()

        sm.write_watermark("sales_invoice", strategy="full")
        wm = sm.read_watermark("sales_invoice")
        assert wm is not None
        assert wm["table_name"] == "sales_invoice"
        assert wm["strategy"] == "full"

    def test_read_nonexistent(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()
        assert sm.read_watermark("nonexistent") is None

    def test_upsert_watermark(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()

        sm.write_watermark("test_table", strategy="full")
        sm.write_watermark("test_table", strategy="full")

        con = duckdb.connect(str(sm.path), read_only=True)
        count = con.execute(
            "SELECT COUNT(*) FROM _watermarks WHERE table_name = 'test_table'"
        ).fetchone()[0]
        con.close()
        assert count == 1


class TestRuns:
    def test_record_and_get_status(self, tmp_path: Path):
        from feather.state import StateManager

        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()

        now = datetime.now(timezone.utc)
        sm.record_run(
            run_id="sales_invoice_2025-01-01T00:00:00",
            table_name="sales_invoice",
            started_at=now,
            ended_at=now,
            status="success",
            rows_extracted=100,
            rows_loaded=100,
        )

        status = sm.get_status()
        assert len(status) == 1
        assert status[0]["table_name"] == "sales_invoice"
        assert status[0]["status"] == "success"
        assert status[0]["rows_loaded"] == 100
