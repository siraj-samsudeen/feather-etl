"""Tests for incremental extraction (Slice 3)."""

from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa

from feather.destinations.duckdb import DuckDBDestination
from feather.sources.file_source import FileSource
from feather.state import StateManager


class TestBuildWhereClause:
    """Unit tests for FileSource._build_where_clause()."""

    def setup_method(self):
        # FileSource needs a path but we only test the clause builder
        self.fs = FileSource(Path("/dummy"))

    def test_watermark_only(self):
        result = self.fs._build_where_clause("col", "2025-01-01", None)
        assert result == " WHERE col >= '2025-01-01' ORDER BY col"

    def test_watermark_and_filter(self):
        result = self.fs._build_where_clause("col", "2025-01-01", "status <> 1")
        assert result == " WHERE col >= '2025-01-01' AND (status <> 1) ORDER BY col"

    def test_filter_only(self):
        result = self.fs._build_where_clause(None, None, "status <> 1")
        assert result == " WHERE (status <> 1)"

    def test_no_params(self):
        result = self.fs._build_where_clause(None, None, None)
        assert result == ""


def _ts_arrow_table(timestamps: list[str], ids: list[int] | None = None) -> pa.Table:
    """Build a PyArrow table with a modified_at TIMESTAMP column."""
    ts_values = [datetime.fromisoformat(t) for t in timestamps]
    if ids is None:
        ids = list(range(1, len(timestamps) + 1))
    return pa.table(
        {
            "id": ids,
            "amount": [100.0 * i for i in ids],
            "modified_at": ts_values,
        }
    )


class TestLoadIncremental:
    """Unit tests for DuckDBDestination.load_incremental()."""

    def _setup_dest(self, tmp_path: Path) -> DuckDBDestination:
        db_path = tmp_path / "dest.duckdb"
        dest = DuckDBDestination(path=db_path)
        dest.setup_schemas()
        return dest

    def test_incremental_inserts_new_rows(self, tmp_path: Path):
        dest = self._setup_dest(tmp_path)
        # Initial full load
        data1 = _ts_arrow_table(["2025-01-01T00:00:00", "2025-01-02T00:00:00"], [1, 2])
        dest.load_full("bronze.sales", data1, "run_1")

        # Incremental load with overlapping + new rows
        data2 = _ts_arrow_table(["2025-01-02T00:00:00", "2025-01-03T00:00:00"], [2, 3])
        rows = dest.load_incremental("bronze.sales", data2, "run_2", "modified_at")
        assert rows == 2

        # Verify: row id=2 replaced (not duplicated), row id=3 added
        con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        total = con.execute("SELECT COUNT(*) FROM bronze.sales").fetchone()[0]
        assert total == 3  # ids 1, 2, 3
        con.close()

    def test_incremental_zero_rows_is_noop(self, tmp_path: Path):
        dest = self._setup_dest(tmp_path)
        data1 = _ts_arrow_table(["2025-01-01T00:00:00"], [1])
        dest.load_full("bronze.sales", data1, "run_1")

        # Empty batch
        empty = pa.table({"id": pa.array([], type=pa.int64()), "amount": pa.array([], type=pa.float64()), "modified_at": pa.array([], type=pa.timestamp("us"))})
        rows = dest.load_incremental("bronze.sales", empty, "run_2", "modified_at")
        assert rows == 0

        # Original data untouched
        con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        total = con.execute("SELECT COUNT(*) FROM bronze.sales").fetchone()[0]
        assert total == 1
        con.close()

    def test_incremental_deletes_by_timestamp(self, tmp_path: Path):
        dest = self._setup_dest(tmp_path)
        # Load 3 rows
        data1 = _ts_arrow_table(
            ["2025-01-01T00:00:00", "2025-01-02T00:00:00", "2025-01-03T00:00:00"],
            [1, 2, 3],
        )
        dest.load_full("bronze.sales", data1, "run_1")

        # Incremental: only rows >= 2025-01-02 should be deleted and replaced
        data2 = _ts_arrow_table(
            ["2025-01-02T00:00:00", "2025-01-03T00:00:00"], [20, 30]
        )
        dest.load_incremental("bronze.sales", data2, "run_2", "modified_at")

        con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        ids = sorted(
            r[0] for r in con.execute("SELECT id FROM bronze.sales").fetchall()
        )
        con.close()
        # id=1 untouched, ids 2,3 replaced by 20,30
        assert ids == [1, 20, 30]


class TestWatermarkLastValue:
    """Unit tests for StateManager.write_watermark() with last_value."""

    def test_write_watermark_with_last_value(self, tmp_path: Path):
        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()
        sm.write_watermark(
            "orders",
            strategy="incremental",
            last_value="2025-01-10T00:00:00",
        )
        wm = sm.read_watermark("orders")
        assert wm["last_value"] == "2025-01-10T00:00:00"

    def test_update_watermark_last_value(self, tmp_path: Path):
        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()
        sm.write_watermark("orders", strategy="incremental", last_value="2025-01-05")
        sm.write_watermark("orders", strategy="incremental", last_value="2025-01-10")
        wm = sm.read_watermark("orders")
        assert wm["last_value"] == "2025-01-10"

    def test_full_strategy_watermark_has_no_last_value(self, tmp_path: Path):
        sm = StateManager(tmp_path / "state.duckdb")
        sm.init_state()
        sm.write_watermark("orders", strategy="full")
        wm = sm.read_watermark("orders")
        assert wm["last_value"] is None


class TestPipelineIncremental:
    """Tests for pipeline.run_table() with incremental strategy."""

    def _make_source_db(self, tmp_path: Path) -> Path:
        """Create a small DuckDB source with timestamped rows."""
        src_path = tmp_path / "source.duckdb"
        con = duckdb.connect(str(src_path))
        con.execute("CREATE SCHEMA erp")
        con.execute("""
            CREATE TABLE erp.orders (
                order_id INTEGER,
                amount DECIMAL(10,2),
                modified_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO erp.orders VALUES
            (1, 100.00, '2025-01-01 00:00:00'),
            (2, 200.00, '2025-01-02 00:00:00'),
            (3, 300.00, '2025-01-03 00:00:00'),
            (4, 400.00, '2025-01-04 00:00:00'),
            (5, 500.00, '2025-01-05 00:00:00')
        """)
        con.close()
        return src_path

    def _make_config(self, tmp_path: Path, src_path: Path, filter: str | None = None):
        import yaml
        from feather.config import load_config

        table_def = {
            "name": "orders",
            "source_table": "erp.orders",
            "target_table": "bronze.orders",
            "strategy": "incremental",
            "timestamp_column": "modified_at",
        }
        if filter:
            table_def["filter"] = filter

        config_data = {
            "source": {"type": "duckdb", "path": str(src_path)},
            "destination": {"path": str(tmp_path / "dest.duckdb")},
            "tables": [table_def],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config_data))
        return load_config(config_file), config_file

    def test_first_incremental_run_extracts_all(self, tmp_path: Path):
        from feather.pipeline import run_table

        src_path = self._make_source_db(tmp_path)
        cfg, _ = self._make_config(tmp_path, src_path)
        result = run_table(cfg, cfg.tables[0], tmp_path)

        assert result.status == "success"
        assert result.rows_loaded == 5

        # Watermark should be set to MAX(modified_at)
        sm = StateManager(tmp_path / "feather_state.duckdb")
        wm = sm.read_watermark("orders")
        assert wm is not None
        assert wm["last_value"] is not None
        assert "2025-01-05" in str(wm["last_value"])

    def test_second_run_no_new_rows_extracts_zero(self, tmp_path: Path):
        from feather.pipeline import run_table

        src_path = self._make_source_db(tmp_path)
        cfg, _ = self._make_config(tmp_path, src_path)

        # First run
        run_table(cfg, cfg.tables[0], tmp_path)
        # Second run — file unchanged so change detection should skip
        result = run_table(cfg, cfg.tables[0], tmp_path)
        assert result.status == "skipped"

    def test_incremental_after_new_rows(self, tmp_path: Path):
        from feather.pipeline import run_table

        src_path = self._make_source_db(tmp_path)
        cfg, _ = self._make_config(tmp_path, src_path)

        # First run
        run_table(cfg, cfg.tables[0], tmp_path)

        # Add new rows to source
        con = duckdb.connect(str(src_path))
        con.execute("""
            INSERT INTO erp.orders VALUES
            (6, 600.00, '2025-01-06 00:00:00'),
            (7, 700.00, '2025-01-07 00:00:00')
        """)
        con.close()

        # Second run — should extract only new rows + overlap window
        result = run_table(cfg, cfg.tables[0], tmp_path)
        assert result.status == "success"
        # With overlap_window_minutes=2, effective watermark = 2025-01-05 - 2min
        # = 2025-01-04T23:58:00, so rows with modified_at >= that are extracted
        # That means rows 5, 6, 7 (3 rows)
        assert result.rows_loaded == 3

        # Watermark should advance to 2025-01-07
        sm = StateManager(tmp_path / "feather_state.duckdb")
        wm = sm.read_watermark("orders")
        assert "2025-01-07" in str(wm["last_value"])

    def test_run_after_incremental_no_new_rows_watermark_unchanged(self, tmp_path: Path):
        """PRD scenario 4: run after incremental, no new rows → watermark unchanged."""
        from feather.pipeline import run_table

        src_path = self._make_source_db(tmp_path)
        cfg, _ = self._make_config(tmp_path, src_path)

        # First run
        run_table(cfg, cfg.tables[0], tmp_path)

        # Add new rows
        con = duckdb.connect(str(src_path))
        con.execute("INSERT INTO erp.orders VALUES (6, 600.00, '2025-01-06 00:00:00')")
        con.close()

        # Second run picks up new rows
        run_table(cfg, cfg.tables[0], tmp_path)
        sm = StateManager(tmp_path / "feather_state.duckdb")
        wm_after_second = sm.read_watermark("orders")
        watermark_after_second = wm_after_second["last_value"]

        # Third run — file unchanged → skipped, watermark unchanged
        result = run_table(cfg, cfg.tables[0], tmp_path)
        assert result.status == "skipped"
        wm_after_third = sm.read_watermark("orders")
        assert wm_after_third["last_value"] == watermark_after_second

    def test_filter_excludes_matching_rows(self, tmp_path: Path):
        """PRD scenario 5: filter field prevents matching rows from appearing."""
        from feather.pipeline import run_table

        src_path = self._make_source_db(tmp_path)
        # Add a cancelled row
        con = duckdb.connect(str(src_path))
        con.execute("""
            INSERT INTO erp.orders VALUES
            (6, 600.00, '2025-01-06 00:00:00')
        """)
        # Update one row to cancelled status
        con.execute("ALTER TABLE erp.orders ADD COLUMN status VARCHAR DEFAULT 'active'")
        con.close()

        # Recreate source with status column for cleaner test
        src_path2 = tmp_path / "source2.duckdb"
        con = duckdb.connect(str(src_path2))
        con.execute("CREATE SCHEMA erp")
        con.execute("""
            CREATE TABLE erp.orders (
                order_id INTEGER, amount DECIMAL(10,2),
                status VARCHAR, modified_at TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO erp.orders VALUES
            (1, 100.00, 'active',    '2025-01-01 00:00:00'),
            (2, 200.00, 'active',    '2025-01-02 00:00:00'),
            (3, 300.00, 'cancelled', '2025-01-03 00:00:00'),
            (4, 400.00, 'active',    '2025-01-04 00:00:00'),
            (5, 500.00, 'cancelled', '2025-01-05 00:00:00')
        """)
        con.close()

        cfg, _ = self._make_config(tmp_path, src_path2, filter="status <> 'cancelled'")
        result = run_table(cfg, cfg.tables[0], tmp_path)

        assert result.status == "success"
        assert result.rows_loaded == 3  # only active rows

        # Verify no cancelled rows in destination
        dest_con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        cancelled = dest_con.execute(
            "SELECT COUNT(*) FROM bronze.orders WHERE status = 'cancelled'"
        ).fetchone()[0]
        dest_con.close()
        assert cancelled == 0


class TestIncrementalWithFixture:
    """Integration tests using the sample_erp.duckdb fixture (erp.sales)."""

    def _make_config(self, tmp_path: Path, src_path: Path, filter: str | None = None):
        import yaml
        from feather.config import load_config

        table_def = {
            "name": "sales",
            "source_table": "erp.sales",
            "target_table": "bronze.sales",
            "strategy": "incremental",
            "timestamp_column": "modified_at",
        }
        if filter:
            table_def["filter"] = filter

        config_data = {
            "source": {"type": "duckdb", "path": str(src_path)},
            "destination": {"path": str(tmp_path / "dest.duckdb")},
            "tables": [table_def],
        }
        config_file = tmp_path / "feather.yaml"
        config_file.write_text(yaml.dump(config_data))
        return load_config(config_file), config_file

    def test_full_incremental_cycle_with_fixture(self, sample_erp_db, tmp_path: Path):
        """Full PRD cycle: first run → no change → add rows → incremental."""
        from feather.pipeline import run_table

        cfg, _ = self._make_config(tmp_path, sample_erp_db)

        # 1. First run: all 10 rows extracted
        r1 = run_table(cfg, cfg.tables[0], tmp_path)
        assert r1.status == "success"
        assert r1.rows_loaded == 10

        sm = StateManager(tmp_path / "feather_state.duckdb")
        wm1 = sm.read_watermark("sales")
        assert "2025-01-10" in str(wm1["last_value"])

        # 2. Second run: file unchanged → skipped
        r2 = run_table(cfg, cfg.tables[0], tmp_path)
        assert r2.status == "skipped"

        # 3. Add new rows to source
        con = duckdb.connect(str(sample_erp_db))
        con.execute("""
            INSERT INTO erp.sales VALUES
            (11, 105, 1100.00, 'completed', '2025-01-11 00:00:00'),
            (12, 106, 1200.00, 'completed', '2025-01-12 00:00:00')
        """)
        con.close()

        # 4. Third run: only new rows + overlap
        r3 = run_table(cfg, cfg.tables[0], tmp_path)
        assert r3.status == "success"
        # effective wm = 2025-01-10 - 2min = 2025-01-09T23:58:00
        # rows >= that: sale_id 10 (Jan 10), 11 (Jan 11), 12 (Jan 12) = 3 rows
        assert r3.rows_loaded == 3

        wm3 = sm.read_watermark("sales")
        assert "2025-01-12" in str(wm3["last_value"])

        # 5. Fourth run: file unchanged again → skipped
        r4 = run_table(cfg, cfg.tables[0], tmp_path)
        assert r4.status == "skipped"

        # Verify total rows in destination: 10 original - 1 overlapping + 3 new = 12
        dest_con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        total = dest_con.execute("SELECT COUNT(*) FROM bronze.sales").fetchone()[0]
        dest_con.close()
        assert total == 12

    def test_filter_with_fixture(self, sample_erp_db, tmp_path: Path):
        """erp.sales has 2 cancelled rows — filter should exclude them."""
        from feather.pipeline import run_table

        cfg, _ = self._make_config(
            tmp_path, sample_erp_db, filter="status <> 'cancelled'"
        )
        result = run_table(cfg, cfg.tables[0], tmp_path)

        assert result.status == "success"
        assert result.rows_loaded == 8  # 10 total - 2 cancelled

        dest_con = duckdb.connect(str(tmp_path / "dest.duckdb"), read_only=True)
        cancelled = dest_con.execute(
            "SELECT COUNT(*) FROM bronze.sales WHERE status = 'cancelled'"
        ).fetchone()[0]
        dest_con.close()
        assert cancelled == 0

    def test_overlap_window_arithmetic(self, sample_erp_db, tmp_path: Path):
        """Verify effective_watermark = stored_watermark - overlap_window_minutes."""
        from feather.pipeline import run_table

        cfg, _ = self._make_config(tmp_path, sample_erp_db)

        # First run
        run_table(cfg, cfg.tables[0], tmp_path)

        sm = StateManager(tmp_path / "feather_state.duckdb")
        wm = sm.read_watermark("sales")
        stored_wm = str(wm["last_value"])
        assert "2025-01-10" in stored_wm

        # Add a row barely after the overlap window
        con = duckdb.connect(str(sample_erp_db))
        con.execute("""
            INSERT INTO erp.sales VALUES
            (11, 105, 1100.00, 'completed', '2025-01-10 00:05:00')
        """)
        con.close()

        # Run again — effective watermark = 2025-01-10T00:00:00 - 2min = 2025-01-09T23:58:00
        # Row at 2025-01-10 00:00:00 (sale_id=10) and 2025-01-10 00:05:00 (sale_id=11) should be extracted
        r2 = run_table(cfg, cfg.tables[0], tmp_path)
        assert r2.status == "success"
        assert r2.rows_loaded == 2  # sale_id 10 and 11
