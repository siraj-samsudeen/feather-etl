"""Tests for feather.destinations module."""

from pathlib import Path

import duckdb
import pyarrow as pa
import pytest


def _sample_arrow_table(num_rows: int = 10) -> pa.Table:
    return pa.table(
        {
            "id": list(range(num_rows)),
            "name": [f"item_{i}" for i in range(num_rows)],
            "value": [float(i) * 1.5 for i in range(num_rows)],
        }
    )


class TestDuckDBDestination:
    def test_setup_schemas(self, tmp_path: Path):
        from feather.destinations.duckdb import DuckDBDestination

        db_path = tmp_path / "data.duckdb"
        dest = DuckDBDestination(path=db_path)
        dest.setup_schemas()

        con = duckdb.connect(str(db_path), read_only=True)
        schemas = [
            r[0]
            for r in con.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        ]
        con.close()
        for s in ["bronze", "silver", "gold", "_quarantine"]:
            assert s in schemas

    def test_load_full_creates_table(self, tmp_path: Path):
        from feather.destinations.duckdb import DuckDBDestination

        db_path = tmp_path / "data.duckdb"
        dest = DuckDBDestination(path=db_path)
        dest.setup_schemas()

        data = _sample_arrow_table(10)
        rows = dest.load_full("bronze.test_table", data, "test_run_001")
        assert rows == 10

        con = duckdb.connect(str(db_path), read_only=True)
        result = con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()
        assert result[0] == 10
        con.close()

    def test_load_full_has_etl_metadata(self, tmp_path: Path):
        from feather.destinations.duckdb import DuckDBDestination

        db_path = tmp_path / "data.duckdb"
        dest = DuckDBDestination(path=db_path)
        dest.setup_schemas()

        data = _sample_arrow_table(5)
        dest.load_full("bronze.test_table", data, "run_xyz")

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute(
            "SELECT _etl_loaded_at, _etl_run_id FROM bronze.test_table LIMIT 1"
        ).fetchone()
        con.close()
        assert row[0] is not None  # _etl_loaded_at
        assert row[1] == "run_xyz"  # _etl_run_id

    def test_load_full_swap_replaces_data(self, tmp_path: Path):
        from feather.destinations.duckdb import DuckDBDestination

        db_path = tmp_path / "data.duckdb"
        dest = DuckDBDestination(path=db_path)
        dest.setup_schemas()

        data1 = _sample_arrow_table(10)
        dest.load_full("bronze.test_table", data1, "run_1")

        data2 = _sample_arrow_table(5)
        dest.load_full("bronze.test_table", data2, "run_2")

        con = duckdb.connect(str(db_path), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM bronze.test_table").fetchone()[0]
        run_id = con.execute(
            "SELECT DISTINCT _etl_run_id FROM bronze.test_table"
        ).fetchone()[0]
        con.close()
        assert count == 5  # replaced, not appended
        assert run_id == "run_2"
