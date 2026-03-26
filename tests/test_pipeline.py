"""Tests for feather.pipeline module."""

import json
from pathlib import Path

import duckdb
import pytest
import yaml

from tests.conftest import FIXTURES_DIR


@pytest.fixture
def setup_env(tmp_path: Path) -> tuple[Path, Path]:
    """Set up a complete pipeline environment: config + source DB."""
    import shutil

    client_db = tmp_path / "client.duckdb"
    shutil.copy2(FIXTURES_DIR / "client.duckdb", client_db)

    config = {
        "source": {"type": "duckdb", "path": str(client_db)},
        "destination": {"path": str(tmp_path / "feather_data.duckdb")},
        "tables": [
            {
                "name": "inventory_group",
                "source_table": "icube.InventoryGroup",
                "target_table": "bronze.inventory_group",
                "strategy": "full",
            },
            {
                "name": "customer_master",
                "source_table": "icube.CUSTOMERMASTER",
                "target_table": "bronze.customer_master",
                "strategy": "full",
            },
        ],
    }
    config_path = tmp_path / "feather.yaml"
    config_path.write_text(yaml.dump(config, default_flow_style=False))
    return config_path, tmp_path


class TestRunTable:
    def test_extracts_and_loads(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_table

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)

        result = run_table(cfg, cfg.tables[0], tmp_path)
        assert result.status == "success"
        assert result.rows_loaded == 66  # InventoryGroup has 66 rows

    def test_records_state(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_table
        from feather.state import StateManager

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)
        run_table(cfg, cfg.tables[0], tmp_path)

        sm = StateManager(tmp_path / "feather_state.duckdb")
        status = sm.get_status()
        assert len(status) == 1
        assert status[0]["status"] == "success"

    def test_data_has_etl_metadata(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_table

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)
        run_table(cfg, cfg.tables[0], tmp_path)

        con = duckdb.connect(str(tmp_path / "feather_data.duckdb"), read_only=True)
        row = con.execute(
            "SELECT _etl_loaded_at, _etl_run_id FROM bronze.inventory_group LIMIT 1"
        ).fetchone()
        con.close()
        assert row[0] is not None
        assert "inventory_group" in row[1]


class TestRunAll:
    def test_runs_all_tables(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_all

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)

        results = run_all(cfg, config_path)
        assert len(results) == 2
        assert all(r.status == "success" for r in results)

    def test_failed_table_doesnt_stop_others(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_all

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)
        # Point one table to nonexistent source table
        cfg.tables[0].source_table = "icube.NONEXISTENT"

        results = run_all(cfg, config_path)
        statuses = {r.table_name: r.status for r in results}
        assert statuses["inventory_group"] == "failure"
        assert statuses["customer_master"] == "success"

    def test_writes_validation_json(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_all

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)
        run_all(cfg, config_path)

        vj_path = config_path.parent / "feather_validation.json"
        assert vj_path.exists()
        vj = json.loads(vj_path.read_text())
        assert vj["valid"] is True

    def test_second_run_reextracts(self, setup_env: tuple[Path, Path]):
        from feather.config import load_config
        from feather.pipeline import run_all

        config_path, tmp_path = setup_env
        cfg = load_config(config_path)

        run_all(cfg, config_path)
        results2 = run_all(cfg, config_path)
        assert all(r.status == "success" for r in results2)
