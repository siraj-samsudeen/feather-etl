"""End-to-end integration test: full Slice 1 onboarding flow."""

import shutil
from pathlib import Path

import duckdb
import yaml
from typer.testing import CliRunner

from tests.conftest import FIXTURES_DIR

runner = CliRunner()


def test_full_onboarding_flow(tmp_path: Path):
    """init → validate → discover → setup → run → status → run again."""
    from feather.cli import app

    # --- 1. feather init ---
    project_dir = tmp_path / "client-test"
    result = runner.invoke(app, ["init", str(project_dir)])
    assert result.exit_code == 0, result.output
    assert (project_dir / "feather.yaml").exists()
    assert (project_dir / "pyproject.toml").exists()
    assert (project_dir / ".gitignore").exists()
    assert (project_dir / ".env.example").exists()
    assert (project_dir / "transforms" / "silver").is_dir()
    assert (project_dir / "transforms" / "gold").is_dir()
    assert (project_dir / "tables").is_dir()
    assert (project_dir / "extracts").is_dir()

    # --- 2. Edit feather.yaml (simulating operator) ---
    client_db = project_dir / "client.duckdb"
    shutil.copy2(FIXTURES_DIR / "client.duckdb", client_db)

    config = {
        "source": {"type": "duckdb", "path": str(client_db)},
        "destination": {"path": str(project_dir / "feather_data.duckdb")},
        "tables": [
            {
                "name": "sales_invoice",
                "source_table": "icube.SALESINVOICE",
                "target_table": "bronze.sales_invoice",
                "strategy": "full",
            },
            {
                "name": "customer_master",
                "source_table": "icube.CUSTOMERMASTER",
                "target_table": "bronze.customer_master",
                "strategy": "full",
            },
            {
                "name": "inventory_group",
                "source_table": "icube.InventoryGroup",
                "target_table": "bronze.inventory_group",
                "strategy": "full",
            },
        ],
    }
    config_path = project_dir / "feather.yaml"
    config_path.write_text(yaml.dump(config, default_flow_style=False))

    # --- 3. feather validate ---
    result = runner.invoke(app, ["validate", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    vj_path = project_dir / "feather_validation.json"
    assert vj_path.exists()

    # --- 4. feather discover ---
    result = runner.invoke(app, ["discover", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "SALESINVOICE" in result.output
    assert "CUSTOMERMASTER" in result.output
    assert "InventoryGroup" in result.output

    # --- 5. feather setup ---
    result = runner.invoke(app, ["setup", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert (project_dir / "feather_state.duckdb").exists()

    # --- 6. feather run ---
    result = runner.invoke(app, ["run", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "3/3 tables succeeded" in result.output

    # Verify bronze tables exist with correct data
    data_db = str(project_dir / "feather_data.duckdb")
    con = duckdb.connect(data_db, read_only=True)

    si_count = con.execute("SELECT COUNT(*) FROM bronze.sales_invoice").fetchone()[0]
    assert si_count == 11676

    cm_count = con.execute("SELECT COUNT(*) FROM bronze.customer_master").fetchone()[0]
    assert cm_count == 1339

    ig_count = con.execute("SELECT COUNT(*) FROM bronze.inventory_group").fetchone()[0]
    assert ig_count == 66

    # Verify ETL metadata columns
    row = con.execute(
        "SELECT _etl_loaded_at, _etl_run_id FROM bronze.sales_invoice LIMIT 1"
    ).fetchone()
    assert row[0] is not None  # _etl_loaded_at
    assert "sales_invoice" in row[1]  # _etl_run_id
    con.close()

    # --- 7. feather status ---
    result = runner.invoke(app, ["status", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "sales_invoice" in result.output
    assert "customer_master" in result.output
    assert "inventory_group" in result.output
    assert "success" in result.output

    # --- 8. Second feather run (re-extracts, no change detection) ---
    result = runner.invoke(app, ["run", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "3/3 tables succeeded" in result.output

    # Verify feather_validation.json was written
    assert vj_path.exists()
