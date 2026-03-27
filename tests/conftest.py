"""Shared test fixtures for feather-etl."""

import shutil
from pathlib import Path

import pytest
import yaml

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def client_db(tmp_path) -> Path:
    """Copy test client DuckDB to tmp_path so tests don't modify the fixture."""
    src = FIXTURES_DIR / "client.duckdb"
    dst = tmp_path / "client.duckdb"
    shutil.copy2(src, dst)
    return dst


@pytest.fixture
def config_path(client_db, tmp_path) -> Path:
    """Write a feather.yaml pointing at client_db, return path."""
    config = {
        "source": {
            "type": "duckdb",
            "path": str(client_db),
        },
        "destination": {
            "path": str(tmp_path / "feather_data.duckdb"),
        },
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
    config_file = tmp_path / "feather.yaml"
    config_file.write_text(yaml.dump(config, default_flow_style=False))
    return config_file


@pytest.fixture
def csv_data_dir(tmp_path) -> Path:
    """Copy CSV fixture directory to tmp_path."""
    src = FIXTURES_DIR / "csv_data"
    dst = tmp_path / "csv_data"
    shutil.copytree(src, dst)
    return dst


@pytest.fixture
def sqlite_db(tmp_path) -> Path:
    """Copy SQLite fixture to tmp_path."""
    src = FIXTURES_DIR / "sample_erp.sqlite"
    dst = tmp_path / "sample_erp.sqlite"
    shutil.copy2(src, dst)
    return dst


@pytest.fixture
def sample_erp_db(tmp_path) -> Path:
    """Copy sample_erp DuckDB to tmp_path (includes erp.sales for incremental tests)."""
    src = FIXTURES_DIR / "sample_erp.duckdb"
    dst = tmp_path / "sample_erp.duckdb"
    shutil.copy2(src, dst)
    return dst
