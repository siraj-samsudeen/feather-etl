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
