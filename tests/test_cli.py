"""Tests for feather CLI commands."""

import json
import shutil
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from tests.conftest import FIXTURES_DIR

runner = CliRunner()


@pytest.fixture
def cli_env(tmp_path: Path) -> tuple[Path, Path]:
    """Config + source DB for CLI tests."""
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
        ],
    }
    config_path = tmp_path / "feather.yaml"
    config_path.write_text(yaml.dump(config, default_flow_style=False))
    return config_path, tmp_path


class TestInit:
    def test_scaffolds_project(self, tmp_path: Path):
        from feather.cli import app

        result = runner.invoke(app, ["init", str(tmp_path / "test-project")])
        assert result.exit_code == 0
        project = tmp_path / "test-project"
        assert (project / "feather.yaml").exists()
        assert (project / "pyproject.toml").exists()
        assert (project / ".gitignore").exists()
        assert (project / ".env.example").exists()
        assert (project / "transforms" / "silver").is_dir()
        assert (project / "transforms" / "gold").is_dir()
        assert (project / "tables").is_dir()
        assert (project / "extracts").is_dir()


class TestValidate:
    def test_valid_config(self, cli_env: tuple[Path, Path]):
        from feather.cli import app

        config_path, tmp_path = cli_env
        result = runner.invoke(app, ["validate", "--config", str(config_path)])
        assert result.exit_code == 0
        assert (config_path.parent / "feather_validation.json").exists()

    def test_invalid_config(self, tmp_path: Path):
        from feather.cli import app

        bad_config = tmp_path / "feather.yaml"
        bad_config.write_text(yaml.dump({
            "source": {"type": "mongodb", "path": "/nope"},
            "destination": {"path": "./data.duckdb"},
            "tables": [],
        }))
        result = runner.invoke(app, ["validate", "--config", str(bad_config)])
        assert result.exit_code != 0


class TestDiscover:
    def test_lists_tables(self, cli_env: tuple[Path, Path]):
        from feather.cli import app

        config_path, _ = cli_env
        result = runner.invoke(app, ["discover", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "SALESINVOICE" in result.output
        assert "CUSTOMERMASTER" in result.output


class TestSetup:
    def test_creates_state_and_schemas(self, cli_env: tuple[Path, Path]):
        from feather.cli import app

        config_path, tmp_path = cli_env
        result = runner.invoke(app, ["setup", "--config", str(config_path)])
        assert result.exit_code == 0
        assert (tmp_path / "feather_state.duckdb").exists()


class TestRun:
    def test_extracts_tables(self, cli_env: tuple[Path, Path]):
        from feather.cli import app

        config_path, tmp_path = cli_env
        result = runner.invoke(app, ["run", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "success" in result.output.lower()


class TestStatus:
    def test_shows_status_after_run(self, cli_env: tuple[Path, Path]):
        from feather.cli import app

        config_path, tmp_path = cli_env
        runner.invoke(app, ["run", "--config", str(config_path)])
        result = runner.invoke(app, ["status", "--config", str(config_path)])
        assert result.exit_code == 0
        assert "inventory_group" in result.output
