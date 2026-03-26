"""Tests for feather.config module."""

import json
import os
from pathlib import Path

import pytest
import yaml


def _write_config(tmp_path: Path, config: dict) -> Path:
    config_file = tmp_path / "feather.yaml"
    config_file.write_text(yaml.dump(config, default_flow_style=False))
    return config_file


def _minimal_config(tmp_path: Path, source_path: str | None = None) -> dict:
    """Return a valid minimal config dict."""
    if source_path is None:
        db = tmp_path / "source.duckdb"
        db.touch()
        source_path = str(db)
    return {
        "source": {"type": "duckdb", "path": source_path},
        "destination": {"path": str(tmp_path / "feather_data.duckdb")},
        "tables": [
            {
                "name": "test_table",
                "source_table": "main.test",
                "target_table": "bronze.test_table",
                "strategy": "full",
            }
        ],
    }


class TestConfigParsing:
    def test_valid_config_parses(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        config_file = _write_config(tmp_path, cfg)
        result = load_config(config_file)
        assert result.source.type == "duckdb"
        assert len(result.tables) == 1
        assert result.tables[0].name == "test_table"

    def test_env_var_substitution(self, tmp_path: Path):
        from feather.config import load_config

        os.environ["FEATHER_TEST_PATH"] = str(tmp_path / "source.duckdb")
        (tmp_path / "source.duckdb").touch()
        try:
            cfg = _minimal_config(tmp_path)
            cfg["source"]["path"] = "${FEATHER_TEST_PATH}"
            config_file = _write_config(tmp_path, cfg)
            result = load_config(config_file)
            assert "${" not in str(result.source.path)
            assert "source.duckdb" in str(result.source.path)
        finally:
            del os.environ["FEATHER_TEST_PATH"]

    def test_path_resolution_relative_to_config(self, tmp_path: Path):
        from feather.config import load_config

        subdir = tmp_path / "project"
        subdir.mkdir()
        db = subdir / "source.duckdb"
        db.touch()
        cfg = _minimal_config(tmp_path)
        cfg["source"]["path"] = "./source.duckdb"
        cfg["destination"]["path"] = "./data.duckdb"
        config_file = _write_config(subdir, cfg)
        result = load_config(config_file)
        assert result.source.path == subdir / "source.duckdb"
        assert result.destination.path == subdir / "data.duckdb"

    def test_target_table_defaults_to_silver(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        del cfg["tables"][0]["target_table"]
        config_file = _write_config(tmp_path, cfg)
        result = load_config(config_file)
        assert result.tables[0].target_table == "silver.test_table"

    def test_tables_directory_merge(self, tmp_path: Path):
        from feather.config import load_config

        db = tmp_path / "source.duckdb"
        db.touch()
        cfg = {
            "source": {"type": "duckdb", "path": str(db)},
            "destination": {"path": str(tmp_path / "data.duckdb")},
            "tables": [
                {
                    "name": "inline_table",
                    "source_table": "main.inline",
                    "target_table": "bronze.inline_table",
                    "strategy": "full",
                }
            ],
        }
        config_file = _write_config(tmp_path, cfg)

        tables_dir = tmp_path / "tables"
        tables_dir.mkdir()
        extra = {
            "tables": [
                {
                    "name": "dir_table",
                    "source_table": "main.dir",
                    "target_table": "bronze.dir_table",
                    "strategy": "full",
                }
            ]
        }
        (tables_dir / "extra.yaml").write_text(yaml.dump(extra))

        result = load_config(config_file)
        names = {t.name for t in result.tables}
        assert names == {"inline_table", "dir_table"}

    def test_no_primary_key_does_not_error(self, tmp_path: Path):
        """primary_key validation deferred to Slice 3."""
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        # No primary_key field at all
        config_file = _write_config(tmp_path, cfg)
        result = load_config(config_file)
        assert result.tables[0].primary_key is None


class TestConfigValidation:
    def test_bad_source_type(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        cfg["source"]["type"] = "mongodb"
        config_file = _write_config(tmp_path, cfg)
        with pytest.raises(ValueError, match="source type"):
            load_config(config_file)

    def test_missing_source_path_for_file_source(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        cfg["source"]["path"] = str(tmp_path / "nonexistent.duckdb")
        config_file = _write_config(tmp_path, cfg)
        with pytest.raises(ValueError, match="does not exist"):
            load_config(config_file)

    def test_bad_strategy(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        cfg["tables"][0]["strategy"] = "upsert"
        config_file = _write_config(tmp_path, cfg)
        with pytest.raises(ValueError, match="strategy"):
            load_config(config_file)

    def test_bad_target_schema_prefix(self, tmp_path: Path):
        from feather.config import load_config

        cfg = _minimal_config(tmp_path)
        cfg["tables"][0]["target_table"] = "staging.test_table"
        config_file = _write_config(tmp_path, cfg)
        with pytest.raises(ValueError, match="schema"):
            load_config(config_file)


class TestValidationJson:
    def test_writes_on_success(self, tmp_path: Path):
        from feather.config import load_config, write_validation_json

        cfg = _minimal_config(tmp_path)
        config_file = _write_config(tmp_path, cfg)
        result = load_config(config_file)
        write_validation_json(config_file, result)
        vj = json.loads((tmp_path / "feather_validation.json").read_text())
        assert vj["valid"] is True
        assert vj["errors"] == []
        assert vj["tables_count"] == 1

    def test_writes_on_failure(self, tmp_path: Path):
        from feather.config import write_validation_json

        write_validation_json(
            tmp_path / "feather.yaml", None, errors=["bad source type"]
        )
        vj = json.loads((tmp_path / "feather_validation.json").read_text())
        assert vj["valid"] is False
        assert "bad source type" in vj["errors"]
