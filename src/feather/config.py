"""Configuration parsing and validation for feather-etl."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

VALID_SOURCE_TYPES = {"duckdb", "sqlite", "csv", "excel", "json", "sqlserver"}
FILE_SOURCE_TYPES = {"duckdb", "sqlite", "csv", "excel", "json"}
VALID_STRATEGIES = {"full", "incremental", "append"}
VALID_SCHEMA_PREFIXES = {"bronze", "silver", "gold"}


@dataclass
class SourceConfig:
    type: str
    path: Path | None = None
    connection_string: str | None = None


@dataclass
class DestinationConfig:
    path: Path


@dataclass
class DefaultsConfig:
    overlap_window_minutes: int = 2
    batch_size: int = 120_000


@dataclass
class TableConfig:
    name: str
    source_table: str
    strategy: str
    target_table: str = ""
    primary_key: list[str] | None = None
    timestamp_column: str | None = None
    checksum_columns: list[str] | None = None
    filter: str | None = None
    quality_checks: dict | None = None
    column_map: dict[str, str] | None = None
    schedule: str | None = None


@dataclass
class FeatherConfig:
    source: SourceConfig
    destination: DestinationConfig
    tables: list[TableConfig]
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    config_dir: Path = field(default_factory=lambda: Path("."))


def _resolve_env_vars(text: str) -> str:
    return os.path.expandvars(text)


def _resolve_yaml_env_vars(data: dict | list | str) -> dict | list | str:
    """Recursively resolve ${VAR} in all string values."""
    if isinstance(data, dict):
        return {k: _resolve_yaml_env_vars(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_resolve_yaml_env_vars(item) for item in data]
    if isinstance(data, str):
        return _resolve_env_vars(data)
    return data


def _resolve_path(config_dir: Path, raw: str) -> Path:
    """Resolve a path relative to config file directory, not CWD."""
    p = Path(raw)
    if p.is_absolute():
        return p
    return (config_dir / p).resolve()


def _parse_tables(raw_tables: list[dict], config: dict) -> list[TableConfig]:
    tables = []
    for t in raw_tables:
        target = t.get("target_table", f"silver.{t['name']}")
        tables.append(
            TableConfig(
                name=t["name"],
                source_table=t["source_table"],
                strategy=t["strategy"],
                target_table=target,
                primary_key=t.get("primary_key"),
                timestamp_column=t.get("timestamp_column"),
                checksum_columns=t.get("checksum_columns"),
                filter=t.get("filter"),
                quality_checks=t.get("quality_checks"),
                column_map=t.get("column_map"),
                schedule=t.get("schedule"),
            )
        )
    return tables


def _merge_tables_dir(config_dir: Path, tables: list[dict]) -> list[dict]:
    """Merge table definitions from tables/ directory alongside feather.yaml."""
    tables_dir = config_dir / "tables"
    if not tables_dir.is_dir():
        return tables

    inline_names = {t["name"] for t in tables}
    for yaml_file in sorted(tables_dir.glob("*.yaml")):
        data = yaml.safe_load(yaml_file.read_text())
        if data and "tables" in data:
            for t in data["tables"]:
                if t["name"] not in inline_names:
                    tables.append(t)
    return tables


def _validate(config: FeatherConfig) -> list[str]:
    """Validate config, return list of error messages."""
    errors: list[str] = []

    if config.source.type not in VALID_SOURCE_TYPES:
        errors.append(
            f"Unsupported source type '{config.source.type}'. "
            f"Valid: {sorted(VALID_SOURCE_TYPES)}"
        )

    if config.source.type in FILE_SOURCE_TYPES and config.source.path:
        if not config.source.path.exists():
            errors.append(
                f"Source path does not exist: {config.source.path}"
            )

    for table in config.tables:
        if table.strategy not in VALID_STRATEGIES:
            errors.append(
                f"Table '{table.name}': invalid strategy '{table.strategy}'. "
                f"Valid: {sorted(VALID_STRATEGIES)}"
            )

        if "." in table.target_table:
            schema_prefix = table.target_table.split(".")[0]
            if schema_prefix not in VALID_SCHEMA_PREFIXES:
                errors.append(
                    f"Table '{table.name}': target_table schema '{schema_prefix}' "
                    f"must be one of {sorted(VALID_SCHEMA_PREFIXES)}"
                )

    return errors


def load_config(config_path: Path) -> FeatherConfig:
    """Load and validate feather.yaml, raising ValueError on invalid config."""
    config_dir = config_path.parent.resolve()
    raw = yaml.safe_load(config_path.read_text())
    raw = _resolve_yaml_env_vars(raw)

    source_raw = raw["source"]
    source = SourceConfig(
        type=source_raw["type"],
        path=_resolve_path(config_dir, source_raw["path"]) if "path" in source_raw else None,
        connection_string=source_raw.get("connection_string"),
    )

    dest = DestinationConfig(
        path=_resolve_path(config_dir, raw["destination"]["path"]),
    )

    defaults_raw = raw.get("defaults", {})
    defaults = DefaultsConfig(
        overlap_window_minutes=defaults_raw.get("overlap_window_minutes", 2),
        batch_size=defaults_raw.get("batch_size", 120_000),
    )

    raw_tables = raw.get("tables", [])
    raw_tables = _merge_tables_dir(config_dir, raw_tables)
    tables = _parse_tables(raw_tables, raw)

    config = FeatherConfig(
        source=source,
        destination=dest,
        tables=tables,
        defaults=defaults,
        config_dir=config_dir,
    )

    errors = _validate(config)
    if errors:
        raise ValueError("; ".join(errors))

    return config


def write_validation_json(
    config_path: Path,
    config: FeatherConfig | None,
    errors: list[str] | None = None,
) -> Path:
    """Write feather_validation.json alongside feather.yaml."""
    validation_path = config_path.parent / "feather_validation.json"
    if errors is None:
        errors = []

    result = {
        "valid": config is not None and len(errors) == 0,
        "errors": errors,
        "tables_count": len(config.tables) if config else 0,
        "resolved_paths": {
            "source": str(config.source.path) if config and config.source.path else None,
            "destination": str(config.destination.path) if config else None,
            "config_dir": str(config.config_dir) if config else None,
        }
        if config
        else {},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    validation_path.write_text(json.dumps(result, indent=2))
    return validation_path
