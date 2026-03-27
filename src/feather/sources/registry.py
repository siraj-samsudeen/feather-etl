"""Source registry — maps type names to source classes."""

from __future__ import annotations

from typing import Any

from feather.config import FILE_SOURCE_TYPES, SourceConfig
from feather.sources import Source
from feather.sources.csv import CsvSource
from feather.sources.duckdb_file import DuckDBFileSource
from feather.sources.sqlite import SqliteSource
from feather.sources.sqlserver import SqlServerSource

SOURCE_REGISTRY: dict[str, type[Any]] = {
    "duckdb": DuckDBFileSource,
    "csv": CsvSource,
    "sqlite": SqliteSource,
    "sqlserver": SqlServerSource,
}


def create_source(config: SourceConfig) -> Source:
    """Factory: resolve source type -> instantiate."""
    if config.type not in SOURCE_REGISTRY:
        raise ValueError(
            f'Source type "{config.type}" is not implemented. '
            f"Registered: {list(SOURCE_REGISTRY)}"
        )
    cls = SOURCE_REGISTRY[config.type]
    if config.type in FILE_SOURCE_TYPES:
        return cls(path=config.path)
    return cls(connection_string=config.connection_string)
