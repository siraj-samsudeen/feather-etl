"""Destination connectors for feather-etl."""

from __future__ import annotations

from typing import Protocol

import pyarrow as pa


class Destination(Protocol):
    """Target for loaded data."""

    def load_full(self, table: str, data: pa.Table, run_id: str) -> int: ...

    def load_incremental(
        self, table: str, data: pa.Table, run_id: str, timestamp_column: str
    ) -> int: ...

    def execute_sql(self, sql: str) -> None: ...

    def sync_to_remote(self, tables: list[str], remote_config: dict[str, object]) -> None: ...
