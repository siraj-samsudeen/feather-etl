"""Source connectors for feather-etl."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import pyarrow as pa


@dataclass
class StreamSchema:
    """Describes a discoverable table/stream."""

    name: str
    columns: list[tuple[str, str]]
    primary_key: list[str] | None
    supports_incremental: bool


@dataclass
class ChangeResult:
    """Result of change detection check."""

    changed: bool
    reason: str  # "mtime_changed", "hash_changed", "first_run", "unchanged"
    metadata: dict[str, object] = field(default_factory=dict)


class Source(Protocol):
    """Any class with these methods is a valid feather-etl source."""

    def check(self) -> bool: ...

    def discover(self) -> list[StreamSchema]: ...

    def extract(
        self,
        table: str,
        columns: list[str] | None = None,
        filter: str | None = None,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
    ) -> pa.Table: ...

    def detect_changes(
        self, table: str, last_state: dict[str, object] | None = None
    ) -> ChangeResult: ...

    def get_schema(self, table: str) -> list[tuple[str, str]]: ...
