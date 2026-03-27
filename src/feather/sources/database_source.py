"""DatabaseSource base class — shared behavior for database sources."""

from __future__ import annotations


class DatabaseSource:
    """Base for database sources (SQL Server, etc.).

    Provides:
    - __init__(connection_string): stores the connection string
    - _build_where_clause(): constructs WHERE from filter + watermark

    Subclasses implement: check(), discover(), extract(), detect_changes(), get_schema().
    """

    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def _build_where_clause(
        self,
        filter: str | None = None,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
    ) -> str:
        """Build a WHERE clause from optional filter and watermark."""
        parts: list[str] = []
        if filter:
            parts.append(f"({filter})")
        if watermark_column and watermark_value:
            parts.append(f"{watermark_column} > '{watermark_value}'")
        if not parts:
            return ""
        return " WHERE " + " AND ".join(parts)
