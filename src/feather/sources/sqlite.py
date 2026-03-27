"""SQLite source — reads from a SQLite database using DuckDB sqlite_scan()."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from feather.sources import StreamSchema
from feather.sources.file_source import FileSource

class SqliteSource(FileSource):
    """Source that reads tables from a SQLite database file."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """In-memory connection with sqlite_scanner loaded."""
        con = duckdb.connect(":memory:")
        con.execute("INSTALL sqlite_scanner; LOAD sqlite_scanner")
        return con

    def check(self) -> bool:
        if not self.path.exists():
            return False
        try:
            con = self._connect()
            try:
                con.execute(
                    "SELECT name FROM sqlite_scan(?, 'sqlite_master') WHERE type = 'table'",
                    [str(self.path)],
                )
                return True
            finally:
                con.close()
        except Exception:
            return False

    def discover(self) -> list[StreamSchema]:
        con = self._connect()
        try:
            tables = con.execute(
                "SELECT name FROM sqlite_scan(?, 'sqlite_master') WHERE type = 'table' ORDER BY name",
                [str(self.path)],
            ).fetchall()

            schemas: list[StreamSchema] = []
            for (table_name,) in tables:
                cols = con.execute(
                    "SELECT column_name, column_type "
                    "FROM (DESCRIBE SELECT * FROM sqlite_scan(?, ?))",
                    [str(self.path), table_name],
                ).fetchall()
                schemas.append(
                    StreamSchema(
                        name=table_name,
                        columns=[(c[0], c[1]) for c in cols],
                        primary_key=None,
                        supports_incremental=True,
                    )
                )
        finally:
            con.close()
        return schemas

    def extract(
        self,
        table: str,
        columns: list[str] | None = None,
        filter: str | None = None,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
    ) -> pa.Table:
        con = self._connect()
        try:
            query = f"SELECT * FROM sqlite_scan('{self.path}', '{table}')"
            query += self._build_where_clause(watermark_column, watermark_value, filter)
            result = con.execute(query).arrow().read_all()
        finally:
            con.close()
        return result

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        con = self._connect()
        try:
            cols = con.execute(
                "SELECT column_name, column_type "
                "FROM (DESCRIBE SELECT * FROM sqlite_scan(?, ?))",
                [str(self.path), table],
            ).fetchall()
        finally:
            con.close()
        return [(c[0], c[1]) for c in cols]
