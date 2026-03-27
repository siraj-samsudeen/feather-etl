"""DuckDB file source — reads from a .duckdb file via ATTACH."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from feather.sources import StreamSchema
from feather.sources.file_source import FileSource


class DuckDBFileSource(FileSource):
    """Source that reads tables from a DuckDB file using ATTACH."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)

    def _connect_direct(self) -> duckdb.DuckDBPyConnection:
        """Connect directly to the source DB (read-only) for metadata queries."""
        return duckdb.connect(str(self.path), read_only=True)

    def _connect_attached(self) -> duckdb.DuckDBPyConnection:
        """Connect in-memory with source ATTACH'd for data queries."""
        con = duckdb.connect(":memory:")
        con.execute(f"ATTACH '{self.path}' AS source_db (READ_ONLY)")
        return con

    def check(self) -> bool:
        if not self.path.exists():
            return False
        try:
            con = self._connect_direct()
            con.close()
            return True
        except Exception:
            return False

    def discover(self) -> list[StreamSchema]:
        con = self._connect_direct()
        rows = con.execute(
            "SELECT table_schema, table_name "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
            "ORDER BY table_schema, table_name"
        ).fetchall()

        schemas: list[StreamSchema] = []
        for schema_name, table_name in rows:
            qualified = f"{schema_name}.{table_name}"
            cols = con.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? "
                "ORDER BY ordinal_position",
                [schema_name, table_name],
            ).fetchall()
            schemas.append(
                StreamSchema(
                    name=qualified,
                    columns=[(c[0], c[1]) for c in cols],
                    primary_key=None,
                    supports_incremental=False,
                )
            )
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
        con = self._connect_attached()
        schema, tbl = table.split(".", 1)
        query = f'SELECT * FROM source_db."{schema}"."{tbl}"'
        query += self._build_where_clause(watermark_column, watermark_value, filter)
        result = con.execute(query).arrow().read_all()
        con.close()
        return result

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        con = self._connect_direct()
        parts = table.split(".")
        schema_name, table_name = parts[0], parts[1]
        cols = con.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema_name, table_name],
        ).fetchall()
        con.close()
        return [(c[0], c[1]) for c in cols]
