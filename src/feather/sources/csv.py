"""CSV source — reads CSV files from a directory using DuckDB read_csv()."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa

from feather.sources import StreamSchema
from feather.sources.file_source import FileSource


class CsvSource(FileSource):
    """Source that reads CSV files from a directory."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)

    def _source_path_for_table(self, table: str) -> Path:
        """CSV: each table is a separate file in the directory."""
        return self.path / table

    def check(self) -> bool:
        return self.path.is_dir()

    def discover(self) -> list[StreamSchema]:
        csv_files = sorted(self.path.glob("*.csv"))
        schemas: list[StreamSchema] = []
        con = duckdb.connect(":memory:")
        try:
            for csv_file in csv_files:
                cols = con.execute(
                    "SELECT column_name, column_type "
                    "FROM (DESCRIBE SELECT * FROM read_csv(?))",
                    [str(csv_file)],
                ).fetchall()
                schemas.append(
                    StreamSchema(
                        name=csv_file.name,
                        columns=[(c[0], c[1]) for c in cols],
                        primary_key=None,
                        supports_incremental=False,
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
        con = duckdb.connect(":memory:")
        file_path = str(self.path / table)
        try:
            query = f"SELECT * FROM read_csv('{file_path}')"
            query += self._build_where_clause(watermark_column, watermark_value, filter)
            result = con.execute(query).arrow().read_all()
        finally:
            con.close()
        return result

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        con = duckdb.connect(":memory:")
        file_path = str(self.path / table)
        try:
            cols = con.execute(
                "SELECT column_name, column_type "
                "FROM (DESCRIBE SELECT * FROM read_csv(?))",
                [file_path],
            ).fetchall()
        finally:
            con.close()
        return [(c[0], c[1]) for c in cols]
