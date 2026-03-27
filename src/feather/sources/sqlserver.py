"""SQL Server source — reads from SQL Server via pyodbc."""

from __future__ import annotations

import pyarrow as pa
import pyodbc

from feather.sources import ChangeResult, StreamSchema
from feather.sources.database_source import DatabaseSource

# pyodbc type code → PyArrow type mapping
# SQL Server types reported via cursor.description type_code
_PYODBC_TYPE_MAP: dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}

# SQL Server INFORMATION_SCHEMA type name → PyArrow type
_SQL_TYPE_MAP: dict[str, pa.DataType] = {
    "int": pa.int64(),
    "bigint": pa.int64(),
    "smallint": pa.int16(),
    "tinyint": pa.int8(),
    "bit": pa.bool_(),
    "float": pa.float64(),
    "real": pa.float64(),
    "decimal": pa.float64(),
    "numeric": pa.float64(),
    "money": pa.float64(),
    "smallmoney": pa.float64(),
    "varchar": pa.string(),
    "nvarchar": pa.string(),
    "char": pa.string(),
    "nchar": pa.string(),
    "text": pa.string(),
    "ntext": pa.string(),
    "datetime": pa.timestamp("us"),
    "datetime2": pa.timestamp("us"),
    "smalldatetime": pa.timestamp("us"),
    "date": pa.date32(),
    "time": pa.time64("us"),
    "uniqueidentifier": pa.string(),
    "binary": pa.binary(),
    "varbinary": pa.binary(),
    "image": pa.binary(),
    "xml": pa.string(),
}


def _pyodbc_type_to_arrow(type_code: type) -> pa.DataType:
    """Map a pyodbc cursor.description type_code to a PyArrow type."""
    return _PYODBC_TYPE_MAP.get(type_code, pa.string())


class SqlServerSource(DatabaseSource):
    """Source that reads tables from SQL Server via pyodbc."""

    def __init__(self, connection_string: str, batch_size: int = 120_000) -> None:
        super().__init__(connection_string)
        self.batch_size = batch_size

    def check(self) -> bool:
        try:
            con = pyodbc.connect(self.connection_string, timeout=10)
            con.close()
            return True
        except pyodbc.Error:
            return False

    def discover(self) -> list[StreamSchema]:
        con = pyodbc.connect(self.connection_string)
        cursor = con.cursor()

        # Get all user tables
        cursor.execute(
            "SELECT TABLE_SCHEMA, TABLE_NAME "
            "FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
        )
        tables = cursor.fetchall()

        schemas: list[StreamSchema] = []
        for schema_name, table_name in tables:
            qualified = f"{schema_name}.{table_name}"
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE "
                "FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
                "ORDER BY ORDINAL_POSITION",
                [schema_name, table_name],
            )
            cols = cursor.fetchall()
            schemas.append(
                StreamSchema(
                    name=qualified,
                    columns=[(c[0], c[1]) for c in cols],
                    primary_key=None,
                    supports_incremental=True,
                )
            )

        cursor.close()
        con.close()
        return schemas

    def get_schema(self, table: str) -> list[tuple[str, str]]:
        con = pyodbc.connect(self.connection_string)
        cursor = con.cursor()

        parts = table.split(".")
        if len(parts) == 2:
            schema_name, table_name = parts
        else:
            schema_name, table_name = "dbo", parts[0]

        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
            "ORDER BY ORDINAL_POSITION",
            [schema_name, table_name],
        )
        cols = cursor.fetchall()
        cursor.close()
        con.close()
        return [(c[0], c[1]) for c in cols]

    def extract(
        self,
        table: str,
        columns: list[str] | None = None,
        filter: str | None = None,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
    ) -> pa.Table:
        con = pyodbc.connect(self.connection_string)
        cursor = con.cursor()
        cursor.execute("SET NOCOUNT ON")

        col_clause = ", ".join(columns) if columns else "*"
        where = self._build_where_clause(filter, watermark_column, watermark_value)
        query = f"SELECT {col_clause} FROM {table}{where}"

        cursor.execute(query)

        # Build schema from cursor.description
        if cursor.description is None:
            cursor.close()
            con.close()
            return pa.table({})

        col_names = [desc[0] for desc in cursor.description]
        col_types = [_pyodbc_type_to_arrow(desc[1]) for desc in cursor.description]
        arrow_schema = pa.schema(
            [pa.field(name, typ) for name, typ in zip(col_names, col_types)]
        )

        # Chunked fetch → PyArrow batches
        batches: list[pa.RecordBatch] = []
        while True:
            rows = cursor.fetchmany(self.batch_size)
            if not rows:
                break
            # Convert rows to column-oriented dict
            col_data: dict[str, list] = {name: [] for name in col_names}
            for row in rows:
                for i, name in enumerate(col_names):
                    col_data[name].append(row[i])
            batch = pa.RecordBatch.from_pydict(col_data, schema=arrow_schema)
            batches.append(batch)

        cursor.close()
        con.close()

        if not batches:
            return pa.table(
                {
                    name: pa.array([], type=typ)
                    for name, typ in zip(col_names, col_types)
                }
            )

        return pa.Table.from_batches(batches, schema=arrow_schema)

    def detect_changes(
        self, table: str, last_state: dict[str, object] | None = None
    ) -> ChangeResult:
        # For incremental strategy, always return changed — watermark handles skip
        # For full strategy, use CHECKSUM_AGG
        if last_state is not None and last_state.get("strategy") == "incremental":
            return ChangeResult(changed=True, reason="incremental")

        try:
            con = pyodbc.connect(self.connection_string)
            cursor = con.cursor()
            cursor.execute(
                f"SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) AS checksum, "
                f"COUNT(*) AS row_count FROM {table}"
            )
            row = cursor.fetchone()
            cursor.close()
            con.close()
        except pyodbc.Error:
            # If we can't compute checksum, assume changed
            return ChangeResult(changed=True, reason="checksum_error")

        if row is None:
            return ChangeResult(changed=True, reason="first_run")

        current_checksum = row[0]
        current_row_count = row[1]

        # First run — no prior state
        if last_state is None or last_state.get("last_checksum") is None:
            return ChangeResult(
                changed=True,
                reason="first_run",
                metadata={
                    "checksum": current_checksum,
                    "row_count": current_row_count,
                },
            )

        # Compare against previous
        if current_checksum == last_state.get(
            "last_checksum"
        ) and current_row_count == last_state.get("last_row_count"):
            return ChangeResult(changed=False, reason="unchanged")

        return ChangeResult(
            changed=True,
            reason="checksum_changed",
            metadata={
                "checksum": current_checksum,
                "row_count": current_row_count,
            },
        )
