"""Tests for SQL Server source — all pyodbc calls mocked."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from feather.sources.sqlserver import SqlServerSource

FAKE_CONN_STR = "DRIVER={ODBC Driver 18};SERVER=fake;DATABASE=testdb"


@pytest.fixture
def source() -> SqlServerSource:
    return SqlServerSource(connection_string=FAKE_CONN_STR, batch_size=100)


# --- extract ---


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_full_extract_loads_rows(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Full extract returns a PyArrow table with correct columns and rows."""
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("id", int, None, None, None, None, None),
        ("name", str, None, None, None, None, None),
        ("amount", float, None, None, None, None, None),
    ]
    mock_cursor.fetchmany.side_effect = [
        [(1, "Alice", 100.0), (2, "Bob", 200.0)],
        [],  # end of results
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    result = source.extract("dbo.orders")

    assert isinstance(result, pa.Table)
    assert result.num_rows == 2
    assert result.column_names == ["id", "name", "amount"]
    assert result.column("id").to_pylist() == [1, 2]
    assert result.column("name").to_pylist() == ["Alice", "Bob"]
    # Verify SET NOCOUNT ON was called
    mock_cursor.execute.assert_any_call("SET NOCOUNT ON")


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_incremental_extract_with_watermark(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Incremental extract builds WHERE clause with watermark."""
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("id", int, None, None, None, None, None),
        ("updated_at", str, None, None, None, None, None),
    ]
    mock_cursor.fetchmany.side_effect = [
        [(3, "2026-03-27")],
        [],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    result = source.extract(
        "dbo.orders",
        watermark_column="updated_at",
        watermark_value="2026-03-26",
    )

    assert result.num_rows == 1
    # Verify the query included watermark WHERE clause
    calls = [str(c) for c in mock_cursor.execute.call_args_list]
    query_call = [c for c in calls if "updated_at" in c]
    assert len(query_call) == 1
    assert "updated_at > '2026-03-26'" in query_call[0]


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_extract_with_filter_and_columns(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Extract respects column list and filter clause."""
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("id", int, None, None, None, None, None),
        ("name", str, None, None, None, None, None),
    ]
    mock_cursor.fetchmany.side_effect = [
        [(1, "Alice")],
        [],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    result = source.extract(
        "dbo.orders",
        columns=["id", "name"],
        filter="status = 'active'",
    )

    assert result.num_rows == 1
    calls = [str(c) for c in mock_cursor.execute.call_args_list]
    query_call = [c for c in calls if "id, name" in c]
    assert len(query_call) == 1
    assert "status = 'active'" in query_call[0]


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_empty_table(mock_pyodbc: MagicMock, source: SqlServerSource) -> None:
    """Extract on empty table returns empty table with correct schema."""
    mock_cursor = MagicMock()
    mock_cursor.description = [
        ("id", int, None, None, None, None, None),
        ("name", str, None, None, None, None, None),
    ]
    mock_cursor.fetchmany.return_value = []  # no rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    result = source.extract("dbo.empty_table")

    assert isinstance(result, pa.Table)
    assert result.num_rows == 0
    assert result.column_names == ["id", "name"]


# --- change detection ---


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_change_detection_first_run(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """First run (no last_state) returns changed=True with reason=first_run."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (12345, 100)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    result = source.detect_changes("dbo.orders", last_state=None)

    assert result.changed is True
    assert result.reason == "first_run"
    assert result.metadata["checksum"] == 12345
    assert result.metadata["row_count"] == 100


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_change_detection_skip(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Unchanged checksum+row_count returns changed=False."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (12345, 100)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    last_state = {"last_checksum": 12345, "last_row_count": 100}
    result = source.detect_changes("dbo.orders", last_state=last_state)

    assert result.changed is False
    assert result.reason == "unchanged"


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_change_detection_changed(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Changed checksum returns changed=True with new metadata."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (99999, 150)
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    last_state = {"last_checksum": 12345, "last_row_count": 100}
    result = source.detect_changes("dbo.orders", last_state=last_state)

    assert result.changed is True
    assert result.reason == "checksum_changed"
    assert result.metadata["checksum"] == 99999
    assert result.metadata["row_count"] == 150


@pytest.mark.unit
def test_sqlserver_change_detection_incremental_always_changed(
    source: SqlServerSource,
) -> None:
    """Incremental strategy always returns changed=True (watermark handles skip)."""
    last_state = {
        "strategy": "incremental",
        "last_checksum": 12345,
        "last_row_count": 100,
    }
    result = source.detect_changes("dbo.orders", last_state=last_state)

    assert result.changed is True
    assert result.reason == "incremental"


# --- discover ---


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_discover_returns_schemas(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """Discover returns StreamSchema list from INFORMATION_SCHEMA."""
    mock_cursor = MagicMock()
    # First call: list tables
    # Second call: columns for first table
    # Third call: columns for second table
    mock_cursor.fetchall.side_effect = [
        [("dbo", "orders"), ("dbo", "customers")],
        [("id", "int"), ("total", "decimal")],
        [("id", "int"), ("name", "nvarchar")],
    ]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    schemas = source.discover()

    assert len(schemas) == 2
    assert schemas[0].name == "dbo.orders"
    assert schemas[0].columns == [("id", "int"), ("total", "decimal")]
    assert schemas[0].supports_incremental is True
    assert schemas[1].name == "dbo.customers"
    assert schemas[1].columns == [("id", "int"), ("name", "nvarchar")]


# --- get_schema ---


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_get_schema(mock_pyodbc: MagicMock, source: SqlServerSource) -> None:
    """get_schema returns column list for a single table."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("id", "int"), ("name", "nvarchar")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    cols = source.get_schema("dbo.orders")

    assert cols == [("id", "int"), ("name", "nvarchar")]


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_get_schema_unqualified(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """get_schema with unqualified name defaults to dbo schema."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("id", "int")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_pyodbc.connect.return_value = mock_conn

    cols = source.get_schema("orders")

    assert cols == [("id", "int")]
    # Verify it used 'dbo' as schema
    execute_calls = mock_cursor.execute.call_args_list
    assert execute_calls[0][0][1] == ["dbo", "orders"]


# --- connection ---


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_check_success(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """check() returns True on successful connection."""
    mock_conn = MagicMock()
    mock_pyodbc.connect.return_value = mock_conn

    assert source.check() is True
    mock_conn.close.assert_called_once()


@pytest.mark.unit
@patch("feather.sources.sqlserver.pyodbc")
def test_sqlserver_connection_failure(
    mock_pyodbc: MagicMock, source: SqlServerSource
) -> None:
    """check() returns False when connection fails."""
    mock_pyodbc.Error = Exception
    mock_pyodbc.connect.side_effect = Exception("Connection refused")

    assert source.check() is False


# --- registry + config integration ---


@pytest.mark.unit
def test_registry_creates_sqlserver_source() -> None:
    """Registry creates SqlServerSource when type is 'sqlserver'."""
    from feather.sources.registry import SOURCE_REGISTRY

    assert "sqlserver" in SOURCE_REGISTRY
    assert SOURCE_REGISTRY["sqlserver"] is SqlServerSource


@pytest.mark.unit
def test_config_validation_sqlserver_requires_connection_string(tmp_path) -> None:
    """Config validation rejects sqlserver without connection_string."""
    config_yaml = tmp_path / "feather.yaml"
    config_yaml.write_text(
        "source:\n"
        "  type: sqlserver\n"
        "destination:\n"
        f"  path: {tmp_path}/dest.duckdb\n"
        "tables:\n"
        "  - name: orders\n"
        "    source_table: dbo.orders\n"
        "    strategy: full\n"
        "    target_table: bronze.orders\n"
    )
    from feather.config import load_config

    with pytest.raises(ValueError, match="connection_string"):
        load_config(config_yaml)


# --- DatabaseSource._build_where_clause ---


@pytest.mark.unit
def test_build_where_clause_empty(source: SqlServerSource) -> None:
    """No filter or watermark returns empty string."""
    assert source._build_where_clause() == ""


@pytest.mark.unit
def test_build_where_clause_filter_only(source: SqlServerSource) -> None:
    """Filter alone produces WHERE with filter."""
    result = source._build_where_clause(filter="status = 'active'")
    assert result == " WHERE (status = 'active')"


@pytest.mark.unit
def test_build_where_clause_watermark_only(source: SqlServerSource) -> None:
    """Watermark alone produces WHERE with watermark condition."""
    result = source._build_where_clause(
        watermark_column="updated_at", watermark_value="2026-01-01"
    )
    assert result == " WHERE updated_at > '2026-01-01'"


@pytest.mark.unit
def test_build_where_clause_both(source: SqlServerSource) -> None:
    """Filter + watermark combines with AND."""
    result = source._build_where_clause(
        filter="status = 'active'",
        watermark_column="updated_at",
        watermark_value="2026-01-01",
    )
    assert result == " WHERE (status = 'active') AND updated_at > '2026-01-01'"
