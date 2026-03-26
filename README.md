# feather-etl

A lightweight, config-driven Python ETL package for extracting data from multiple sources, transforming with plain SQL in DuckDB, and loading to local or cloud destinations.

## Why feather-etl?

Modern ETL stacks (dlt + dbt/SQLMesh + Dagster) are powerful but introduce a "complexity tax" — hundreds of dependencies, proprietary abstractions, and fragmented configuration. feather-etl replaces the full stack with a single Python package (~1200 LOC, 7 dependencies).

| Heavy Stack | feather-etl |
|------------|-------------|
| dlt (extraction) | `sources/` — Protocol-based connectors |
| dbt/SQLMesh (transforms) | Plain `.sql` files executed in DuckDB |
| Dagster (orchestration) | APScheduler + CLI |
| 3 tools, hundreds of deps | 1 package, 7 deps |

## Design Principles

- **Package-first** — Reusable library, not a standalone app. Client projects import and configure it.
- **File-first** — Core works with file-based sources (CSV, DuckDB, SQLite, Excel, JSON). No servers needed for development or testing.
- **Config-driven** — YAML defines sources, tables, schedules, transforms. Adding a table = editing YAML.
- **Local-first** — Extract and transform in local DuckDB (free). Push only gold tables to cloud (MotherDuck).
- **Testable** — Full pipeline testable end-to-end with file sources. No mocking needed for core tests.

## Supported Sources

| Source | Type | Change Detection | Status |
|--------|------|-----------------|--------|
| CSV | File | mtime + file hash | Planned |
| DuckDB | File | mtime + file hash | Planned |
| SQLite | File | mtime + file hash | Planned |
| Excel | File | mtime + file hash | Planned |
| JSON | File | mtime + file hash | Planned |
| SQL Server | Database | CHECKSUM_AGG + timestamp watermark | Planned |

New sources are added by implementing a simple Python Protocol (~30 lines for file sources, ~80 lines for database sources).

## Architecture

```
Sources (CSV, DuckDB, SQLite, Excel, JSON, SQL Server)
    |
    v
PyArrow Tables (common format)
    |
    v
Local DuckDB
    |-- bronze (raw data + ETL metadata)
    |-- silver views (column renames, filters)
    |-- gold tables (denormalized, business-ready)
    |
    v (optional)
MotherDuck (gold tables only)
    |
    v
Rill Data (dashboards)
```

## Quick Start

```bash
# Install
uv add feather-etl

# Configure
# Edit feather.yaml with your sources and tables

# Initialize
feather setup

# Run
feather run                          # all tables
feather run --table sales_invoice    # single table
feather run --tier hot               # all hot tables

# Monitor
feather status                       # watermarks + last run
feather history                      # run history

# Schedule
feather schedule                     # APScheduler daemon
# OR use OS cron:
# 0 */6 * * * cd /project && uv run feather run --tier hot
```

## Configuration

```yaml
source:
  type: csv                    # or duckdb, sqlite, sqlserver, etc.
  path: ./data/

destination:
  path: ./output.duckdb

schedule_tiers:
  hot: "twice daily"
  cold: weekly

tables:
  - name: sales_invoice
    source_table: sales_invoice.csv
    target_table: bronze.sales_invoice
    strategy: incremental
    timestamp_column: modified_date
    schedule: hot
    primary_key: [invoice_id]
    quality_checks:
      not_null: [invoice_id, customer_code]

  - name: customers
    source_table: customers.csv
    target_table: bronze.customers
    strategy: full
    schedule: cold
    primary_key: [customer_code]
```

## Dependencies

| Package | Purpose |
|---------|---------|
| duckdb | Local processing, file readers |
| pyarrow | Zero-copy data interchange |
| pyyaml | Config parsing |
| typer | CLI |
| pyodbc | SQL Server extraction |
| apscheduler | Built-in scheduling |
| requests | Slack alerting |

## Documentation

- [Product Requirements](docs/prd.md) — Full PRD with 25 features across 5 phases
- [Research](docs/research.md) — Design research synthesis from 8 research agents

## Status

**Pre-alpha** — Architecture designed, implementation starting. See [PRD](docs/prd.md) for the full feature roadmap.

## License

MIT
