# feather-etl

A config-driven Python ETL platform for deploying data pipelines across multiple clients with heterogeneous ERP source systems — SQL Server, SAP B1, SAP S4 HANA, and custom Indian ERPs.

feather-etl is the entire data platform for clients who have none. For clients who already have a data warehouse, it is a lightweight extraction and local transform layer that feeds only clean, curated gold tables to the cloud.

## Why feather-etl?

Enterprise ETL stacks (dlt + dbt/SQLMesh + Dagster) introduce a complexity tax — hundreds of transitive dependencies, proprietary abstractions, steep learning curves — that makes them unsuitable for Indian SMBs who need a working data pipeline, not a data engineering career.

feather-etl replaces the full stack with a single Python package (~1,200 LOC, 7 dependencies), deployable by a small team, configurable in YAML, and understandable by anyone who can read SQL.

| Heavy Stack | feather-etl equivalent |
|------------|----------------------|
| dlt (extraction) | `sources/` — Protocol-based connectors |
| dbt / SQLMesh (transforms) | Plain `.sql` files executed in local DuckDB |
| Dagster / Airflow (orchestration) | APScheduler + CLI |
| 3 tools, hundreds of deps | 1 package, 7 deps |

## Design Principles

- **Multi-client by design** — Each client is an independent `feather.yaml`. Same package, different config. One team deploys across dozens of clients.
- **Config-driven** — YAML defines sources, tables, schedules, transforms. Adding a table means editing YAML, not writing Python.
- **Local-first** — Extract and transform in local DuckDB (free compute). Push only gold tables to MotherDuck (minimal cloud cost).
- **Layers are optional** — Skip bronze for SMB clients who don't need it. Use bronze as a dev cache or compliance audit trail when you do.
- **Testable** — Full pipeline testable end-to-end with file-based sources. No mocking needed for core pipeline tests.
- **Package-first** — Reusable library. Client projects import and configure it. The v2 connector library (`feather-connectors`) will provide canonical silver mappings per ERP system — reusable across all clients on the same source.

## Layer Model

```
Source ERP (SQL Server / SAP B1 / SAP S4 HANA / custom)
    |
    v  column_map applied (zero-copy rename + select)
    |
Local DuckDB
    |
    |-- bronze  (optional)
    |     Raw ERP data, all columns.
    |     Use as dev cache (iterate transforms without hitting source DB)
    |     or compliance audit trail (strategy: append, insert-only).
    |
    |-- silver  (primary working layer)
    |     Canonical names, selected columns, light cleaning.
    |     Always views — lazy, always current, no pipeline step.
    |     Client analysts + LLM agents work here.
    |
    |-- gold    (dashboard layer)
    |     KPIs, aggregations, denormalized tables.
    |     Views by default; materialized tables where performance requires.
    |     Only layer synced to MotherDuck.
    |
    v  (optional sync)
MotherDuck → Rill Data / BI tools
```

## Supported Sources

| Source | Reader | Change Detection | Incremental |
|--------|--------|-----------------|-------------|
| CSV | `read_csv()` | mtime + MD5 hash | — |
| DuckDB file | ATTACH | mtime + MD5 hash | ✓ |
| SQLite | `sqlite_scan()` | mtime + MD5 hash | ✓ |
| Excel `.xlsx` | `read_xlsx()` (excel ext) | mtime + MD5 hash | — |
| Excel `.xls` | openpyxl fallback | mtime + MD5 hash | — |
| JSON | `read_json()` | mtime + MD5 hash | — |
| SQL Server | pyodbc → PyArrow | CHECKSUM_AGG + COUNT(*) | ✓ |

New sources: implement the `Source` Protocol (~30 lines for file sources, ~80 lines for database sources) and register in the source registry.

## Load Strategies

| Strategy | Behavior | When to use |
|----------|----------|-------------|
| `full` | Atomic swap (drop + recreate) | Small reference tables, no history needed |
| `incremental` | Partition overwrite (delete + insert on watermark window) | Large transactional tables |
| `append` | Insert only, never delete | Audit trail, compliance, full history |

All three strategies are idempotent — safe to re-run after partial failures.

## This Repo vs Client Projects

**feather-etl** (this repo) is a Python package only — no client config, no client data. Install it as a dependency:

```bash
uv add feather-etl
```

Each client lives in its own GitHub repository, scaffolded with `feather init`. The operator (or an LLM agent guided by a human) runs the wizard once per client:

```
feather init
  → asks: client name, source type, connection details
  → connects to source, discovers available tables
  → asks: which tables to include
  → generates: feather.yaml, silver transform stubs, .gitignore, pyproject.toml
  → runs: feather validate to confirm everything is wired correctly
  → prints: next steps
```

For LLM agent-driven onboarding, the wizard runs fully non-interactive:

```bash
feather init \
  --non-interactive \
  --name client-abc \
  --source sqlserver \
  --connection-string "${SQL_SERVER_CONNECTION_STRING}" \
  --json   # machine-readable output
```

## CLI

```bash
# New client setup
feather init                           # interactive wizard — scaffold + discover + validate
feather init --non-interactive --json  # agent-driven, machine-readable output

# Day-to-day operations
feather setup                          # init state DB, create schemas, apply transforms
feather run                            # run all tables
feather run --table sales_invoice      # run one table
feather run --tier hot                 # run all hot-tier tables
feather status                         # watermarks + last run status per table
feather history                        # run history
feather history --table sales_invoice  # run history for one table
feather schedule                       # start APScheduler daemon

# Diagnostic
feather validate                       # validate config, resolve paths — no execution
feather discover                       # list all tables in the configured source
```

All commands accept `--config PATH` (default: `feather.yaml`) and `--json` for machine-readable NDJSON output.

## Client Project Layout

`feather init` generates this structure (one repo per client):

```
client-abc/                         # separate GitHub repo per client
├── .gitignore                      # excludes *.duckdb, .env
├── pyproject.toml                  # depends on feather-etl
├── .env.example                    # credential placeholders
├── feather.yaml                    # source, destination, sync, schedules, alerts
├── tables/                         # optional — split by domain
│   ├── sales.yaml
│   └── inventory.yaml
├── transforms/
│   ├── silver/                     # canonical mapping views
│   │   └── sales_invoice.sql       # -- depends_on: (none)
│   └── gold/                       # client-specific output
│       └── sales_summary.sql       # -- materialized: true
└── extracts/                       # optional — custom source SELECT queries
    └── sales_invoice.sql
```

`feather_state.duckdb` and `feather_data.duckdb` are created at runtime, gitignored.

## Configuration

### SMB client — silver-direct (no bronze)

Small clients land data directly into silver with column selection at extraction time. No bronze layer needed.

```yaml
source:
  type: sqlserver
  connection_string: "${SQL_SERVER_CONNECTION_STRING}"

destination:
  path: ./feather_data.duckdb

sync:
  type: motherduck
  token: "${MOTHERDUCK_TOKEN}"
  database: "client_analytics"

alerts:
  smtp_host: "smtp.gmail.com"
  smtp_port: 587
  smtp_user: "${ALERT_EMAIL_USER}"
  smtp_password: "${ALERT_EMAIL_PASSWORD}"
  alert_to: "operator@example.com"

schedule_tiers:
  hot: "twice daily"
  cold: weekly

tables:
  - name: sales_invoice
    source_table: dbo.SALESINVOICE
    target_table: silver.sales_invoice
    strategy: incremental
    timestamp_column: ModifiedDate
    schedule: hot
    primary_key: [ID]
    column_map:                       # select 8 of 120 columns, rename to canonical
      ID: invoice_id
      SI_NO: invoice_no
      Custome_Code: customer_code
      NetAmount: net_amount
      ModifiedDate: modified_date
    quality_checks:
      not_null: [invoice_id, customer_code]
```

### Enterprise client — bronze + append (compliance)

```yaml
tables:
  - name: sales_invoice
    source_table: dbo.SALESINVOICE
    target_table: bronze.sales_invoice   # raw, all columns
    strategy: append                     # insert-only, full audit trail
    timestamp_column: ModifiedDate
    schedule: hot
    primary_key: [ID]
```

Silver views over bronze live in `transforms/silver/sales_invoice.sql`:

```sql
-- depends_on: (none — reads from bronze directly)
CREATE OR REPLACE VIEW silver.sales_invoice AS
SELECT
    ID           AS invoice_id,
    SI_NO        AS invoice_no,
    Custome_Code AS customer_code,
    NetAmount    AS net_amount,
    ModifiedDate AS modified_date,
    _etl_loaded_at,
    _etl_run_id
FROM bronze.sales_invoice
```

### Table splitting (large deployments)

```
client-project/
├── feather.yaml
└── tables/
    ├── sales.yaml       # 8 sales tables
    ├── inventory.yaml   # 6 inventory tables
    └── hr.yaml          # 4 HR tables
```

feather-etl auto-discovers and merges all `.yaml` files in the `tables/` directory.

## Transform Dependencies

Declare dependencies with a SQL comment — the system builds the execution order automatically:

```sql
-- depends_on: silver.sales_invoice
-- depends_on: silver.customer_master
-- materialized: true
CREATE OR REPLACE TABLE gold.sales_summary AS
SELECT ...
FROM silver.sales_invoice si
JOIN silver.customer_master cm ON si.customer_code = cm.customer_code
```

## Dependencies

| Package | Purpose |
|---------|---------|
| duckdb | Local processing, file readers, MotherDuck sync |
| pyarrow | Zero-copy data interchange |
| pyyaml | Config parsing |
| typer | CLI |
| pyodbc | SQL Server / SAP B1 extraction |
| apscheduler | Built-in scheduling |
| openpyxl | Excel `.xls` fallback (`.xlsx` handled natively by DuckDB) |

Alerting uses Python stdlib `smtplib` — no extra dependency.

## Alerting

SMTP email via Python stdlib — works with Gmail, any corporate SMTP relay, or transactional email services. No Slack account, no webhook setup.

| Severity | Trigger |
|----------|---------|
| `[CRITICAL]` | Pipeline failure, load error, type cast quarantine |
| `[WARNING]` | DQ check failure, schema drift |
| `[INFO]` | Schema drift (informational) |

No-op if `alerts` section is not configured.

## Observability

Every run is recorded in `feather_state.duckdb`:

```sql
-- What happened in the last 24 hours?
SELECT table_name, status, rows_extracted, rows_loaded, duration_sec
FROM _runs
WHERE started_at > now() - INTERVAL 1 DAY
ORDER BY started_at DESC;

-- Any DQ failures?
SELECT * FROM _dq_results WHERE result = 'fail';

-- Schema drift detected?
SELECT table_name, schema_changes FROM _runs
WHERE schema_changes IS NOT NULL;
```

## Documentation

- [PRD v1.5](docs/prd.md) — Full requirements, EARS spec, design decisions
- [Research](docs/research.md) — Design research synthesis from 7 research agents

## Roadmap

**v1 (current):** Core pipeline — extraction, loading (full/incremental/append), silver/gold transforms, scheduling, DQ checks, schema drift detection, SMTP alerts, state management.

**v2:** `feather-connectors` — canonical silver transform library for SAP B1, SAP S4 HANA, and common Indian ERPs. Reuse silver mappings across all clients on the same source system.

**v3+:** LLM agent interface for client analysts — last-mile dashboard customisation guided by AI, working against the canonical silver layer.

## Status

**Pre-alpha** — Architecture complete, implementation starting. See [PRD](docs/prd.md) for the full feature roadmap (25 features across 5 phases).

## License

MIT
