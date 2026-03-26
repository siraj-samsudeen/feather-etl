"""feather init — project scaffolding (Slice 1: scaffold only, no wizard)."""

from __future__ import annotations

from pathlib import Path

FEATHER_YAML_TEMPLATE = """\
# feather-etl configuration
# Docs: https://github.com/your-org/feather-etl

source:
  type: duckdb                        # duckdb, sqlite, csv, excel, json, sqlserver
  path: ./source.duckdb               # path to source file (file-based sources)
  # connection_string: "${SQL_SERVER_CONNECTION_STRING}"  # for sqlserver

destination:
  path: ./feather_data.duckdb         # local DuckDB for extracted data

# state:
#   path: ./feather_state.duckdb      # defaults to feather_state.duckdb

# defaults:
#   overlap_window_minutes: 2
#   batch_size: 120000

# schedule_tiers:
#   hot: "twice daily"
#   cold: weekly

tables:
  - name: example_table
    source_table: schema.table_name   # e.g., icube.SALESINVOICE
    target_table: bronze.example_table
    strategy: full                    # full, incremental, append
    # primary_key: [id]
    # timestamp_column: modified_date  # required for incremental/append
"""

PYPROJECT_TEMPLATE = """\
[project]
name = "{name}"
version = "0.1.0"
description = "feather-etl client project"
requires-python = ">=3.10"
dependencies = ["feather-etl>=0.1.0"]
"""

GITIGNORE_TEMPLATE = """\
*.duckdb
*.duckdb.wal
feather_validation.json
.env
__pycache__/
.venv/
"""

ENV_EXAMPLE_TEMPLATE = """\
# Environment variables for feather-etl
# Copy to .env and fill in values

# SQL_SERVER_CONNECTION_STRING=
# MOTHERDUCK_TOKEN=
# ALERT_EMAIL_USER=
# ALERT_EMAIL_PASSWORD=
"""


def scaffold_project(project_path: Path) -> list[str]:
    """Create a new client project directory with template files.

    Returns list of created file paths (relative to project_path).
    """
    project_path.mkdir(parents=True, exist_ok=True)
    created: list[str] = []

    name = project_path.name

    files: dict[str, str] = {
        "feather.yaml": FEATHER_YAML_TEMPLATE,
        "pyproject.toml": PYPROJECT_TEMPLATE.format(name=name),
        ".gitignore": GITIGNORE_TEMPLATE,
        ".env.example": ENV_EXAMPLE_TEMPLATE,
    }
    for rel_path, content in files.items():
        fp = project_path / rel_path
        fp.write_text(content)
        created.append(rel_path)

    dirs = [
        "transforms/silver",
        "transforms/gold",
        "tables",
        "extracts",
    ]
    for d in dirs:
        (project_path / d).mkdir(parents=True, exist_ok=True)
        created.append(d + "/")

    return created
