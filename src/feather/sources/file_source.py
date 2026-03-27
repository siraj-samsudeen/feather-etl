"""FileSource base class — shared behavior for file-based sources."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path

from feather.sources import ChangeResult


class FileSource:
    """Base for file-based sources (DuckDB, CSV, SQLite, etc.).

    Provides:
    - __init__(path): stores the source path
    - check(): verifies the path exists
    - detect_changes(): two-tier change detection (mtime → MD5 hash)
    - _source_path_for_table(): template method for file path resolution

    Subclasses implement: discover(), extract(), get_schema().
    """

    def __init__(self, path: Path) -> None:
        self.path = path

    def check(self) -> bool:
        return self.path.exists()

    def _source_path_for_table(self, table: str) -> Path:
        """Return the file path to check for change detection.

        Override in subclasses where the mapping differs (e.g., CSV).
        """
        return self.path

    def _build_where_clause(
        self,
        watermark_column: str | None,
        watermark_value: str | None,
        filter: str | None,
    ) -> str:
        """Build a SQL WHERE clause for watermark and/or filter conditions."""
        clauses: list[str] = []
        if watermark_column and watermark_value is not None:
            clauses.append(f"{watermark_column} >= '{watermark_value}'")
        if filter:
            clauses.append(f"({filter})")
        if not clauses:
            return ""
        suffix = " WHERE " + " AND ".join(clauses)
        if watermark_column and watermark_value is not None:
            suffix += f" ORDER BY {watermark_column}"
        return suffix

    def _compute_file_hash(self, file_path: Path) -> str:
        """Compute MD5 hex digest of file contents, reading in 8KB chunks."""
        h = hashlib.md5()
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()

    def detect_changes(
        self, table: str, last_state: dict[str, object] | None = None
    ) -> ChangeResult:
        file_path = self._source_path_for_table(table)
        current_mtime = os.path.getmtime(file_path)

        # Step 3: First run — no prior state, or partial watermark (NULL mtime)
        if last_state is None or last_state.get("last_file_mtime") is None:
            file_hash = self._compute_file_hash(file_path)
            return ChangeResult(
                changed=True,
                reason="first_run",
                metadata={"file_mtime": current_mtime, "file_hash": file_hash},
            )

        # Step 4: Mtime unchanged — skip without hashing
        if current_mtime == last_state.get("last_file_mtime"):
            return ChangeResult(changed=False, reason="unchanged")

        # Step 5: Mtime changed — compute hash to confirm
        file_hash = self._compute_file_hash(file_path)

        # Step 6: Hash identical (touch scenario) — unchanged but update mtime
        if file_hash == last_state.get("last_file_hash"):
            return ChangeResult(
                changed=False,
                reason="unchanged",
                metadata={"file_mtime": current_mtime, "file_hash": file_hash},
            )

        # Step 7: Hash differs — real change
        return ChangeResult(
            changed=True,
            reason="hash_changed",
            metadata={"file_mtime": current_mtime, "file_hash": file_hash},
        )
