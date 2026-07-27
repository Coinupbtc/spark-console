"""Backward-compatible CSV schema migration and append helpers."""

from __future__ import annotations

import csv
import fcntl
import os
from pathlib import Path


SCHEMA_VERSION = 2


def _migrate_header(path: Path, headers: list[str]) -> None:
    """Rewrite only the header shape while preserving every historical row."""
    if not path.exists() or path.stat().st_size == 0:
        return

    with path.open(newline="") as source:
        rows = list(csv.reader(source))
    if not rows or rows[0] == headers:
        return

    old_headers = rows[0]
    temporary = path.with_suffix(path.suffix + ".schema-v2.tmp")
    with temporary.open("w", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=headers)
        writer.writeheader()
        for values in rows[1:]:
            # Short legacy rows remain valid; new columns are intentionally blank.
            legacy = dict(zip(old_headers, values))
            writer.writerow({header: legacy.get(header, "") for header in headers})
        target.flush()
        os.fsync(target.fileno())
    os.replace(temporary, path)


def append_row(path_value: str, headers: list[str], row: dict) -> None:
    """Migrate once and append atomically under a process-wide file lock."""
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")

    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        _migrate_header(path, headers)
        write_header = not path.exists() or path.stat().st_size == 0
        with path.open("a", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=headers, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow(row)
            target.flush()
            os.fsync(target.fileno())
