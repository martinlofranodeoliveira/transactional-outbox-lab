"""Actual SQLite support boundary; distribution names do not prove a backport."""
from __future__ import annotations

import re

MINIMUM_SQLITE = (3, 51, 3)


def require_qualified_sqlite(connection) -> dict[str, str]:
    row = connection.execute("SELECT sqlite_version(), sqlite_source_id()").fetchone()
    if row is None or not isinstance(row[0], str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", row[0]):
        raise RuntimeError("SQLite runtime identity is unavailable")
    version = tuple(int(part) for part in row[0].split("."))
    if version < MINIMUM_SQLITE or not isinstance(row[1], str) or not 1 <= len(row[1]) <= 256:
        raise RuntimeError("Actual linked SQLite 3.51.3 or newer is required")
    return {"sqlite_version": row[0], "sqlite_source_id": row[1]}
