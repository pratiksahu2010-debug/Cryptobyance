"""Small stdlib SQLite adapter for local runs.

The production path remains asyncpg/PostgreSQL. SQLite is deliberately a
single-connection, serialized local backend; it is not intended for Render or
multi-worker production deployments.
"""
from __future__ import annotations

import asyncio
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_TIMESTAMP_COLUMNS = {
    "created_at", "updated_at", "delivered_at", "triggered_at", "sent_at",
    "next_attempt_at", "lease_until", "activated_at", "deactivated_at",
    "reviewed_at", "shadow_until",
}


def sqlite_path_from_dsn(dsn: str) -> str:
    value = str(dsn or "").strip()
    if value == "sqlite:///:memory:":
        return ":memory:"
    if value.startswith("sqlite:////"):
        return "/" + value[len("sqlite:////"):]
    if value.startswith("sqlite:///"):
        path = value[len("sqlite:///"):]
        if not path:
            raise ValueError("SQLite DATABASE_URL must include a database path")
        return path
    raise ValueError("SQLite URL must use sqlite:///relative.db or sqlite:////absolute/path.db")


def _translate_sql(sql: str, args: tuple[Any, ...]) -> tuple[str, tuple[Any, ...]]:
    query = str(sql).strip()
    if not query:
        return query, ()

    if "pg_advisory_xact_lock" in query:
        return "SELECT 1", ()

    # PostgreSQL-only JSON path extraction.
    query = query.replace(
        "s.payload #>> '{performance_attribution,strategy_key}'",
        "json_extract(s.payload, '$.performance_attribution.strategy_key')",
    )

    # JSON array expansion has equivalent JSON1 table-valued syntax.
    query = query.replace(
        "CROSS JOIN LATERAL jsonb_array_elements_text(c.rejection_reasons) AS r(reason)",
        "CROSS JOIN json_each(c.rejection_reasons) AS r",
    )
    query = query.replace("r.reason", "r.value")
    query = query.replace(
        "rejection_reasons ? 'manual_missed_opportunity'",
        "EXISTS (SELECT 1 FROM json_each(rejection_reasons) WHERE value='manual_missed_opportunity')",
    )

    # UTC hour extraction; SQLite stores CURRENT_TIMESTAMP in UTC.
    query = re.sub(
        r"EXTRACT\(HOUR FROM ([A-Za-z_][A-Za-z0-9_.]*) AT TIME ZONE 'UTC'\)",
        r"CAST(strftime('%H', \1) AS INTEGER)",
        query,
        flags=re.IGNORECASE,
    )

    # Convert common interval arithmetic into SQLite datetime modifiers.
    query = re.sub(
        r"NOW\(\)\s*\+\s*\(\$(\d+)\s*\*\s*INTERVAL\s*'1 day'\)",
        r"datetime('now', '+' || $\1 || ' days')", query, flags=re.IGNORECASE,
    )
    query = re.sub(
        r"NOW\(\)\s*-\s*\(\$(\d+)\s*\*\s*INTERVAL\s*'1 day'\)",
        r"datetime('now', '-' || $\1 || ' days')", query, flags=re.IGNORECASE,
    )
    query = re.sub(
        r"NOW\(\)\s*\+\s*\(\$(\d+)\s*\*\s*INTERVAL\s*'1 second'\)",
        r"datetime('now', '+' || $\1 || ' seconds')", query, flags=re.IGNORECASE,
    )
    query = re.sub(
        r"NOW\(\)\s*\+\s*INTERVAL\s*'([0-9]+)\s+(seconds?|minutes?|hours?|days?)'",
        r"datetime('now', '+\1 \2')", query, flags=re.IGNORECASE,
    )
    query = re.sub(
        r"NOW\(\)\s*-\s*INTERVAL\s*'([0-9]+)\s+(seconds?|minutes?|hours?|days?)'",
        r"datetime('now', '-\1 \2')", query, flags=re.IGNORECASE,
    )
    query = re.sub(r"\bNOW\(\)", "CURRENT_TIMESTAMP", query, flags=re.IGNORECASE)

    # PostgreSQL row locks are replaced by BEGIN IMMEDIATE at transaction start.
    query = re.sub(r"\s+FOR\s+UPDATE(?:\s+SKIP\s+LOCKED)?\b", "", query, flags=re.IGNORECASE)

    # PostgreSQL casts used by JSONB/numeric analytics are unnecessary in SQLite.
    query = re.sub(
        r"::\s*(?:jsonb|json|text|double\s+precision|numeric|integer|bigint|boolean|timestamptz)",
        "", query, flags=re.IGNORECASE,
    )

    # BIGSERIAL needs SQLite's INTEGER PRIMARY KEY rowid behavior.
    query = re.sub(r"\bBIGSERIAL\b", "INTEGER", query, flags=re.IGNORECASE)

    # SQLite has no DROP/ADD CONSTRAINT ALTER syntax; CREATE TABLE retains checks.
    if re.match(r"^ALTER\s+TABLE\s+\w+\s+DROP\s+CONSTRAINT\b", query, re.IGNORECASE):
        return "", ()
    if re.match(r"^ALTER\s+TABLE\s+\w+\s+ADD\s+CONSTRAINT\b", query, re.IGNORECASE):
        return "", ()

    # Add-column migrations are handled separately with PRAGMA table_info.
    query = re.sub(r"\bADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\b", "ADD COLUMN", query, flags=re.IGNORECASE)

    # Convert asyncpg's numbered placeholders to sqlite's positional placeholders.
    indexes = [int(value) for value in re.findall(r"\$(\d+)", query)]
    if indexes:
        if any(index < 1 or index > len(args) for index in indexes):
            raise ValueError("SQL placeholder does not match supplied arguments")
        params = tuple(args[index - 1] for index in indexes)
        query = re.sub(r"\$\d+", "?", query)
    else:
        params = ()
    normalized = []
    for value in params:
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            value = value.strftime("%Y-%m-%d %H:%M:%S.%f") if value.microsecond else value.strftime("%Y-%m-%d %H:%M:%S")
        normalized.append(value)
    return query, tuple(normalized)


class SQLiteRow(dict):
    """Mapping row that mirrors asyncpg's datetime values for timestamp columns."""

    def __init__(self, row: sqlite3.Row):
        values = {}
        for key in row.keys():
            value = row[key]
            if key in _TIMESTAMP_COLUMNS and isinstance(value, str):
                try:
                    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                except ValueError:
                    pass
            values[key] = value
        super().__init__(values)


class SQLiteTransaction:
    def __init__(self, connection: "SQLiteConnection"):
        self.connection = connection

    async def __aenter__(self):
        self.connection.raw.execute("BEGIN IMMEDIATE")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.connection.raw.commit()
        else:
            self.connection.raw.rollback()
        return False


class SQLiteConnection:
    def __init__(self, raw: sqlite3.Connection):
        self.raw = raw

    def transaction(self) -> SQLiteTransaction:
        return SQLiteTransaction(self)

    def _execute_one(self, sql: str, args: tuple[Any, ...] = ()):
        query, params = _translate_sql(sql, args)
        if not query:
            return None
        match = re.match(
            r"^ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)\s+(.+)$",
            query, re.IGNORECASE | re.DOTALL,
        )
        if match:
            table, column = match.group(1), match.group(2)
            columns = {
                str(row["name"])
                for row in self.raw.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if column in columns:
                return None
        return self.raw.execute(query, params)

    async def execute(self, sql: str, *args):
        query = str(sql).strip()
        # Schema DDL is passed as a multi-statement script without bind arguments.
        if ";" in query and not args:
            for statement in query.split(";"):
                statement = statement.strip()
                if statement:
                    self._execute_one(statement)
            return "OK"
        cursor = self._execute_one(query, tuple(args))
        if cursor is None:
            return "OK"
        verb = query.lstrip().split(None, 1)[0].upper()
        if verb == "INSERT":
            return f"INSERT 0 {max(cursor.rowcount, 0)}"
        if verb == "UPDATE":
            return f"UPDATE {max(cursor.rowcount, 0)}"
        if verb == "DELETE":
            return f"DELETE {max(cursor.rowcount, 0)}"
        return "OK"

    async def fetchrow(self, sql: str, *args):
        query, params = _translate_sql(sql, tuple(args))
        if not query:
            return None
        row = self.raw.execute(query, params).fetchone()
        return SQLiteRow(row) if row is not None else None

    async def fetch(self, sql: str, *args):
        query, params = _translate_sql(sql, tuple(args))
        if not query:
            return []
        rows = self.raw.execute(query, params).fetchall()
        return [SQLiteRow(row) for row in rows]


class SQLiteAcquire:
    def __init__(self, pool: "SQLitePool"):
        self.pool = pool

    async def __aenter__(self):
        await self.pool.lock.acquire()
        return self.pool.connection

    async def __aexit__(self, exc_type, exc, tb):
        self.pool.lock.release()
        return False


class SQLitePool:
    is_sqlite = True

    def __init__(self, path: str, raw: sqlite3.Connection):
        self.path = path
        self.connection = SQLiteConnection(raw)
        self.lock = asyncio.Lock()

    @classmethod
    async def connect(cls, dsn: str) -> "SQLitePool":
        path = sqlite_path_from_dsn(dsn)
        if path != ":memory:":
            path = str(Path(path).expanduser())
            Path(path).resolve().parent.mkdir(parents=True, exist_ok=True)
        raw = sqlite3.connect(path, timeout=10, isolation_level=None, check_same_thread=False)
        raw.row_factory = sqlite3.Row
        raw.execute("PRAGMA foreign_keys=ON")
        raw.execute("PRAGMA busy_timeout=10000")
        if path != ":memory:":
            raw.execute("PRAGMA journal_mode=WAL")
            raw.execute("PRAGMA synchronous=NORMAL")
        return cls(path, raw)

    def acquire(self) -> SQLiteAcquire:
        return SQLiteAcquire(self)

    async def close(self) -> None:
        async with self.lock:
            self.connection.raw.close()
