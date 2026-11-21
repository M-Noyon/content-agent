"""Persistent job store (SQLite, WAL mode). This is what makes the agent pause/crash-safe."""
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from app.models import UNFINISHED, JobStatus

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    path        TEXT NOT NULL,
    sha256      TEXT NOT NULL UNIQUE,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    metadata    TEXT,
    publish     TEXT,
    analytics   TEXT,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
"""
JSON_FIELDS = ("metadata", "publish", "analytics")
UPDATABLE = {"path", "status", "attempts", "metadata", "publish", "analytics", "error"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class JobStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn: aiosqlite.Connection | None = None

    async def init(self) -> None:
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL")
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()

    @staticmethod
    def _to_dict(row) -> dict:
        d = dict(row)
        for f in JSON_FIELDS:
            d[f] = json.loads(d[f]) if d[f] else None
        return d

    async def create(self, path: str, filename: str, sha256: str) -> tuple[dict, bool]:
        """Insert a job. Returns (job, created). Same content twice -> existing job (dedupe)."""
        ts, job_id = now_iso(), uuid.uuid4().hex[:12]
        try:
            await self.conn.execute(
                "INSERT INTO jobs (id, filename, path, sha256, status, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_id, filename, path, sha256, JobStatus.QUEUED.value, ts, ts))
            await self.conn.commit()
            return await self.get(job_id), True
        except sqlite3.IntegrityError:
            return await self.get_by_sha(sha256), False

    async def get(self, job_id: str) -> dict | None:
        async with self.conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)) as cur:
            row = await cur.fetchone()
        return self._to_dict(row) if row else None

    async def get_by_sha(self, sha256: str) -> dict | None:
        async with self.conn.execute("SELECT * FROM jobs WHERE sha256 = ?", (sha256,)) as cur:
            row = await cur.fetchone()
        return self._to_dict(row) if row else None

    async def list_jobs(self, limit: int = 50, status: str | None = None) -> list[dict]:
        sql, args = "SELECT * FROM jobs", []
        if status:
            sql += " WHERE status = ?"
            args.append(status)
        sql += " ORDER BY created_at DESC LIMIT ?"
        args.append(limit)
        async with self.conn.execute(sql, args) as cur:
            return [self._to_dict(r) for r in await cur.fetchall()]

    async def unfinished(self) -> list[str]:
        marks = ",".join("?" * len(UNFINISHED))
        async with self.conn.execute(
                f"SELECT id FROM jobs WHERE status IN ({marks}) ORDER BY created_at",
                [s.value for s in UNFINISHED]) as cur:
            return [r["id"] for r in await cur.fetchall()]

    async def update(self, job_id: str, **fields) -> None:
        bad = set(fields) - UPDATABLE
        if bad:
            raise ValueError(f"Cannot update fields: {bad}")
        fields["updated_at"] = now_iso()
        cols, vals = [], []
        for key, value in fields.items():
            if key in JSON_FIELDS and value is not None:
                value = json.dumps(value)
            if isinstance(value, JobStatus):
                value = value.value
            cols.append(f"{key} = ?")
            vals.append(value)
        vals.append(job_id)
        await self.conn.execute(f"UPDATE jobs SET {', '.join(cols)} WHERE id = ?", vals)
        await self.conn.commit()
