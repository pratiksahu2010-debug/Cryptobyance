"""Durable PostgreSQL store for temporarily ADX-pending signal candidates."""
from __future__ import annotations
from datetime import datetime, timezone
import asyncpg

DDL = """
CREATE TABLE IF NOT EXISTS signal_candidates (
 id BIGSERIAL PRIMARY KEY,
 symbol TEXT NOT NULL,
 direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
 first_candle_open_ms BIGINT NOT NULL,
 last_checked_candle_open_ms BIGINT NOT NULL,
 expires_after_candle_open_ms BIGINT NOT NULL,
 initial_adx DOUBLE PRECISION NOT NULL,
 current_adx DOUBLE PRECISION NOT NULL,
 status TEXT NOT NULL DEFAULT 'PENDING'
   CHECK(status IN ('PENDING','TRIGGERED','EXPIRED','INVALID')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 triggered_at TIMESTAMPTZ,
 UNIQUE(symbol,direction,first_candle_open_ms)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_signal_candidates_one_pending_per_symbol
 ON signal_candidates(symbol) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS ix_signal_candidates_status_expiry
 ON signal_candidates(status,expires_after_candle_open_ms);
"""

class CandidateStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str):
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=5,
                                         timeout=10, command_timeout=8)
        async with pool.acquire() as conn:
            await conn.execute(DDL)
        return cls(pool)

    async def close(self):
        await self.pool.close()

    async def invalidate(self, symbol: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE symbol=$1 AND status='PENDING'",
                symbol)

    async def observe(self, symbol: str, direction: str, candle_ms: int, adx: float,
                      adx_min: float) -> tuple[str, int | None]:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """UPDATE signal_candidates SET status='EXPIRED',updated_at=NOW()
                       WHERE symbol=$1 AND status='PENDING' AND expires_after_candle_open_ms < $2""",
                    symbol, candle_ms)
                row = await conn.fetchrow(
                    """SELECT id,direction,expires_after_candle_open_ms FROM signal_candidates
                       WHERE symbol=$1 AND status='PENDING' FOR UPDATE""", symbol)
                if row and row["direction"] != direction:
                    await conn.execute("UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE id=$1", row["id"])
                    row = None
                if row:
                    await conn.execute(
                        """UPDATE signal_candidates SET last_checked_candle_open_ms=$2,
                           current_adx=$3,updated_at=NOW() WHERE id=$1""",
                        row["id"], candle_ms, adx)
                    if adx >= adx_min:
                        return "TRIGGER", int(row["id"])
                    return "PENDING", int(row["id"])
                if adx >= adx_min:
                    return "READY", None
                row = await conn.fetchrow(
                    """INSERT INTO signal_candidates
                       (symbol,direction,first_candle_open_ms,last_checked_candle_open_ms,
                        expires_after_candle_open_ms,initial_adx,current_adx)
                       VALUES($1,$2,$3,$3,$4,$5,$5)
                       ON CONFLICT DO NOTHING RETURNING id""",
                    symbol,direction,candle_ms,candle_ms+3*300_000,adx)
                return ("PENDING", int(row["id"])) if row else ("PENDING", None)

    async def resolve(self, candidate_id: int | None, delivered: bool):
        if candidate_id is None:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE signal_candidates SET status=$2,triggered_at=$3,updated_at=NOW()
                   WHERE id=$1 AND status='PENDING'""",
                candidate_id, "TRIGGERED" if delivered else "PENDING",
                datetime.now(timezone.utc) if delivered else None)
