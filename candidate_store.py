"""Transactional PostgreSQL persistence for candidates, signals, cooldowns and Telegram outbox."""
from __future__ import annotations

import json
from datetime import datetime, timezone
import asyncpg

DDL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
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

CREATE TABLE IF NOT EXISTS signal_history (
 id BIGSERIAL PRIMARY KEY,
 idempotency_key TEXT NOT NULL UNIQUE,
 symbol TEXT NOT NULL,
 direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
 candle_open_ms BIGINT NOT NULL,
 payload JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'QUEUED'
   CHECK(status IN ('QUEUED','SENT','DELIVERY_FAILED')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 delivered_at TIMESTAMPTZ,
 UNIQUE(symbol,direction,candle_open_ms)
);
CREATE INDEX IF NOT EXISTS ix_signal_history_created ON signal_history(created_at DESC);

CREATE TABLE IF NOT EXISTS telegram_outbox (
 id BIGSERIAL PRIMARY KEY,
 signal_id BIGINT NOT NULL UNIQUE REFERENCES signal_history(id) ON DELETE RESTRICT,
 idempotency_key TEXT NOT NULL UNIQUE,
 payload JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'PENDING'
   CHECK(status IN ('PENDING','DELIVERING','SENT','DEAD')),
 attempts INTEGER NOT NULL DEFAULT 0,
 next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 lease_until TIMESTAMPTZ,
 last_error_type TEXT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_telegram_outbox_ready
 ON telegram_outbox(status,next_attempt_at,created_at);

CREATE TABLE IF NOT EXISTS signal_cooldowns (
 symbol TEXT PRIMARY KEY,
 last_direction TEXT NOT NULL CHECK(last_direction IN ('LONG','SHORT')),
 last_candle_open_ms BIGINT NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS scanner_runtime (
 instance_key TEXT PRIMARY KEY,
 status JSONB NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO schema_migrations(version) VALUES (1) ON CONFLICT DO NOTHING;
"""

class CandidateStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    @classmethod
    async def connect(cls, dsn: str):
        pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=8,
                                         timeout=10, command_timeout=10)
        try:
            async with pool.acquire() as conn:
                await conn.execute(DDL)
            return cls(pool)
        except BaseException:
            await pool.close()
            raise

    async def close(self):
        await self.pool.close()

    async def cleanup(self, now_ms: int) -> dict[str, int]:
        """Expire candidates and prune only old terminal candidates; retain signal audit/outbox."""
        latest_completed_open_ms = ((now_ms // 300_000) - 1) * 300_000
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                expired_tag = await conn.execute(
                    """UPDATE signal_candidates SET status='EXPIRED',updated_at=NOW()
                       WHERE status='PENDING' AND expires_after_candle_open_ms < $1""",
                    latest_completed_open_ms)
                deleted_tag = await conn.execute(
                    """DELETE FROM signal_candidates
                       WHERE status IN ('EXPIRED','INVALID')
                         AND updated_at < NOW() - INTERVAL '7 days'""")
        return {"expired": int(expired_tag.rsplit(" ", 1)[-1]),
                "deleted": int(deleted_tag.rsplit(" ", 1)[-1])}

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
                    """SELECT id,direction,expires_after_candle_open_ms,last_checked_candle_open_ms
                       FROM signal_candidates WHERE symbol=$1 AND status='PENDING' FOR UPDATE""", symbol)
                if row and row["direction"] != direction:
                    await conn.execute(
                        "UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE id=$1", row["id"])
                    row = None
                if row and candle_ms <= row["last_checked_candle_open_ms"]:
                    return "PENDING", int(row["id"])
                if row:
                    await conn.execute(
                        """UPDATE signal_candidates SET last_checked_candle_open_ms=$2,
                           current_adx=$3,updated_at=NOW() WHERE id=$1""", row["id"], candle_ms, adx)
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

    async def enqueue_signal(self, signal: dict, candidate_id: int | None,
                             cooldown_candles: int) -> bool:
        """Atomically persist signal, mark candidate, apply durable cooldown and enqueue alert."""
        symbol, direction = signal["symbol"], signal["side"]
        candle_ms = int(signal["candle"])
        key = f"{symbol}:{direction}:{candle_ms}"
        payload = json.dumps(signal, separators=(",", ":"), allow_nan=False)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                cooldown = await conn.fetchrow(
                    "SELECT last_direction,last_candle_open_ms FROM signal_cooldowns WHERE symbol=$1 FOR UPDATE", symbol)
                if cooldown and cooldown["last_direction"] == direction and candle_ms - int(cooldown["last_candle_open_ms"]) < cooldown_candles * 300_000:
                    if candidate_id is not None:
                        await conn.execute(
                            "UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE id=$1 AND status='PENDING'",
                            candidate_id)
                    return False
                row = await conn.fetchrow(
                    """INSERT INTO signal_history(idempotency_key,symbol,direction,candle_open_ms,payload)
                       VALUES($1,$2,$3,$4,$5::jsonb)
                       ON CONFLICT(idempotency_key) DO NOTHING RETURNING id""",
                    key,symbol,direction,candle_ms,payload)
                if row is None:
                    return False
                signal_id = int(row["id"])
                await conn.execute(
                    """INSERT INTO telegram_outbox(signal_id,idempotency_key,payload)
                       VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING""",
                    signal_id,key,payload)
                await conn.execute(
                    """INSERT INTO signal_cooldowns(symbol,last_direction,last_candle_open_ms)
                       VALUES($1,$2,$3)
                       ON CONFLICT(symbol) DO UPDATE SET last_direction=EXCLUDED.last_direction,
                       last_candle_open_ms=EXCLUDED.last_candle_open_ms,updated_at=NOW()""",
                    symbol,direction,candle_ms)
                if candidate_id is not None:
                    await conn.execute(
                        """UPDATE signal_candidates SET status='TRIGGERED',triggered_at=NOW(),updated_at=NOW()
                           WHERE id=$1 AND status='PENDING'""", candidate_id)
                return True

    async def claim_outbox(self) -> dict | None:
        """Claim one ready message using a lease; safe across concurrent workers."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """SELECT id,payload::text AS payload,attempts FROM telegram_outbox
                       WHERE ((status='PENDING' AND next_attempt_at<=NOW())
                           OR (status='DELIVERING' AND lease_until<NOW()))
                       ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1""")
                if row is None:
                    return None
                await conn.execute(
                    """UPDATE telegram_outbox SET status='DELIVERING',attempts=attempts+1,
                       lease_until=NOW()+INTERVAL '90 seconds' WHERE id=$1""", row["id"])
                return {"id":int(row["id"]),"payload":json.loads(row["payload"]),
                        "attempts":int(row["attempts"])+1}

    async def finish_outbox(self, outbox_id: int):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "UPDATE telegram_outbox SET status='SENT',sent_at=NOW(),lease_until=NULL WHERE id=$1 RETURNING signal_id",
                    outbox_id)
                if row:
                    await conn.execute(
                        "UPDATE signal_history SET status='SENT',delivered_at=NOW() WHERE id=$1", row["signal_id"])

    async def retry_outbox(self, outbox_id: int, attempts: int, error_type: str):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT signal_id FROM telegram_outbox WHERE id=$1 FOR UPDATE", outbox_id)
                if row is None:
                    return
                if attempts >= 12:
                    await conn.execute(
                        """UPDATE telegram_outbox SET status='DEAD',lease_until=NULL,last_error_type=$2
                           WHERE id=$1""", outbox_id, error_type[:80])
                    await conn.execute(
                        "UPDATE signal_history SET status='DELIVERY_FAILED' WHERE id=$1", row["signal_id"])
                else:
                    delay = min(900, 2 ** min(attempts, 9))
                    await conn.execute(
                        """UPDATE telegram_outbox SET status='PENDING',lease_until=NULL,
                           next_attempt_at=NOW()+($2 * INTERVAL '1 second'),last_error_type=$3
                           WHERE id=$1""", outbox_id, delay, error_type[:80])

    async def persist_runtime(self, status: dict):
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO scanner_runtime(instance_key,status,updated_at)
                   VALUES('primary',$1::jsonb,NOW())
                   ON CONFLICT(instance_key) DO UPDATE SET status=EXCLUDED.status,updated_at=NOW()""",
                json.dumps(status, separators=(",", ":"), default=str))
