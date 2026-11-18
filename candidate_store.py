"""Transactional PostgreSQL persistence for candidates, signals, cooldowns and Telegram outbox."""
from __future__ import annotations

import json
from datetime import datetime, timezone
import asyncpg

from state_machine import STATE_DDL, advance_persisted_state, persist_signal_lifecycle

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

CREATE TABLE IF NOT EXISTS signal_outcomes (
 signal_id BIGINT PRIMARY KEY REFERENCES signal_history(id) ON DELETE RESTRICT,
 symbol TEXT NOT NULL,
 direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
 signal_candle_open_ms BIGINT NOT NULL,
 entry DOUBLE PRECISION NOT NULL,
 stop DOUBLE PRECISION NOT NULL,
 target DOUBLE PRECISION NOT NULL,
 risk_per_unit DOUBLE PRECISION NOT NULL CHECK(risk_per_unit > 0),
 status TEXT NOT NULL DEFAULT 'OPEN'
   CHECK(status IN ('OPEN','TARGET','STOP_LOSS','AMBIGUOUS')),
 best_favorable_r DOUBLE PRECISION NOT NULL DEFAULT 0,
 worst_adverse_r DOUBLE PRECISION NOT NULL DEFAULT 0,
 bars_observed INTEGER NOT NULL DEFAULT 0,
 last_observed_candle_open_ms BIGINT NOT NULL,
 exit_candle_open_ms BIGINT,
 exit_price DOUBLE PRECISION,
 realized_r DOUBLE PRECISION,
 data_gap BOOLEAN NOT NULL DEFAULT FALSE,
 best_favorable_candle_open_ms BIGINT,
 worst_adverse_candle_open_ms BIGINT,
 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE signal_outcomes
 ADD COLUMN IF NOT EXISTS best_favorable_candle_open_ms BIGINT;
ALTER TABLE signal_outcomes
 ADD COLUMN IF NOT EXISTS worst_adverse_candle_open_ms BIGINT;
CREATE INDEX IF NOT EXISTS ix_signal_outcomes_symbol_status
 ON signal_outcomes(symbol,status,last_observed_candle_open_ms);

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
INSERT INTO schema_migrations(version) VALUES (1),(2) ON CONFLICT DO NOTHING;
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
                await conn.execute(STATE_DDL)
                # Repair rows created by the earlier queued-before-delivery
                # implementation. Only Telegram-confirmed SENT alerts belong
                # in performance statistics; also backfill any SENT row that
                # was missing an outcome record.
                async with conn.transaction():
                    await conn.execute(
                        """DELETE FROM signal_outcomes o
                           USING signal_history s
                           WHERE s.id=o.signal_id AND s.status <> 'SENT'"""
                    )
                    await conn.execute(
                        """INSERT INTO signal_outcomes(
                               signal_id,symbol,direction,signal_candle_open_ms,
                               entry,stop,target,risk_per_unit,last_observed_candle_open_ms
                           )
                           SELECT s.id,s.symbol,s.direction,s.candle_open_ms,
                                  (s.payload->>'entry')::double precision,
                                  (s.payload->>'stop')::double precision,
                                  (s.payload->>'target')::double precision,
                                  ABS((s.payload->>'entry')::double precision -
                                      (s.payload->>'stop')::double precision),
                                  s.candle_open_ms
                           FROM signal_history s
                           WHERE s.status='SENT'
                             AND NULLIF(s.payload->>'entry','') IS NOT NULL
                             AND NULLIF(s.payload->>'stop','') IS NOT NULL
                             AND NULLIF(s.payload->>'target','') IS NOT NULL
                             AND ABS((s.payload->>'entry')::double precision - (s.payload->>'stop')::double precision) > 0
                           ON CONFLICT(signal_id) DO NOTHING"""
                    )
            return cls(pool)
        except BaseException:
            await pool.close()
            raise

    async def close(self):
        await self.pool.close()

    async def performance_summary(self) -> dict:
        """Return aggregate and global risk diagnostics without exposing trade payloads."""
        async with self.pool.acquire() as conn:
            totals = await conn.fetchrow(
                """SELECT
                       COUNT(*) FILTER (WHERE s.status='SENT') AS delivered_signals,
                       COUNT(*) FILTER (WHERE s.status='DELIVERY_FAILED') AS delivery_failed,
                       COUNT(*) FILTER (WHERE s.status='QUEUED') AS queued,
                       COUNT(o.signal_id) AS tracked_outcomes,
                       COUNT(o.signal_id) FILTER (WHERE o.status='OPEN') AS open,
                       COUNT(o.signal_id) FILTER (WHERE o.status='TARGET') AS targets,
                       COUNT(o.signal_id) FILTER (WHERE o.status='STOP_LOSS') AS stop_losses,
                       COUNT(o.signal_id) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                       COUNT(o.signal_id) FILTER (WHERE o.data_gap) AS data_gaps,
                       COUNT(*) FILTER (WHERE s.status='SENT' AND o.signal_id IS NULL) AS sent_without_outcome,
                       ROUND((AVG(o.realized_r) FILTER (WHERE o.status IN ('TARGET','STOP_LOSS')))::numeric, 3) AS avg_realized_r,
                       ROUND((AVG(o.best_favorable_r))::numeric, 3) AS avg_mfe_r,
                       ROUND((AVG(o.worst_adverse_r))::numeric, 3) AS avg_mae_r
                   FROM signal_history s
                   LEFT JOIN signal_outcomes o ON o.signal_id=s.id"""
            )
            by_strategy = await conn.fetch(
                """SELECT COALESCE(s.payload #>> '{performance_attribution,strategy_key}',
                                      'legacy_unattributed') AS strategy_key,
                       COUNT(*) FILTER (WHERE s.status='SENT') AS delivered_signals,
                       COUNT(*) FILTER (WHERE s.status='DELIVERY_FAILED') AS delivery_failed,
                       COUNT(*) FILTER (WHERE s.status='QUEUED') AS queued,
                       COUNT(o.signal_id) FILTER (WHERE o.status='TARGET') AS targets,
                       COUNT(o.signal_id) FILTER (WHERE o.status='STOP_LOSS') AS stop_losses,
                       COUNT(o.signal_id) FILTER (WHERE o.status='OPEN') AS open,
                       COUNT(o.signal_id) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                       ROUND(AVG(o.realized_r) FILTER (WHERE o.status IN ('TARGET','STOP_LOSS'))::numeric,3) AS avg_realized_r,
                       ROUND(AVG(o.best_favorable_r)::numeric,3) AS avg_mfe_r,
                       ROUND(AVG(o.worst_adverse_r)::numeric,3) AS avg_mae_r
                   FROM signal_history s
                   LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   GROUP BY 1 ORDER BY strategy_key"""
            )
            by_symbol = await conn.fetch(
                """SELECT o.symbol,o.direction,
                       COUNT(*) AS tracked,
                       COUNT(*) FILTER (WHERE o.status='TARGET') AS targets,
                       COUNT(*) FILTER (WHERE o.status='STOP_LOSS') AS stop_losses,
                       COUNT(*) FILTER (WHERE o.status='OPEN') AS open,
                       COUNT(*) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                       ROUND((AVG(o.realized_r) FILTER (WHERE o.status IN ('TARGET','STOP_LOSS')))::numeric, 3) AS avg_realized_r,
                       ROUND((AVG(o.best_favorable_r))::numeric, 3) AS avg_mfe_r,
                       ROUND((AVG(o.worst_adverse_r))::numeric, 3) AS avg_mae_r
                   FROM signal_outcomes o
                   JOIN signal_history s ON s.id=o.signal_id
                   WHERE s.status='SENT'
                   GROUP BY o.symbol,o.direction
                   ORDER BY o.symbol,o.direction"""
            )
            # Global stop/target profile. Timestamp comparisons deliberately
            # exclude the exit candle because OHLC cannot reveal the order of
            # its high and low. This makes "before exit" excursion counts safe.
            risk_by_outcome = await conn.fetch(
                """SELECT o.status,COUNT(*) AS count,
                       ROUND(AVG(o.best_favorable_r)::numeric,3) AS avg_mfe_r,
                       ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY o.best_favorable_r)::numeric,3) AS median_mfe_r,
                       ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY o.best_favorable_r)::numeric,3) AS p75_mfe_r,
                       ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY o.best_favorable_r)::numeric,3) AS p90_mfe_r,
                       ROUND(AVG(o.worst_adverse_r)::numeric,3) AS avg_mae_r,
                       ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY o.worst_adverse_r)::numeric,3) AS median_mae_r,
                       ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY o.worst_adverse_r)::numeric,3) AS p75_mae_r,
                       ROUND(percentile_cont(0.90) WITHIN GROUP (ORDER BY o.worst_adverse_r)::numeric,3) AS p90_mae_r,
                       ROUND(AVG(o.bars_observed)::numeric,1) AS avg_bars_to_exit,
                       COUNT(o.best_favorable_candle_open_ms) AS mfe_timestamped,
                       COUNT(o.worst_adverse_candle_open_ms) AS mae_timestamped,
                       COUNT(*) FILTER (WHERE o.status='STOP_LOSS'
                           AND o.best_favorable_r>=0.50
                           AND o.best_favorable_candle_open_ms<o.exit_candle_open_ms) AS stop_mfe_050r_before_exit,
                       COUNT(*) FILTER (WHERE o.status='STOP_LOSS'
                           AND o.best_favorable_r>=1.00
                           AND o.best_favorable_candle_open_ms<o.exit_candle_open_ms) AS stop_mfe_100r_before_exit,
                       COUNT(*) FILTER (WHERE o.status='TARGET'
                           AND o.worst_adverse_r>=0.50
                           AND o.worst_adverse_candle_open_ms<o.exit_candle_open_ms) AS target_mae_050r_before_exit,
                       COUNT(*) FILTER (WHERE o.status='TARGET'
                           AND o.worst_adverse_r>=1.00
                           AND o.worst_adverse_candle_open_ms<o.exit_candle_open_ms) AS target_mae_100r_before_exit
                   FROM signal_outcomes o
                   JOIN signal_history s ON s.id=o.signal_id
                   WHERE s.status='SENT' AND o.status IN ('TARGET','STOP_LOSS')
                   GROUP BY o.status ORDER BY o.status"""
            )
            by_direction = await conn.fetch(
                """SELECT o.direction,o.status,COUNT(*) AS count,
                       ROUND(AVG(o.realized_r)::numeric,3) AS avg_realized_r,
                       ROUND(AVG(o.best_favorable_r)::numeric,3) AS avg_mfe_r,
                       ROUND(AVG(o.worst_adverse_r)::numeric,3) AS avg_mae_r,
                       ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY o.best_favorable_r)::numeric,3) AS median_mfe_r,
                       ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY o.worst_adverse_r)::numeric,3) AS median_mae_r
                   FROM signal_outcomes o
                   JOIN signal_history s ON s.id=o.signal_id
                   WHERE s.status='SENT'
                   GROUP BY o.direction,o.status ORDER BY o.direction,o.status"""
            )
            outbox = await conn.fetch(
                """SELECT status,COUNT(*) AS count
                   FROM telegram_outbox GROUP BY status ORDER BY status"""
            )
        return {
            "totals": dict(totals) if totals else {},
            "by_strategy": [dict(row) for row in by_strategy],
            "risk_by_outcome": [dict(row) for row in risk_by_outcome],
            "by_direction": [dict(row) for row in by_direction],
            "by_symbol_direction": [dict(row) for row in by_symbol],
            "outbox": [dict(row) for row in outbox],
        }

    async def cleanup(self, now_ms: int) -> dict[str, int]:
        """Expire stale pending candidates without deleting any persisted records."""
        latest_completed_open_ms = ((now_ms // 300_000) - 1) * 300_000
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                expired_tag = await conn.execute(
                    """UPDATE signal_candidates SET status='EXPIRED',updated_at=NOW()
                       WHERE status='PENDING' AND expires_after_candle_open_ms < $1""",
                    latest_completed_open_ms)
        # Retain candidate audit rows and all signal, delivery, cooldown and
        # runtime records. No automatic DELETE is performed by this cleanup.
        return {"expired": int(expired_tag.rsplit(" ", 1)[-1]), "deleted": 0}

    async def load_symbol_states(self, strategy_key: str, symbol: str) -> dict[str, str]:
        """Load durable direction states so RSI is computed only for ARMED sides."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT direction,state FROM strategy_signal_states
                   WHERE strategy_key=$1 AND symbol=$2""", strategy_key, symbol
            )
        return {str(row["direction"]): str(row["state"]) for row in rows}

    async def advance_symbol_state(
        self, *, strategy_key: str, symbol: str, direction: str,
        candle_open_ms: int, setup_valid: bool, invalidation: bool,
        trigger: bool, cooldown_candles: int,
    ) -> str:
        """Advance one per-symbol/per-side state on a newly closed candle."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                return await advance_persisted_state(
                    conn, strategy_key=strategy_key, symbol=symbol,
                    direction=direction, candle_open_ms=candle_open_ms,
                    setup_valid=setup_valid, invalidation=invalidation,
                    trigger=trigger, cooldown_candles=cooldown_candles,
                )

    async def invalidate(self, symbol: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE symbol=$1 AND status='PENDING'",
                symbol)

    async def observe(self, symbol: str, direction: str, candle_ms: int, adx: float,
                      adx_min: float) -> tuple[str, int | None]:
        """Compatibility guard: ADX is evaluated immediately, never promoted later.

        Pending rows from older releases are invalidated and cannot generate
        delayed alerts. New callers should queue only a fully valid signal.
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE signal_candidates SET status='INVALID',updated_at=NOW()
                   WHERE symbol=$1 AND status='PENDING'""", symbol)
        return ("READY", None) if float(adx) >= float(adx_min) else ("REJECT", None)

    async def enqueue_signal(self, signal: dict, candidate_id: int | None,
                             cooldown_candles: int, strategy_key: str = "default") -> bool:
        """Atomically persist signal, mark candidate, apply durable cooldown and enqueue alert."""
        symbol, direction = signal["symbol"], signal["side"]
        candle_ms = int(signal["candle"])
        key = f"{symbol}:{direction}:{candle_ms}"
        # Persist the exact strategy lifecycle key with each newly queued signal.
        # Older rows intentionally remain unattributed; never infer their version.
        attributed_signal = dict(signal)
        attributed_signal["performance_attribution"] = {"strategy_key": str(strategy_key)}
        payload = json.dumps(attributed_signal, separators=(",", ":"), allow_nan=False)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Serialize competing signal writers for this symbol even when
                # no cooldown row exists yet (SELECT FOR UPDATE cannot lock absence).
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", symbol)
                lock_key = f"{strategy_key}:{symbol}:{direction}"
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", lock_key)
                cooldown = await conn.fetchrow(
                    "SELECT last_direction,last_candle_open_ms FROM signal_cooldowns WHERE symbol=$1 FOR UPDATE", symbol)
                if cooldown and cooldown["last_direction"] == direction and candle_ms - int(cooldown["last_candle_open_ms"]) < cooldown_candles * 300_000:
                    if candidate_id is not None:
                        await conn.execute(
                            "UPDATE signal_candidates SET status='INVALID',updated_at=NOW() WHERE id=$1 AND status='PENDING'",
                            candidate_id)
                    return False
                existing = await conn.fetchrow(
                    "SELECT id FROM signal_history WHERE idempotency_key=$1", key
                )
                if existing is not None:
                    return False
                lifecycle_recorded = await persist_signal_lifecycle(
                    conn, strategy_key=strategy_key, symbol=symbol, direction=direction,
                    candle_open_ms=candle_ms, cooldown_candles=cooldown_candles,
                )
                if not lifecycle_recorded:
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
                    # The same signal may already have been committed before a
                    # process restart; complete the candidate transition without
                    # creating another outbox message.
                    if candidate_id is not None:
                        await conn.execute(
                            """UPDATE signal_candidates SET status='TRIGGERED',
                               triggered_at=COALESCE(triggered_at,NOW()),updated_at=NOW()
                               WHERE id=$1 AND status='PENDING'""", candidate_id)
                    return False
                signal_id = int(row["id"])
                entry = float(signal["entry"])
                stop = float(signal["stop"])
                target = float(signal["target"])
                risk_per_unit = abs(entry - stop)
                if risk_per_unit <= 0:
                    raise ValueError("signal stop must define positive risk")
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

    async def update_open_outcomes(self, symbol: str, candles: list[dict]) -> int:
        """Update MFE/MAE and conservative stop/target outcomes from later closed 5m bars.

        Only delivered signals are measured. The signal candle itself is excluded
        because entry is assumed at its close. If both stop and target occur in
        one OHLC candle, the result is AMBIGUOUS rather than assuming an intrabar
        path that OHLC data cannot prove.
        """
        normalized = []
        for candle in candles:
            ts = int(candle["timestamp"])
            normalized.append((ts, float(candle["high"]), float(candle["low"])))
        normalized.sort(key=lambda item: item[0])
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT o.signal_id,o.direction,o.signal_candle_open_ms,o.entry,
                          o.stop,o.target,o.risk_per_unit,o.best_favorable_r,
                          o.worst_adverse_r,o.bars_observed,
                          o.last_observed_candle_open_ms,o.data_gap,
                          o.best_favorable_candle_open_ms,o.worst_adverse_candle_open_ms
                   FROM signal_outcomes o
                   JOIN signal_history s ON s.id=o.signal_id
                   WHERE o.symbol=$1 AND o.status='OPEN' AND s.status='SENT'
                   ORDER BY o.signal_candle_open_ms""",
                symbol)
            updated = 0
            for outcome in rows:
                side = outcome["direction"]
                entry = float(outcome["entry"])
                stop = float(outcome["stop"])
                target = float(outcome["target"])
                risk = float(outcome["risk_per_unit"])
                best = float(outcome["best_favorable_r"])
                worst = float(outcome["worst_adverse_r"])
                best_ts = outcome["best_favorable_candle_open_ms"]
                worst_ts = outcome["worst_adverse_candle_open_ms"]
                bars_seen = int(outcome["bars_observed"])
                last_ts = int(outcome["last_observed_candle_open_ms"])
                data_gap = bool(outcome["data_gap"])
                status = "OPEN"
                exit_ts = None
                exit_price = None
                realized_r = None

                for ts, high, low in normalized:
                    if ts <= last_ts or ts <= int(outcome["signal_candle_open_ms"]):
                        continue
                    if ts - last_ts != 300_000:
                        # We cannot know whether stop or target was touched in
                        # an unobserved candle. Never classify a later barrier
                        # touch as a reliable win/loss after losing chronology.
                        data_gap = True
                        status = "AMBIGUOUS"
                        exit_ts = ts
                        bars_seen += 1
                        last_ts = ts
                        break
                    if side == "LONG":
                        favorable = max(0.0, (high - entry) / risk)
                        adverse = max(0.0, (entry - low) / risk)
                        stop_hit = low <= stop
                        target_hit = high >= target
                    else:
                        favorable = max(0.0, (entry - low) / risk)
                        adverse = max(0.0, (high - entry) / risk)
                        stop_hit = high >= stop
                        target_hit = low <= target
                    if favorable > best:
                        best = favorable
                        best_ts = ts
                    if adverse > worst:
                        worst = adverse
                        worst_ts = ts
                    bars_seen += 1
                    last_ts = ts
                    if stop_hit and target_hit:
                        status = "AMBIGUOUS"
                        exit_ts = ts
                    elif stop_hit:
                        status = "STOP_LOSS"
                        exit_ts = ts
                        exit_price = stop
                        realized_r = -1.0
                    elif target_hit:
                        status = "TARGET"
                        exit_ts = ts
                        exit_price = target
                        realized_r = abs(target - entry) / risk
                    if status != "OPEN":
                        break

                if bars_seen == int(outcome["bars_observed"]) and data_gap == bool(outcome["data_gap"]):
                    continue
                await conn.execute(
                    """UPDATE signal_outcomes
                       SET status=$2,best_favorable_r=$3,worst_adverse_r=$4,
                           bars_observed=$5,last_observed_candle_open_ms=$6,
                           exit_candle_open_ms=$7,exit_price=$8,realized_r=$9,
                           data_gap=$10,best_favorable_candle_open_ms=$11,
                           worst_adverse_candle_open_ms=$12,updated_at=NOW()
                       WHERE signal_id=$1 AND status='OPEN'""",
                    int(outcome["signal_id"]),status,best,worst,bars_seen,last_ts,
                    exit_ts,exit_price,realized_r,data_gap,best_ts,worst_ts)
                updated += 1
            return updated

    async def claim_outbox(self) -> dict | None:
        """Claim one ready message using a lease; safe across concurrent workers."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """SELECT id,signal_id,payload::text AS payload,attempts FROM telegram_outbox
                       WHERE ((status='PENDING' AND next_attempt_at<=NOW())
                           OR (status='DELIVERING' AND lease_until<NOW()))
                       ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1""")
                if row is None:
                    return None
                await conn.execute(
                    """UPDATE telegram_outbox SET status='DELIVERING',attempts=attempts+1,
                       lease_until=NOW()+INTERVAL '90 seconds' WHERE id=$1""", row["id"])
                return {"id":int(row["id"]),"signal_id":int(row["signal_id"]),
                        "payload":json.loads(row["payload"]),"attempts":int(row["attempts"])+1}

    async def finish_outbox(self, outbox_id: int):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "UPDATE telegram_outbox SET status='SENT',sent_at=NOW(),lease_until=NULL WHERE id=$1 RETURNING signal_id",
                    outbox_id)
                if row:
                    await conn.execute(
                        "UPDATE signal_history SET status='SENT',delivered_at=NOW() WHERE id=$1", row["signal_id"])
                    # Outcome tracking starts only after Telegram confirms delivery.
                    # Keeping this insert in the same transaction prevents queued or
                    # permanently failed alerts from polluting performance statistics.
                    await conn.execute(
                        """INSERT INTO signal_outcomes(
                               signal_id,symbol,direction,signal_candle_open_ms,
                               entry,stop,target,risk_per_unit,last_observed_candle_open_ms
                           )
                           SELECT id,symbol,direction,candle_open_ms,
                                  (payload->>'entry')::double precision,
                                  (payload->>'stop')::double precision,
                                  (payload->>'target')::double precision,
                                  ABS((payload->>'entry')::double precision -
                                      (payload->>'stop')::double precision),
                                  candle_open_ms
                           FROM signal_history WHERE id=$1
                           ON CONFLICT(signal_id) DO NOTHING""",
                        row["signal_id"])

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

    async def load_runtime(self) -> dict | None:
        """Read the last durable scanner checkpoint for restart diagnostics."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status::text AS status,updated_at FROM scanner_runtime WHERE instance_key='primary'")
        if row is None:
            return None
        return {"status": json.loads(row["status"]), "updated_at": row["updated_at"].isoformat()}
