"""Auditable self-learning journal for delivered signals and rejected setups.

The journal is observational: it never changes strategy rules or places orders.
"""
from __future__ import annotations

import asyncio
import json
import math
from datetime import datetime, timezone, timedelta
from typing import Any

from strategy import indicators, ADX_MIN, ATR_STOP_MULT, MIN_RR

JOURNAL_DDL = """
CREATE TABLE IF NOT EXISTS journal_feedback (
    id BIGSERIAL PRIMARY KEY,
    signal_id BIGINT NOT NULL REFERENCES signal_history(id) ON DELETE RESTRICT,
    category TEXT NOT NULL CHECK (category IN (
      'GOOD_ENTRY','BAD_SIGNAL','LATE_ENTRY','FAKE_SIGNAL','SKIP','MISSED_OPPORTUNITY',
      'STOP_LOSS_TOO_TIGHT','TARGET_TOO_CLOSE','WRONG_DIRECTION','NEEDS_REVIEW'
    )),
    comment TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'dashboard' CHECK (source IN ('dashboard','telegram','api')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_journal_feedback_signal_created
 ON journal_feedback(signal_id,created_at DESC);
ALTER TABLE journal_feedback DROP CONSTRAINT IF EXISTS journal_feedback_category_check;
ALTER TABLE journal_feedback ADD CONSTRAINT journal_feedback_category_check
 CHECK (category IN (
   'GOOD_ENTRY','BAD_SIGNAL','LATE_ENTRY','FAKE_SIGNAL','SKIP','MISSED_OPPORTUNITY',
   'STOP_LOSS_TOO_TIGHT','TARGET_TOO_CLOSE','WRONG_DIRECTION','NEEDS_REVIEW'
 ));
CREATE TABLE IF NOT EXISTS journal_candidates (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('LONG','SHORT')),
    candle_open_ms BIGINT NOT NULL,
    entry DOUBLE PRECISION NOT NULL,
    stop DOUBLE PRECISION NOT NULL,
    target DOUBLE PRECISION NOT NULL,
    risk_per_unit DOUBLE PRECISION NOT NULL CHECK(risk_per_unit > 0),
    snapshot JSONB NOT NULL,
    rejection_reasons JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN'
      CHECK(status IN ('OPEN','TARGET','STOP_LOSS','AMBIGUOUS')),
    best_favorable_r DOUBLE PRECISION NOT NULL DEFAULT 0,
    worst_adverse_r DOUBLE PRECISION NOT NULL DEFAULT 0,
    bars_observed INTEGER NOT NULL DEFAULT 0,
    last_observed_candle_open_ms BIGINT NOT NULL,
    data_gap BOOLEAN NOT NULL DEFAULT FALSE,
    exit_candle_open_ms BIGINT,
    realized_r DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(symbol,direction,candle_open_ms)
);
CREATE INDEX IF NOT EXISTS ix_journal_candidates_open
 ON journal_candidates(symbol,status,last_observed_candle_open_ms);
CREATE TABLE IF NOT EXISTS journal_candidate_feedback (
    id BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT NOT NULL REFERENCES journal_candidates(id) ON DELETE RESTRICT,
    category TEXT NOT NULL CHECK (category IN ('MISSED_OPPORTUNITY','NEEDS_REVIEW')),
    comment TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'dashboard' CHECK (source IN ('dashboard','telegram','api')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_journal_candidate_feedback_created
 ON journal_candidate_feedback(candidate_id,created_at DESC);
CREATE TABLE IF NOT EXISTS training_instructions (
    id BIGSERIAL PRIMARY KEY,
    raw_text TEXT NOT NULL,
    parsed JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING_APPROVAL'
      CHECK(status IN ('PENDING_APPROVAL','APPROVED_RESEARCH','REJECTED')),
    reviewer_note TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_training_instructions_status_created
 ON training_instructions(status,created_at DESC);
CREATE TABLE IF NOT EXISTS training_daily_reviews (
    id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    summary JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(window_start,window_end)
);
CREATE TABLE IF NOT EXISTS training_proposals (
    id BIGSERIAL PRIMARY KEY,
    base_config_version TEXT NOT NULL,
    proposed_config_version TEXT NOT NULL,
    proposed_config JSONB NOT NULL,
    changes JSONB NOT NULL,
    metrics JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PROPOSED'
      CHECK(status IN ('PROPOSED','APPROVED','REJECTED','ROLLED_BACK','SHADOW')),
    reviewer_note TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_training_proposals_status_created
 ON training_proposals(status,created_at DESC);
CREATE TABLE IF NOT EXISTS config_history (
    config_version TEXT PRIMARY KEY,
    config JSONB NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('ACTIVE','INACTIVE','SHADOW')),
    previous_version TEXT,
    approved_by TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    deactivated_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_config_history_one_active
 ON config_history(status) WHERE status='ACTIVE';
CREATE TABLE IF NOT EXISTS config_history_events (
    id BIGSERIAL PRIMARY KEY,
    action TEXT NOT NULL CHECK(action IN ('INITIALIZE','APPROVE','REJECT','ROLLBACK','SHADOW_START','SHADOW_STOP','SHADOW_EXPIRE')),
    config_version TEXT NOT NULL,
    previous_version TEXT,
    actor TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_config_history_events_created
 ON config_history_events(created_at DESC);
CREATE TABLE IF NOT EXISTS training_weekly_digests (
    id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    summary JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','SENT')),
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMPTZ,
    UNIQUE(window_start,window_end)
);
CREATE INDEX IF NOT EXISTS ix_training_weekly_digests_pending
 ON training_weekly_digests(status,created_at);
ALTER TABLE config_history DROP CONSTRAINT IF EXISTS config_history_status_check;
ALTER TABLE config_history ADD CONSTRAINT config_history_status_check
 CHECK(status IN ('ACTIVE','INACTIVE','SHADOW'));
ALTER TABLE config_history ADD COLUMN IF NOT EXISTS shadow_until TIMESTAMPTZ;
CREATE UNIQUE INDEX IF NOT EXISTS uq_config_history_one_shadow
 ON config_history(status) WHERE status='SHADOW';
ALTER TABLE training_proposals DROP CONSTRAINT IF EXISTS training_proposals_status_check;
ALTER TABLE training_proposals ADD CONSTRAINT training_proposals_status_check
 CHECK(status IN ('PROPOSED','APPROVED','REJECTED','ROLLED_BACK','SHADOW'));
ALTER TABLE config_history_events DROP CONSTRAINT IF EXISTS config_history_events_action_check;
ALTER TABLE config_history_events ADD CONSTRAINT config_history_events_action_check
 CHECK(action IN ('INITIALIZE','APPROVE','REJECT','ROLLBACK','SHADOW_START','SHADOW_STOP','SHADOW_EXPIRE'));
"""

FEEDBACK_CATEGORIES = {
    "GOOD_ENTRY", "BAD_SIGNAL", "LATE_ENTRY", "FAKE_SIGNAL", "SKIP", "MISSED_OPPORTUNITY",
    "STOP_LOSS_TOO_TIGHT", "TARGET_TOO_CLOSE", "WRONG_DIRECTION", "NEEDS_REVIEW",
}


def rejected_observation(df5, df15) -> dict[str, Any] | None:
    """Create a conservative, point-in-time rejected-setup snapshot, if meaningful."""
    if df5 is None or len(df5) < 60:
        return None
    frame = indicators(df5.copy())
    if len(frame) < 35:
        return None
    c = frame.iloc[-1]
    fields = ("ema9", "ema21", "rsi", "adx", "plus_di", "minus_di", "atr", "vwap", "close")
    values = {name: float(c[name]) for name in fields}
    if not all(math.isfinite(v) for v in values.values()) or values["atr"] <= 0:
        return None
    long_hint = values["ema9"] > values["ema21"] and values["close"] > values["vwap"] and values["rsi"] > 50
    short_hint = values["ema9"] < values["ema21"] and values["close"] < values["vwap"] and values["rsi"] < 50
    if not (long_hint or short_hint):
        return None
    side = "LONG" if long_hint else "SHORT"
    reasons = []
    if values["adx"] < ADX_MIN:
        reasons.append("adx_below_minimum")
    if (side == "LONG" and values["plus_di"] <= values["minus_di"]) or (side == "SHORT" and values["minus_di"] <= values["plus_di"]):
        reasons.append("directional_index_mismatch")
    alignment_age = 0
    for row in frame.tail(6).iloc[::-1].itertuples(index=False):
        aligned = row.ema9 > row.ema21 if side == "LONG" else row.ema9 < row.ema21
        if not bool(aligned):
            break
        alignment_age += 1
    if alignment_age > 3:
        reasons.append("ema_alignment_too_old")
    prior = frame.iloc[-4]
    fast_slope = (values["ema9"] - float(prior.ema9)) / values["atr"]
    slow_slope = (values["ema21"] - float(prior.ema21)) / values["atr"]
    extension = (values["close"] - values["vwap"]) / values["atr"]
    if side == "LONG" and (fast_slope <= 0 or slow_slope <= 0):
        reasons.append("ema_slope_not_rising")
    if side == "SHORT" and (fast_slope >= 0 or slow_slope >= 0):
        reasons.append("ema_slope_not_falling")
    if abs(extension) > 1.0:
        reasons.append("entry_extended_from_vwap")
    context = None
    if df15 is None or len(df15) < 35:
        reasons.append("15m_context_unavailable")
    else:
        ctx = indicators(df15.copy()).iloc[-1]
        if not math.isfinite(float(ctx.ema9)) or not math.isfinite(float(ctx.ema21)):
            reasons.append("15m_context_invalid")
        else:
            context = "BULLISH" if ctx.ema9 > ctx.ema21 else "BEARISH" if ctx.ema9 < ctx.ema21 else "NEUTRAL"
            if (side == "LONG" and context != "BULLISH") or (side == "SHORT" and context != "BEARISH"):
                reasons.append("15m_trend_veto")
    if not reasons:
        reasons.append("strategy_rejected_unclassified")
    entry = values["close"]
    risk = values["atr"] * ATR_STOP_MULT
    if not math.isfinite(risk) or risk <= 0:
        return None
    stop = entry - risk if side == "LONG" else entry + risk
    target = entry + risk * MIN_RR if side == "LONG" else entry - risk * MIN_RR
    timestamp = int(c.timestamp)
    close_series = df5["close"].astype(float)
    macd_line = close_series.ewm(span=12, adjust=False).mean() - close_series.ewm(span=26, adjust=False).mean()
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    volume_average = df5["volume"].rolling(20, min_periods=20).mean().iloc[-1]
    relative_volume = float(df5["volume"].iloc[-1] / volume_average) if math.isfinite(float(volume_average)) and volume_average > 0 else None
    snapshot = {
        "version": 1, **values, "relative_volume_20": relative_volume,
        "macd_line": float(macd_line.iloc[-1]), "macd_signal": float(macd_signal.iloc[-1]),
        "macd_histogram": float(macd_line.iloc[-1] - macd_signal.iloc[-1]),
        "ema_alignment_age_bars": alignment_age,
        "ema9_slope_atr_3bars": fast_slope, "ema21_slope_atr_3bars": slow_slope,
        "price_vwap_distance_atr": extension, "context_15m": context,
    }
    return {
        "symbol": "", "direction": side, "candle_open_ms": timestamp,
        "entry": entry, "stop": stop, "target": target, "risk_per_unit": risk,
        "snapshot": snapshot, "rejection_reasons": sorted(set(reasons)),
    }


class SignalJournal:
    def __init__(self, candidate_store):
        self.pool = candidate_store.pool

    async def initialize(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(JOURNAL_DDL)

    async def record_rejected(self, observation: dict[str, Any]) -> bool:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO journal_candidates(
                     symbol,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                     snapshot,rejection_reasons,last_observed_candle_open_ms)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$3)
                   ON CONFLICT(symbol,direction,candle_open_ms) DO NOTHING RETURNING id""",
                observation["symbol"], observation["direction"], observation["candle_open_ms"],
                observation["entry"], observation["stop"], observation["target"],
                observation["risk_per_unit"],
                json.dumps(observation["snapshot"], separators=(",", ":"), allow_nan=False),
                json.dumps(observation["rejection_reasons"], separators=(",", ":")),
            )
        return row is not None

    async def update_rejected_outcomes(self, symbol: str, candles: list[dict]) -> int:
        normalized = []
        for candle in candles:
            ts = int(candle.get("timestamp", 0))
            high, low = float(candle.get("high", 0)), float(candle.get("low", 0))
            if ts and math.isfinite(high) and math.isfinite(low) and high >= low > 0:
                normalized.append((ts, high, low))
        if not normalized:
            return 0
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                          best_favorable_r,worst_adverse_r,bars_observed,
                          last_observed_candle_open_ms,data_gap
                   FROM journal_candidates WHERE symbol=$1 AND status='OPEN'
                   ORDER BY candle_open_ms""", symbol)
            updated = 0
            for row in rows:
                side = row["direction"]
                entry, stop, target = float(row["entry"]), float(row["stop"]), float(row["target"])
                risk = float(row["risk_per_unit"])
                best, worst = float(row["best_favorable_r"]), float(row["worst_adverse_r"])
                bars, last_ts, gap = int(row["bars_observed"]), int(row["last_observed_candle_open_ms"]), bool(row["data_gap"])
                status, exit_ts, realized = "OPEN", None, None
                for ts, high, low in normalized:
                    if ts <= last_ts or ts <= int(row["candle_open_ms"]):
                        continue
                    if ts - last_ts != 300_000:
                        status, gap, exit_ts = "AMBIGUOUS", True, ts
                        bars += 1
                        last_ts = ts
                        break
                    favorable = max(0.0, (high-entry)/risk) if side == "LONG" else max(0.0, (entry-low)/risk)
                    adverse = max(0.0, (entry-low)/risk) if side == "LONG" else max(0.0, (high-entry)/risk)
                    best, worst = max(best, favorable), max(worst, adverse)
                    stop_hit = low <= stop if side == "LONG" else high >= stop
                    target_hit = high >= target if side == "LONG" else low <= target
                    bars += 1
                    last_ts = ts
                    if stop_hit and target_hit:
                        status, exit_ts = "AMBIGUOUS", ts
                    elif stop_hit:
                        status, exit_ts, realized = "STOP_LOSS", ts, -1.0
                    elif target_hit:
                        status, exit_ts = "TARGET", ts
                        realized = abs(target-entry)/risk
                    if status != "OPEN":
                        break
                if bars == int(row["bars_observed"]) and gap == bool(row["data_gap"]):
                    continue
                await conn.execute(
                    """UPDATE journal_candidates SET status=$2,best_favorable_r=$3,
                         worst_adverse_r=$4,bars_observed=$5,last_observed_candle_open_ms=$6,
                         data_gap=$7,exit_candle_open_ms=$8,realized_r=$9,updated_at=NOW()
                       WHERE id=$1 AND status='OPEN'""",
                    int(row["id"]), status, best, worst, bars, last_ts, gap, exit_ts, realized)
                updated += 1
        return updated

    async def add_feedback(self, signal_id: int, category: str, comment: str = "",
                           source: str = "dashboard") -> int:
        category = str(category).strip().upper()
        if category not in FEEDBACK_CATEGORIES:
            raise ValueError("unsupported feedback category")
        source = source if source in {"dashboard", "telegram", "api"} else "api"
        comment = str(comment or "").strip()[:1000]
        async with self.pool.acquire() as conn:
            signal = await conn.fetchrow(
                "SELECT id FROM signal_history WHERE id=$1 AND status='SENT'", int(signal_id))
            if signal is None:
                raise LookupError("delivered signal not found")
            row = await conn.fetchrow(
                """INSERT INTO journal_feedback(signal_id,category,comment,source)
                   VALUES($1,$2,$3,$4) RETURNING id""",
                int(signal_id), category, comment, source)
        return int(row["id"])

    async def record_manual_opportunity(self, symbol: str, direction: str,
                                       candle_open_ms: int, entry: float,
                                       stop: float, target: float, comment: str = "") -> int:
        symbol = str(symbol).strip().upper()
        direction = str(direction).strip().upper()
        entry, stop, target = float(entry), float(stop), float(target)
        if not symbol or len(symbol) > 40 or direction not in {"LONG", "SHORT"}:
            raise ValueError("symbol and LONG/SHORT direction are required")
        if not all(math.isfinite(v) and v > 0 for v in (entry, stop, target)):
            raise ValueError("entry, stop and target must be finite positive prices")
        if (direction == "LONG" and not stop < entry < target) or (direction == "SHORT" and not target < entry < stop):
            raise ValueError("stop and target must be on the correct side of entry")
        if int(candle_open_ms) <= 0 or int(candle_open_ms) % 300_000:
            raise ValueError("candle_open_ms must be a UTC-aligned 5-minute candle timestamp")
        risk = abs(entry-stop)
        snapshot = {"version": 1, "manual_comment": str(comment or "").strip()[:1000]}
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO journal_candidates(
                     symbol,direction,candle_open_ms,entry,stop,target,risk_per_unit,
                     snapshot,rejection_reasons,last_observed_candle_open_ms)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$3)
                   ON CONFLICT(symbol,direction,candle_open_ms) DO UPDATE
                     SET snapshot=journal_candidates.snapshot || EXCLUDED.snapshot
                   RETURNING id""",
                symbol, direction, int(candle_open_ms), entry, stop, target, risk,
                json.dumps(snapshot, separators=(",", ":")),
                json.dumps(["manual_missed_opportunity"]),
            )
        return int(row["id"])

    async def add_candidate_feedback(self, candidate_id: int, category: str,
                                     comment: str = "", source: str = "dashboard") -> int:
        category = str(category).strip().upper()
        if category not in {"MISSED_OPPORTUNITY", "NEEDS_REVIEW"}:
            raise ValueError("candidate feedback must be MISSED_OPPORTUNITY or NEEDS_REVIEW")
        source = source if source in {"dashboard", "telegram", "api"} else "api"
        comment = str(comment or "").strip()[:1000]
        async with self.pool.acquire() as conn:
            candidate = await conn.fetchrow("SELECT id FROM journal_candidates WHERE id=$1", int(candidate_id))
            if candidate is None:
                raise LookupError("rejected candidate not found")
            row = await conn.fetchrow(
                """INSERT INTO journal_candidate_feedback(candidate_id,category,comment,source)
                   VALUES($1,$2,$3,$4) RETURNING id""",
                int(candidate_id), category, comment, source)
        return int(row["id"])

    async def add_telegram_feedback(self, signal_id: int, category: str) -> int:
        return await self.add_feedback(signal_id, category, "", "telegram")

    async def relabel_signal(self, signal_id: int, category: str, comment: str = "") -> int:
        """Replace the latest manual label for a delivered signal, or create one."""
        category = str(category).strip().upper()
        if category not in FEEDBACK_CATEGORIES:
            raise ValueError("unsupported feedback category")
        comment = str(comment or "").strip()[:1000]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                signal = await conn.fetchrow(
                    "SELECT id FROM signal_history WHERE id=$1 AND status='SENT' FOR UPDATE",
                    int(signal_id),
                )
                if signal is None:
                    raise LookupError("delivered signal not found")
                latest = await conn.fetchrow(
                    """SELECT id FROM journal_feedback
                       WHERE signal_id=$1 ORDER BY created_at DESC,id DESC
                       LIMIT 1 FOR UPDATE""", int(signal_id))
                if latest:
                    row = await conn.fetchrow(
                        """UPDATE journal_feedback SET category=$2,comment=$3,
                               source='api',created_at=NOW()
                           WHERE id=$1 RETURNING id""",
                        int(latest["id"]), category, comment)
                else:
                    row = await conn.fetchrow(
                        """INSERT INTO journal_feedback(signal_id,category,comment,source)
                           VALUES($1,$2,$3,'api') RETURNING id""",
                        int(signal_id), category, comment)
        return int(row["id"])

    async def pending_entries(self, limit: int = 100) -> list[dict]:
        """Delivered signals that have never received a manual label."""
        limit = max(1, min(int(limit), 5000))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.id,s.symbol,s.direction,s.delivered_at,
                          s.payload->>'config_version' AS config_version,
                          o.status AS outcome_status,o.result,o.realized_r
                   FROM signal_history s
                   LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                     AND NOT EXISTS (
                       SELECT 1 FROM journal_feedback f WHERE f.signal_id=s.id
                     )
                   ORDER BY s.delivered_at DESC,s.id DESC LIMIT $1""", limit)
        return [
            {**dict(row),
             "delivered_at": row["delivered_at"].isoformat() if row["delivered_at"] else None}
            for row in rows
        ]

    async def stats(self, days: int = 30) -> dict:
        """Aggregate labels, outcomes and performance dimensions for a time window."""
        days = max(1, min(int(days), 365))
        async with self.pool.acquire() as conn:
            labels = await conn.fetch(
                """SELECT f.category,COUNT(*) AS count
                   FROM journal_feedback f
                   JOIN signal_history s ON s.id=f.signal_id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   GROUP BY f.category ORDER BY count DESC,f.category""", days)
            totals = await conn.fetchrow(
                """WITH latest_feedback AS (
                     SELECT signal_id,category FROM (
                       SELECT signal_id,category,
                              ROW_NUMBER() OVER (
                                PARTITION BY signal_id ORDER BY created_at DESC,id DESC
                              ) AS feedback_rank
                       FROM journal_feedback
                     ) ranked_feedback WHERE feedback_rank=1
                   ), labeled AS (
                     SELECT s.id,o.realized_r,o.best_favorable_r,o.worst_adverse_r,
                            f.category,
                            CASE WHEN f.category='GOOD_ENTRY' THEN 'WIN'
                                 WHEN f.category IN ('BAD_SIGNAL','LATE_ENTRY','FAKE_SIGNAL','WRONG_DIRECTION') THEN 'LOSS'
                                 ELSE o.result END AS effective_result
                     FROM signal_history s
                     LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                     LEFT JOIN latest_feedback f ON f.signal_id=s.id
                     WHERE s.status='SENT'
                       AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   )
                   SELECT COUNT(*) AS delivered,
                          COUNT(*) FILTER (WHERE effective_result='WIN') AS wins,
                          COUNT(*) FILTER (WHERE effective_result='LOSS') AS losses,
                          COUNT(*) FILTER (WHERE effective_result='TIMEOUT') AS timeouts,
                          COUNT(*) FILTER (WHERE effective_result IS NULL) AS unresolved,
                          COUNT(*) FILTER (WHERE category IN ('FAKE_SIGNAL','BAD_SIGNAL','LATE_ENTRY')) AS false_labels,
                          COUNT(*) FILTER (WHERE category IN ('GOOD_ENTRY','BAD_SIGNAL','LATE_ENTRY','FAKE_SIGNAL')) AS manual_labels,
                          ROUND(AVG(realized_r)::numeric,3) AS avg_net_realized_r,
                          ROUND(AVG(best_favorable_r)::numeric,3) AS avg_mfe_r,
                          ROUND(AVG(worst_adverse_r)::numeric,3) AS avg_mae_r
                   FROM labeled""", days)
            by_side = await conn.fetch(
                """SELECT s.direction,COUNT(*) AS signals,
                          COUNT(*) FILTER (WHERE o.result='WIN') AS auto_wins,
                          COUNT(*) FILTER (WHERE o.result='LOSS') AS auto_losses,
                          ROUND(AVG(o.realized_r)::numeric,3) AS avg_net_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   GROUP BY s.direction ORDER BY s.direction""", days)
            by_symbol = await conn.fetch(
                """SELECT s.symbol,s.direction,COUNT(*) AS signals,
                          COUNT(*) FILTER (WHERE o.result='WIN') AS wins,
                          COUNT(*) FILTER (WHERE o.result='LOSS') AS losses,
                          COUNT(*) FILTER (WHERE o.result='TIMEOUT') AS timeouts,
                          ROUND(AVG(o.realized_r)::numeric,3) AS avg_net_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   GROUP BY s.symbol,s.direction ORDER BY s.symbol,s.direction""", days)
            by_hour = await conn.fetch(
                """SELECT EXTRACT(HOUR FROM s.delivered_at AT TIME ZONE 'UTC')::integer AS hour_utc,
                          COUNT(*) AS signals,ROUND(AVG(o.realized_r)::numeric,3) AS avg_net_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   GROUP BY hour_utc ORDER BY hour_utc""", days)
            by_config = await conn.fetch(
                """SELECT COALESCE(s.payload->>'config_version','unknown') AS config_version,
                          COUNT(*) AS signals,ROUND(AVG(o.realized_r)::numeric,3) AS avg_net_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-($1 * INTERVAL '1 day')
                   GROUP BY 1 ORDER BY 1""", days)
        totals_dict = dict(totals) if totals else {}
        manual = int(totals_dict.get("manual_labels") or 0)
        false_labels = int(totals_dict.get("false_labels") or 0)
        totals_dict["false_signal_rate"] = round(false_labels / manual, 4) if manual else None
        return {
            "window_days": days,
            "totals": totals_dict,
            "labels": [dict(row) for row in labels],
            "by_side": [dict(row) for row in by_side],
            "by_symbol": [dict(row) for row in by_symbol],
            "by_hour_utc": [dict(row) for row in by_hour],
            "by_config_version": [dict(row) for row in by_config],
        }

    async def entries(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 5000))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.id,s.symbol,s.direction,s.created_at,s.delivered_at,
                          s.payload::text AS payload,o.status AS outcome_status,
                          o.first_hit,o.result,o.realized_r,o.best_favorable_r,o.worst_adverse_r,
                          o.best_favorable_pct,o.worst_adverse_pct,
                          o.best_favorable_atr,o.worst_adverse_atr,
                          (SELECT json_agg(json_build_object(
                              'id',f.id,'category',f.category,'comment',f.comment,
                              'source',f.source,'created_at',f.created_at)
                              ORDER BY f.created_at DESC)
                           FROM journal_feedback f WHERE f.signal_id=s.id) AS feedback
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                   ORDER BY s.delivered_at DESC,s.id DESC LIMIT $1""", limit)
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload"))
            item["created_at"] = item["created_at"].isoformat() if item["created_at"] else None
            item["delivered_at"] = item["delivered_at"].isoformat() if item["delivered_at"] else None
            item["feedback"] = json.loads(item["feedback"]) if item["feedback"] else []
            for feedback in item["feedback"]:
                if feedback.get("created_at"):
                    feedback["created_at"] = feedback["created_at"].isoformat()
            result.append(item)
        return result

    async def candidates(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 250))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,symbol,direction,candle_open_ms,entry,stop,target,
                          snapshot::text AS snapshot,rejection_reasons::text AS rejection_reasons,
                          status,best_favorable_r,worst_adverse_r,bars_observed,
                          data_gap,realized_r,created_at,
                          (SELECT json_agg(json_build_object('id',f.id,'category',f.category,
                              'comment',f.comment,'source',f.source,'created_at',f.created_at)
                              ORDER BY f.created_at DESC)::text
                           FROM journal_candidate_feedback f WHERE f.candidate_id=journal_candidates.id) AS feedback
                   FROM journal_candidates ORDER BY created_at DESC,id DESC LIMIT $1""", limit)
        result = []
        for row in rows:
            item = dict(row)
            item["snapshot"] = json.loads(item["snapshot"])
            item["rejection_reasons"] = json.loads(item["rejection_reasons"])
            item["feedback"] = json.loads(item["feedback"]) if item["feedback"] else []
            item["created_at"] = item["created_at"].isoformat()
            result.append(item)
        return result

    async def add_training_instruction(self, raw_text: str) -> dict:
        from training_rules import parse_instruction
        parsed = parse_instruction(raw_text)
        clean = " ".join(str(raw_text).strip().split())
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO training_instructions(raw_text,parsed)
                   VALUES($1,$2::jsonb) RETURNING id,raw_text,parsed,status,created_at""",
                clean, json.dumps(parsed, separators=(",", ":"), allow_nan=False))
        return {"id": int(row["id"]), "raw_text": row["raw_text"],
                "parsed": json.loads(row["parsed"]) if isinstance(row["parsed"], str) else dict(row["parsed"]),
                "status": row["status"], "created_at": row["created_at"].isoformat()}

    async def training_instructions(self, limit: int = 100) -> list[dict]:
        limit = max(1, min(int(limit), 250))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,raw_text,parsed::text AS parsed,status,reviewer_note,created_at,reviewed_at
                   FROM training_instructions ORDER BY created_at DESC,id DESC LIMIT $1""", limit)
        return [{"id": int(r["id"]), "raw_text": r["raw_text"], "parsed": json.loads(r["parsed"]),
                 "status": r["status"], "reviewer_note": r["reviewer_note"],
                 "created_at": r["created_at"].isoformat(),
                 "reviewed_at": r["reviewed_at"].isoformat() if r["reviewed_at"] else None} for r in rows]

    async def review_training_instruction(self, instruction_id: int, decision: str,
                                          reviewer_note: str = "") -> dict:
        decision = str(decision).strip().upper()
        if decision not in {"APPROVED_RESEARCH", "REJECTED"}:
            raise ValueError("decision must be APPROVED_RESEARCH or REJECTED")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE training_instructions SET status=$2,reviewer_note=$3,reviewed_at=NOW()
                   WHERE id=$1 RETURNING id,status,reviewed_at""",
                int(instruction_id), decision, str(reviewer_note or "").strip()[:1000])
        if row is None:
            raise LookupError("training instruction not found")
        return {"id": int(row["id"]), "status": row["status"],
                "reviewed_at": row["reviewed_at"].isoformat(),
                "live_strategy_changed": False}

    async def train(self) -> dict:
        """Run the bounded offline trainer and persist only valid proposals."""
        from config import settings, CONFIG_VERSION
        from trainer import build_training_proposal

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.id AS signal_id,s.direction,s.candle_open_ms,
                          s.payload::text AS payload,o.result AS auto_result,
                          o.realized_r,
                          (SELECT f.category FROM journal_feedback f
                           WHERE f.signal_id=s.id
                           ORDER BY f.created_at DESC,f.id DESC LIMIT 1) AS manual_label
                   FROM signal_history s
                   JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                     AND o.status IN ('TARGET','STOP_LOSS','TIMEOUT')
                     AND o.result IN ('WIN','LOSS','TIMEOUT')
                     AND o.realized_r IS NOT NULL
                   ORDER BY s.candle_open_ms DESC,s.id DESC
                   LIMIT 10000"""
            )
        dataset = []
        for row in reversed(rows):
            payload = json.loads(row["payload"]) if row["payload"] else {}
            features = dict(payload.get("indicator_snapshot") or payload.get("diagnostics") or {})
            ema9 = features.get("ema9")
            ema21 = features.get("ema21")
            close = features.get("close")
            vwap = features.get("vwap")
            try:
                if ema9 is not None and ema21 not in (None, 0):
                    features["ema_spread_pct"] = abs(float(ema9)-float(ema21)) / abs(float(ema21)) * 100
                if close is not None and vwap not in (None, 0):
                    features["price_vwap_distance_pct"] = (float(close)-float(vwap)) / abs(float(vwap)) * 100
            except (TypeError, ValueError, ZeroDivisionError):
                pass
            features["hour_utc"] = (int(row["candle_open_ms"]) // 3_600_000) % 24
            dataset.append({
                "signal_id": int(row["signal_id"]),
                "direction": row["direction"],
                "candle_open_ms": int(row["candle_open_ms"]),
                "features": features,
                "auto_result": row["auto_result"],
                "realized_r": float(row["realized_r"]),
                "manual_label": row["manual_label"],
            })

        current = {
            "adx_min": settings.adx_min,
            "rsi_long_min": settings.rsi_long_min,
            "rsi_long_max": settings.rsi_long_max,
            "rsi_short_min": settings.rsi_short_min,
            "rsi_short_max": settings.rsi_short_max,
        }
        proposal = await asyncio.to_thread(
            build_training_proposal, dataset, current,
            base_config_version=CONFIG_VERSION,
            min_samples=settings.training_min_samples,
            min_bucket_samples=settings.training_min_bucket_samples,
        )
        if proposal.get("status") != "PROPOSED":
            return proposal

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO training_proposals(
                     base_config_version,proposed_config_version,proposed_config,changes,metrics
                   ) VALUES($1,$2,$3::jsonb,$4::jsonb,$5::jsonb) RETURNING id""",
                proposal["base_config_version"], proposal["proposed_config_version"],
                json.dumps(proposal["proposed_config"], separators=(",", ":"), allow_nan=False),
                json.dumps(proposal["changes"], separators=(",", ":"), allow_nan=False),
                json.dumps({
                    "summary": proposal["summary"],
                    "expected_effect": proposal["expected_effect"],
                    "confidence_note": proposal["confidence_note"],
                }, separators=(",", ":"), allow_nan=False),
            )
        proposal["proposal_id"] = int(row["id"])
        return proposal

    async def start_shadow_proposal(self, proposal_id: int, actor: str,
                                   days: int = 3, reason: str = "Shadow test") -> dict:
        """Run a proposed config in parallel without changing live settings."""
        actor = str(actor or "unknown")[:120]
        reason = str(reason or "Shadow test")[:500]
        days = int(days)
        if not 1 <= days <= 30:
            raise ValueError("shadow duration must be between 1 and 30 days")
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                proposal = await conn.fetchrow(
                    """SELECT id,base_config_version,proposed_config_version,
                              proposed_config::text AS proposed_config,status
                       FROM training_proposals WHERE id=$1 FOR UPDATE""", int(proposal_id))
                if proposal is None:
                    raise LookupError("training proposal not found")
                if proposal["status"] != "PROPOSED":
                    raise ValueError("only a PROPOSED item can enter shadow mode")
                active = await conn.fetchrow(
                    """SELECT config_version,config::text AS config
                       FROM config_history WHERE status='ACTIVE' FOR UPDATE""")
                if active is None:
                    raise RuntimeError("active runtime config is not initialized")
                if active["config_version"] != proposal["base_config_version"]:
                    raise ValueError("proposal is stale; run /train again against the current config")
                expired_shadow = await conn.fetchrow(
                    """SELECT config_version,previous_version FROM config_history
                       WHERE status='SHADOW' AND shadow_until <= NOW() FOR UPDATE""")
                if expired_shadow:
                    await conn.execute(
                        """UPDATE config_history SET status='INACTIVE',deactivated_at=NOW()
                           WHERE config_version=$1""", expired_shadow["config_version"])
                    await conn.execute(
                        """UPDATE training_proposals SET status='PROPOSED',
                               reviewer_note='Shadow period expired; awaiting human approval',
                               reviewed_at=NULL
                           WHERE proposed_config_version=$1 AND status='SHADOW'""",
                        expired_shadow["config_version"])
                    await conn.execute(
                        """INSERT INTO config_history_events(
                             action,config_version,previous_version,actor,reason)
                           VALUES('SHADOW_EXPIRE',$1,$2,'system','Configured shadow duration elapsed')""",
                        expired_shadow["config_version"], expired_shadow["previous_version"])
                existing_shadow = await conn.fetchrow(
                    """SELECT config_version FROM config_history
                       WHERE status='SHADOW' AND shadow_until > NOW() FOR UPDATE""")
                if existing_shadow:
                    raise ValueError(f"shadow config {existing_shadow['config_version']} is already running")
                current = json.loads(active["config"]) if isinstance(active["config"], str) else dict(active["config"])
                delta = json.loads(proposal["proposed_config"])
                merged = {**current, **delta}
                versions = await conn.fetch("SELECT config_version FROM config_history")
                max_number = max(
                    (int(str(row["config_version"]).lstrip("v"))
                     for row in versions if str(row["config_version"]).lstrip("v").isdigit()),
                    default=1,
                )
                version = proposal["proposed_config_version"]
                if any(row["config_version"] == version for row in versions):
                    version = f"v{max_number + 1}"
                await conn.execute(
                    """INSERT INTO config_history(
                         config_version,config,status,previous_version,approved_by,reason,shadow_until)
                       VALUES($1,$2::jsonb,'SHADOW',$3,$4,$5,NOW()+($6 * INTERVAL '1 day'))""",
                    version, json.dumps(merged, separators=(",", ":"), allow_nan=False),
                    active["config_version"], actor, reason, days)
                await conn.execute(
                    """UPDATE training_proposals SET status='SHADOW',
                           proposed_config_version=$2,reviewer_note=$3,reviewed_at=NOW()
                       WHERE id=$1""", int(proposal_id), version, reason)
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('SHADOW_START',$1,$2,$3,$4)""",
                    version, active["config_version"], actor, f"{reason}; duration={days}d")
                shadow = await conn.fetchrow(
                    """SELECT shadow_until FROM config_history WHERE config_version=$1""", version)
        return {"id": int(proposal_id), "version": version, "config": merged,
                "base_version": active["config_version"], "shadow_until": shadow["shadow_until"].isoformat()}

    async def active_shadow_config(self) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT c.config_version,c.config::text AS config,c.shadow_until,
                          p.id AS proposal_id
                   FROM config_history c
                   JOIN training_proposals p ON p.proposed_config_version=c.config_version
                   WHERE c.status='SHADOW' AND c.shadow_until > NOW() AND p.status='SHADOW'
                   ORDER BY c.created_at DESC LIMIT 1""")
        if row is None:
            return None
        config = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"])
        return {"version": row["config_version"], "config": config,
                "proposal_id": int(row["proposal_id"]),
                "shadow_until": row["shadow_until"].isoformat()}

    async def shadow_signal_counts(self, proposal_version: str) -> dict:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT COUNT(*) AS total,
                          COUNT(*) FILTER (WHERE direction='LONG') AS longs,
                          COUNT(*) FILTER (WHERE direction='SHORT') AS shorts,
                          COUNT(DISTINCT symbol) AS symbols
                   FROM shadow_signal_history WHERE proposal_version=$1""",
                str(proposal_version))
        return {key: int(row[key] or 0) for key in ("total", "longs", "shorts", "symbols")}

    async def stop_shadow(self, actor: str, reason: str = "Shadow test stopped") -> dict:
        actor = str(actor or "unknown")[:120]
        reason = str(reason or "Shadow test stopped")[:500]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                shadow = await conn.fetchrow(
                    """SELECT config_version,previous_version FROM config_history
                       WHERE status='SHADOW' FOR UPDATE""")
                if shadow is None:
                    raise LookupError("no shadow configuration is running")
                await conn.execute(
                    """UPDATE config_history SET status='INACTIVE',deactivated_at=NOW()
                       WHERE config_version=$1""", shadow["config_version"])
                await conn.execute(
                    """UPDATE training_proposals SET status='PROPOSED',
                           reviewer_note=$2,reviewed_at=NULL
                       WHERE proposed_config_version=$1 AND status='SHADOW'""",
                    shadow["config_version"], reason)
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('SHADOW_STOP',$1,$2,$3,$4)""",
                    shadow["config_version"], shadow["previous_version"], actor, reason)
        return {"version": shadow["config_version"], "status": "STOPPED"}

    async def expire_shadow_if_needed(self) -> bool:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                shadow = await conn.fetchrow(
                    """SELECT config_version,previous_version FROM config_history
                       WHERE status='SHADOW' AND shadow_until <= NOW() FOR UPDATE""")
                if shadow is None:
                    return False
                await conn.execute(
                    """UPDATE config_history SET status='INACTIVE',deactivated_at=NOW()
                       WHERE config_version=$1""", shadow["config_version"])
                await conn.execute(
                    """UPDATE training_proposals SET status='PROPOSED',
                           reviewer_note='Shadow period expired; awaiting human approval',
                           reviewed_at=NULL
                       WHERE proposed_config_version=$1 AND status='SHADOW'""",
                    shadow["config_version"])
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('SHADOW_EXPIRE',$1,$2,'system','Configured shadow duration elapsed')""",
                    shadow["config_version"], shadow["previous_version"])
        return True

    async def ensure_initial_config(self, version: str, config: dict) -> None:
        """Seed the active config once; never overwrite an approved config at boot."""
        encoded = json.dumps(config, separators=(",", ":"), allow_nan=False)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                active = await conn.fetchrow(
                    "SELECT config_version FROM config_history WHERE status='ACTIVE' FOR UPDATE")
                if active:
                    return
                existing = await conn.fetchrow(
                    "SELECT config_version FROM config_history WHERE config_version=$1 FOR UPDATE",
                    version)
                if existing:
                    await conn.execute(
                        """UPDATE config_history SET status='ACTIVE',activated_at=NOW(),
                               deactivated_at=NULL WHERE config_version=$1""", version)
                else:
                    await conn.execute(
                        """INSERT INTO config_history(
                             config_version,config,status,approved_by,reason,activated_at)
                           VALUES($1,$2::jsonb,'ACTIVE','system','Initial runtime configuration',NOW())""",
                        version, encoded)
                await conn.execute(
                    """INSERT INTO config_history_events(action,config_version,actor,reason)
                       VALUES('INITIALIZE',$1,'system','Initial runtime configuration')""", version)

    async def active_config(self) -> dict | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT config_version,config::text AS config
                   FROM config_history WHERE status='ACTIVE' LIMIT 1""")
        if row is None:
            return None
        config = json.loads(row["config"]) if isinstance(row["config"], str) else dict(row["config"])
        return {"version": row["config_version"], "config": config}

    async def config_history(self, limit: int = 30) -> dict:
        limit = max(1, min(int(limit), 100))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT config_version,status,previous_version,approved_by,reason,
                          created_at,activated_at,deactivated_at,shadow_until
                   FROM config_history ORDER BY created_at DESC,config_version DESC LIMIT $1""",
                limit)
            events = await conn.fetch(
                """SELECT action,config_version,previous_version,actor,reason,created_at
                   FROM config_history_events ORDER BY created_at DESC,id DESC LIMIT $1""",
                limit)
        return {
            "versions": [
                {**dict(row),
                 "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                 "activated_at": row["activated_at"].isoformat() if row["activated_at"] else None,
                 "deactivated_at": row["deactivated_at"].isoformat() if row["deactivated_at"] else None,
                 "shadow_until": row["shadow_until"].isoformat() if row["shadow_until"] else None}
                for row in rows
            ],
            "events": [
                {**dict(row), "created_at": row["created_at"].isoformat() if row["created_at"] else None}
                for row in events
            ],
        }

    async def approve_proposal(self, proposal_id: int, actor: str,
                               reason: str = "Approved via Telegram") -> dict:
        actor = str(actor or "unknown")[:120]
        reason = str(reason or "Approved via Telegram")[:500]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                proposal = await conn.fetchrow(
                    """SELECT id,base_config_version,proposed_config_version,
                              proposed_config::text AS proposed_config,status
                       FROM training_proposals WHERE id=$1 FOR UPDATE""", int(proposal_id))
                if proposal is None:
                    raise LookupError("training proposal not found")
                if proposal["status"] != "PROPOSED":
                    raise ValueError("only a PROPOSED item can be approved")
                shadow = await conn.fetchrow(
                    """SELECT config_version FROM config_history
                       WHERE status='SHADOW' AND shadow_until > NOW() FOR UPDATE""")
                if shadow:
                    raise ValueError("stop the active shadow test before approving another proposal")
                active = await conn.fetchrow(
                    """SELECT config_version,config::text AS config
                       FROM config_history WHERE status='ACTIVE' FOR UPDATE""")
                if active is None:
                    raise RuntimeError("active runtime config is not initialized")
                if active["config_version"] != proposal["base_config_version"]:
                    raise ValueError("proposal is stale; run /train again against the current config")
                current_config = json.loads(active["config"]) if isinstance(active["config"], str) else dict(active["config"])
                proposed_delta = json.loads(proposal["proposed_config"])
                merged_config = {**current_config, **proposed_delta}
                versions = await conn.fetch("SELECT config_version FROM config_history")
                max_number = max(
                    (int(str(row["config_version"]).lstrip("v"))
                     for row in versions if str(row["config_version"]).lstrip("v").isdigit()),
                    default=1,
                )
                proposed_version = proposal["proposed_config_version"]
                if any(row["config_version"] == proposed_version for row in versions):
                    proposed_version = f"v{max_number + 1}"
                await conn.execute(
                    """UPDATE config_history SET status='INACTIVE',deactivated_at=NOW()
                       WHERE config_version=$1""", active["config_version"])
                await conn.execute(
                    """INSERT INTO config_history(
                         config_version,config,status,previous_version,approved_by,reason,activated_at)
                       VALUES($1,$2::jsonb,'ACTIVE',$3,$4,$5,NOW())""",
                    proposed_version,
                    json.dumps(merged_config, separators=(",", ":"), allow_nan=False),
                    active["config_version"], actor, reason)
                await conn.execute(
                    """UPDATE training_proposals SET status='APPROVED',
                           proposed_config_version=$2,reviewer_note=$3,reviewed_at=NOW()
                       WHERE id=$1""", int(proposal_id), proposed_version, reason)
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('APPROVE',$1,$2,$3,$4)""",
                    proposed_version, active["config_version"], actor, reason)
        return {"version": proposed_version, "config": merged_config,
                "previous_version": active["config_version"]}

    async def reject_proposal(self, proposal_id: int, actor: str,
                              reason: str = "Rejected via Telegram") -> dict:
        actor = str(actor or "unknown")[:120]
        reason = str(reason or "Rejected via Telegram")[:500]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                proposal = await conn.fetchrow(
                    """SELECT id,base_config_version,proposed_config_version,status
                       FROM training_proposals WHERE id=$1 FOR UPDATE""", int(proposal_id))
                if proposal is None:
                    raise LookupError("training proposal not found")
                if proposal["status"] != "PROPOSED":
                    raise ValueError("only a PROPOSED item can be rejected")
                await conn.execute(
                    """UPDATE training_proposals SET status='REJECTED',
                           reviewer_note=$2,reviewed_at=NOW() WHERE id=$1""",
                    int(proposal_id), reason)
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('REJECT',$1,$2,$3,$4)""",
                    proposal["proposed_config_version"], proposal["base_config_version"],
                    actor, reason)
        return {"id": int(proposal_id), "status": "REJECTED"}

    async def rollback_config(self, actor: str, reason: str = "Rollback via Telegram") -> dict:
        actor = str(actor or "unknown")[:120]
        reason = str(reason or "Rollback via Telegram")[:500]
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                active = await conn.fetchrow(
                    """SELECT config_version,config::text AS config,previous_version
                       FROM config_history WHERE status='ACTIVE' FOR UPDATE""")
                shadow = await conn.fetchrow(
                    """SELECT config_version FROM config_history
                       WHERE status='SHADOW' AND shadow_until > NOW() FOR UPDATE""")
                if shadow:
                    raise ValueError("stop the active shadow test before rolling back live settings")
                if active is None or not active["previous_version"]:
                    raise LookupError("no previous approved configuration is available")
                previous = await conn.fetchrow(
                    """SELECT config_version,config::text AS config
                       FROM config_history WHERE config_version=$1 AND status='INACTIVE' FOR UPDATE""",
                    active["previous_version"])
                if previous is None:
                    raise LookupError("previous configuration record is missing")
                await conn.execute(
                    """UPDATE config_history SET status='INACTIVE',deactivated_at=NOW()
                       WHERE config_version=$1""", active["config_version"])
                await conn.execute(
                    """UPDATE config_history SET status='ACTIVE',activated_at=NOW(),
                           deactivated_at=NULL,approved_by=$2,reason=$3
                       WHERE config_version=$1""",
                    previous["config_version"], actor, reason)
                await conn.execute(
                    """UPDATE training_proposals SET status='ROLLED_BACK',
                           reviewer_note=$2,reviewed_at=NOW()
                       WHERE proposed_config_version=$1 AND status='APPROVED'""",
                    active["config_version"], reason)
                await conn.execute(
                    """INSERT INTO config_history_events(
                         action,config_version,previous_version,actor,reason)
                       VALUES('ROLLBACK',$1,$2,$3,$4)""",
                    previous["config_version"], active["config_version"], actor, reason)
        config = json.loads(previous["config"]) if isinstance(previous["config"], str) else dict(previous["config"])
        return {"version": previous["config_version"], "config": config,
                "previous_version": active["config_version"]}

    async def training_proposals(self, limit: int = 20) -> list[dict]:
        limit = max(1, min(int(limit), 100))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,base_config_version,proposed_config_version,
                          proposed_config::text AS proposed_config,
                          changes::text AS changes,metrics::text AS metrics,
                          status,reviewer_note,created_at,reviewed_at
                   FROM training_proposals
                   ORDER BY created_at DESC,id DESC LIMIT $1""", limit)
        result = []
        for row in rows:
            item = dict(row)
            for field in ("proposed_config", "changes", "metrics"):
                item[field] = json.loads(item[field]) if item[field] else {}
            item["created_at"] = item["created_at"].isoformat() if item["created_at"] else None
            item["reviewed_at"] = item["reviewed_at"].isoformat() if item["reviewed_at"] else None
            result.append(item)
        return result

    async def build_weekly_digest(self, window_start: datetime | None = None,
                                  window_end: datetime | None = None) -> dict:
        """Summarize a completed UTC week and its weakest sufficiently sampled feature bucket."""
        from config import settings
        from trainer import _analysis_buckets, prepare_samples

        window_end = window_end or datetime.now(timezone.utc)
        window_start = window_start or (window_end - timedelta(days=7))
        if window_start.tzinfo is None:
            window_start = window_start.replace(tzinfo=timezone.utc)
        if window_end.tzinfo is None:
            window_end = window_end.replace(tzinfo=timezone.utc)
        if window_start >= window_end:
            raise ValueError("weekly digest window_start must be earlier than window_end")
        days = max(1, min(14, int((window_end-window_start).total_seconds() / 86400)))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT s.id AS signal_id,s.direction,s.candle_open_ms,
                          s.payload::text AS payload,o.result AS auto_result,o.realized_r,
                          (SELECT f.category FROM journal_feedback f
                           WHERE f.signal_id=s.id
                           ORDER BY f.created_at DESC,f.id DESC LIMIT 1) AS manual_label
                   FROM signal_history s
                   JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                     AND s.delivered_at >= $1 AND s.delivered_at < $2
                     AND o.status IN ('TARGET','STOP_LOSS','TIMEOUT')
                     AND o.result IN ('WIN','LOSS','TIMEOUT')
                     AND o.realized_r IS NOT NULL
                   ORDER BY s.candle_open_ms,s.id""", window_start, window_end)
            symbols = await conn.fetch(
                """SELECT s.symbol,COUNT(*) AS resolved,
                          COUNT(*) FILTER (WHERE o.result='WIN') AS wins,
                          COUNT(*) FILTER (WHERE o.result='LOSS') AS losses,
                          COUNT(*) FILTER (WHERE o.result='TIMEOUT') AS timeouts,
                          ROUND(AVG(o.realized_r)::numeric,3) AS expectancy_r
                   FROM signal_history s JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'
                     AND s.delivered_at >= $1 AND s.delivered_at < $2
                     AND o.status IN ('TARGET','STOP_LOSS','TIMEOUT')
                     AND o.realized_r IS NOT NULL
                   GROUP BY s.symbol HAVING COUNT(*) >= 3
                   ORDER BY expectancy_r DESC,s.symbol""", window_start, window_end)
        dataset = []
        for row in rows:
            payload = json.loads(row["payload"]) if row["payload"] else {}
            features = dict(payload.get("indicator_snapshot") or payload.get("diagnostics") or {})
            ema9, ema21 = features.get("ema9"), features.get("ema21")
            close, vwap = features.get("close"), features.get("vwap")
            try:
                if ema9 is not None and ema21 not in (None, 0):
                    features["ema_spread_pct"] = abs(float(ema9)-float(ema21)) / abs(float(ema21)) * 100
                if close is not None and vwap not in (None, 0):
                    features["price_vwap_distance_pct"] = (float(close)-float(vwap)) / abs(float(vwap)) * 100
            except (TypeError, ValueError, ZeroDivisionError):
                pass
            features["hour_utc"] = (int(row["candle_open_ms"]) // 3_600_000) % 24
            dataset.append({
                "signal_id": int(row["signal_id"]),
                "direction": row["direction"],
                "candle_open_ms": int(row["candle_open_ms"]),
                "features": features,
                "auto_result": row["auto_result"],
                "realized_r": float(row["realized_r"]),
                "manual_label": row["manual_label"],
            })
        samples, conflicts = prepare_samples(dataset)
        buckets = _analysis_buckets(samples, settings.training_min_bucket_samples)
        weakest = None
        for parameter, analysis in buckets.items():
            for item in analysis["buckets"]:
                metrics = item.get("metrics")
                if not metrics or metrics.get("expectancy_r") is None:
                    continue
                candidate = {
                    "parameter": parameter, "bucket": item["bucket"],
                    "sample_count": item["sample_count"],
                    "expectancy_r": metrics["expectancy_r"],
                    "win_rate": metrics["win_rate"],
                    "false_signal_rate": metrics["false_signal_rate"],
                }
                if weakest is None or candidate["expectancy_r"] < weakest["expectancy_r"]:
                    weakest = candidate
        eligible_symbols = [dict(row) for row in symbols]
        return {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "window_days": round((window_end-window_start).total_seconds()/86400, 2),
            "resolved_sample": len(samples),
            "manual_label_conflicts": conflicts,
            "totals": await self.stats(days),
            "best_symbols": eligible_symbols[:3],
            "worst_symbols": list(reversed(eligible_symbols[-3:])),
            "weakest_parameter_bucket": weakest,
            "confidence_note": (
                "Descriptive weekly review only. Small samples are excluded from symbol rankings and feature buckets. "
                "This report never changes live thresholds."
            ),
            "live_strategy_changed": False,
            "orders_enabled": False,
        }

    async def save_weekly_digest(self, summary: dict,
                                 window_start: datetime,
                                 window_end: datetime) -> int:
        encoded = json.dumps(summary, default=str, separators=(",", ":"), allow_nan=False)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """INSERT INTO training_weekly_digests(window_start,window_end,summary)
                   VALUES($1,$2,$3::jsonb)
                   ON CONFLICT(window_start,window_end) DO NOTHING RETURNING id""",
                window_start, window_end, encoded)
            if row:
                return int(row["id"])
            existing = await conn.fetchrow(
                """SELECT id FROM training_weekly_digests
                   WHERE window_start=$1 AND window_end=$2""", window_start, window_end)
        return int(existing["id"])

    async def pending_weekly_digests(self, limit: int = 5) -> list[dict]:
        limit = max(1, min(int(limit), 20))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT id,summary::text AS summary,attempts
                   FROM training_weekly_digests WHERE status='PENDING'
                   ORDER BY created_at,id LIMIT $1""", limit)
        return [
            {"id": int(row["id"]),
             "summary": json.loads(row["summary"]) if isinstance(row["summary"], str) else dict(row["summary"]),
             "attempts": int(row["attempts"])}
            for row in rows
        ]

    async def mark_weekly_digest_sent(self, digest_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE training_weekly_digests SET status='SENT',
                       attempts=attempts+1,sent_at=NOW()
                   WHERE id=$1 AND status='PENDING'""", int(digest_id))

    async def mark_weekly_digest_attempt(self, digest_id: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE training_weekly_digests SET attempts=attempts+1
                   WHERE id=$1 AND status='PENDING'""", int(digest_id))

    async def daily_training_review(self) -> dict:
        async with self.pool.acquire() as conn:
            totals = await conn.fetchrow(
                """SELECT COUNT(*) AS delivered,
                   COUNT(*) FILTER (WHERE o.status IN ('TARGET','STOP_LOSS','TIMEOUT')) AS resolved,
                   COUNT(*) FILTER (WHERE o.status='OPEN') AS open,
                   COUNT(*) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                   ROUND(AVG(o.realized_r)::numeric,3) AS avg_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT' AND s.delivered_at >= NOW()-INTERVAL '24 hours'""")
            feedback = await conn.fetch(
                """SELECT f.category,COUNT(*) AS count FROM journal_feedback f
                   JOIN signal_history s ON s.id=f.signal_id
                   WHERE f.created_at >= NOW()-INTERVAL '24 hours'
                   GROUP BY f.category ORDER BY count DESC""")
            candidate_labels = await conn.fetch(
                """SELECT category,COUNT(*) AS count FROM journal_candidate_feedback
                   WHERE created_at >= NOW()-INTERVAL '24 hours'
                   GROUP BY category ORDER BY count DESC""")
            instructions = await conn.fetch(
                """SELECT status,COUNT(*) AS count FROM training_instructions
                   WHERE created_at >= NOW()-INTERVAL '24 hours' GROUP BY status""")
            missed = await conn.fetch(
                """SELECT COUNT(*) AS count,ROUND(AVG(best_favorable_r)::numeric,3) AS avg_mfe_r
                   FROM journal_candidates WHERE created_at >= NOW()-INTERVAL '24 hours'
                   AND rejection_reasons ? 'manual_missed_opportunity'""")
        resolved = int(totals["resolved"] or 0)
        return {
            "window": "rolling_24_hours", "generated_at": datetime.now(timezone.utc).isoformat(),
            "signals": dict(totals), "signal_feedback": [dict(x) for x in feedback],
            "candidate_feedback": [dict(x) for x in candidate_labels],
            "training_instruction_status": [dict(x) for x in instructions],
            "manual_missed_opportunities": dict(missed),
            "decision": "INSUFFICIENT_DATA" if resolved < 30 else "RESEARCH_REPLAY_REQUIRED",
            "resolved_sample": resolved,
            "next_step": "Keep collecting labelled outcomes; do not tune live rules." if resolved < 30
                         else "Run chronological walk-forward replay with fees, funding, spread and slippage.",
            "live_strategy_changed": False, "orders_enabled": False,
        }

    async def save_daily_training_review(self, summary: dict) -> None:
        window_end = datetime.now(timezone.utc)
        window_start = window_end - timedelta(hours=24)
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO training_daily_reviews(window_start,window_end,summary)
                   VALUES($1,$2,$3::jsonb) ON CONFLICT(window_start,window_end) DO NOTHING""",
                window_start, window_end,
                json.dumps(summary, default=str, separators=(",", ":"), allow_nan=False))

    async def learning_summary(self) -> dict:
        async with self.pool.acquire() as conn:
            totals = await conn.fetchrow(
                """SELECT COUNT(*) AS delivered_signals,
                          COUNT(*) FILTER (WHERE o.status='TARGET') AS targets,
                          COUNT(*) FILTER (WHERE o.status='STOP_LOSS') AS stop_losses,
                          COUNT(*) FILTER (WHERE o.status='TIMEOUT') AS timeouts,
                          COUNT(*) FILTER (WHERE o.status='OPEN') AS open,
                          COUNT(*) FILTER (WHERE o.status='AMBIGUOUS') AS ambiguous,
                          ROUND(AVG(o.realized_r)::numeric,3) AS avg_realized_r
                   FROM signal_history s LEFT JOIN signal_outcomes o ON o.signal_id=s.id
                   WHERE s.status='SENT'""")
            feedback = await conn.fetch(
                "SELECT category,COUNT(*) AS count FROM journal_feedback GROUP BY category ORDER BY count DESC,category")
            candidate_feedback = await conn.fetch(
                "SELECT category,COUNT(*) AS count FROM journal_candidate_feedback GROUP BY category ORDER BY count DESC,category")
            rejected = await conn.fetch(
                """SELECT status,COUNT(*) AS count,ROUND(AVG(best_favorable_r)::numeric,3) AS avg_mfe_r,
                          ROUND(AVG(worst_adverse_r)::numeric,3) AS avg_mae_r
                   FROM journal_candidates GROUP BY status ORDER BY status""")
            reasons = await conn.fetch(
                """SELECT r.reason,COUNT(*) AS count FROM journal_candidates c
                   CROSS JOIN LATERAL jsonb_array_elements_text(c.rejection_reasons) AS r(reason)
                   GROUP BY r.reason ORDER BY count DESC LIMIT 20""")
        sample = (int(totals["targets"] or 0) + int(totals["stop_losses"] or 0)
                  + int(totals["timeouts"] or 0))
        recommendations = []
        if sample < 30:
            recommendations.append("Collect at least 30 resolved delivered signals before judging live rule quality.")
        else:
            recommendations.append("Use chronological walk-forward replay before accepting any proposed threshold change.")
        if not feedback:
            recommendations.append("Add manual feedback to delivered signals to distinguish execution quality from candle outcome.")
        if rejected:
            recommendations.append("Compare rejected-candidate outcomes by rejection reason; do not weaken a filter from raw counts alone.")
        return {
            "delivered_signal_totals": dict(totals),
            "manual_feedback": [dict(x) for x in feedback],
            "candidate_feedback": [dict(x) for x in candidate_feedback],
            "rejected_candidate_outcomes": [dict(x) for x in rejected],
            "top_rejection_reasons": [dict(x) for x in reasons],
            "resolved_signal_sample": sample,
            "recommendations": recommendations,
            "automatic_strategy_changes": False,
            "warning": "Research and decision support only. No live strategy thresholds are modified.",
        }


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>CryptoByance Signal Journal</title>
<style>
body{font:15px system-ui,sans-serif;margin:0;background:#0b1220;color:#e5e7eb}main{max-width:1100px;margin:auto;padding:22px}
h1,h2{margin:8px 0 14px}input,button,textarea{font:inherit;padding:9px;border-radius:8px;border:1px solid #334155;background:#111c30;color:#e5e7eb}
button{cursor:pointer;background:#1d4ed8;border:0;margin:3px}.panel,.entry{background:#111c30;border:1px solid #26364f;border-radius:12px;padding:14px;margin:12px 0}
small,.muted{color:#9ca3af}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}
.entry{display:grid;grid-template-columns:1fr auto;gap:10px}.entry pre{white-space:pre-wrap;overflow-wrap:anywhere;color:#cbd5e1}
textarea{width:95%;min-height:42px}
</style></head><body><main>
<h1>CryptoByance — Signal Journal</h1>
<p class="muted">Delivered signals, outcomes, feedback and rejected setups. Research only; no orders or automatic strategy changes.</p>
<section class="panel"><label>Journal API token <input id="token" type="password" placeholder="Set JOURNAL_API_TOKEN"></label>
<button onclick="loadAll()">Load journal</button><span id="status" class="muted"></span></section>
<div class="grid"><section class="panel"><h2>Learning summary</h2><pre id="learning">Load journal to view.</pre></section>
<section class="panel"><h2>Rejected candidates</h2><pre id="candidates">Load journal to view.</pre></section></div>
<section class="panel"><h2>Teach the bot your preferred timing</h2>
<p class="muted">Write naturally. Example: “For LONG entries, prefer the first retest after MACD turns positive with relative volume above 1.5, between 09:00 and 12:00 IST.”</p>
<textarea id="training-instruction" placeholder="Describe the setup, direction, indicator confirmation or preferred time window..."></textarea>
<button onclick="submitTrainingInstruction()">Save training instruction</button>
<pre id="training-result">New instructions are saved as pending review. They do not change live signals.</pre></section>
<h2>Delivered signals</h2><div id="entries"></div>
<script>
const headers=()=>({'X-Journal-Token':document.getElementById('token').value,'Content-Type':'application/json'});
async function api(path,opts={}){const r=await fetch(path,{...opts,headers:{...headers(),...(opts.headers||{})}});if(!r.ok)throw new Error((await r.text()).slice(0,250)||r.status);return r.json()}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
async function loadAll(){const status=document.getElementById('status');status.textContent='Loading…';try{
 const results=await Promise.all([api('/journal/api/entries'),api('/journal/api/candidates'),api('/learning')]);
 const e=results[0],c=results[1],l=results[2];
 document.getElementById('learning').textContent=JSON.stringify(l,null,2);
 document.getElementById('candidates').innerHTML=c.slice(0,30).map(function(x){return '<article class="entry"><div><b>#'+x.id+' '+esc(x.symbol)+' '+esc(x.direction)+'</b><div class="muted">'+esc(x.status)+' · MFE='+esc(x.best_favorable_r)+'R · MAE='+esc(x.worst_adverse_r)+'R · '+esc((x.rejection_reasons||[]).join(', '))+'</div><pre>'+esc(JSON.stringify(x.snapshot,null,2))+'</pre>'+((x.feedback||[]).map(function(f){return '<small>'+esc(f.category)+' — '+esc(f.comment)+'</small><br>'}).join(''))+'</div><div><button onclick="candidateFeedback('+x.id+',\'MISSED_OPPORTUNITY\')">Missed opportunity</button><button onclick="candidateFeedback('+x.id+',\'NEEDS_REVIEW\')">Review</button></div></article>'}).join('');
 document.getElementById('entries').innerHTML=e.map(function(x){const p=x.payload||{};const buttons=[['GOOD_ENTRY','Good entry'],['BAD_SIGNAL','Bad signal'],['LATE_ENTRY','Late'],['WRONG_DIRECTION','Wrong direction'],['STOP_LOSS_TOO_TIGHT','Stop too tight'],['TARGET_TOO_CLOSE','Target too close'],['NEEDS_REVIEW','Review']].map(function(pair){return '<button onclick="feedback('+x.id+',\''+pair[0]+'\')">'+pair[1]+'</button>'}).join('');
 const old=(x.feedback||[]).map(function(f){return '<small>'+esc(f.category)+' — '+esc(f.comment)+'</small><br>'}).join('');
 return '<article class="entry"><div><b>#'+x.id+' '+esc(x.symbol)+' '+esc(x.direction)+'</b><div class="muted">'+esc(x.delivered_at)+' · '+esc(x.outcome_status||'NO OUTCOME')+' · R='+esc(x.realized_r??'—')+' · MFE='+esc(x.best_favorable_r??'—')+' · MAE='+esc(x.worst_adverse_r??'—')+'</div><pre>'+esc(JSON.stringify({entry:p.entry,stop:p.stop,target:p.target,diagnostics:p.diagnostics},null,2))+'</pre><div>'+old+'</div><textarea id="comment-'+x.id+'" placeholder="Optional feedback comment"></textarea></div><div>'+buttons+'</div></article>'}).join('');
 status.textContent='Loaded '+e.length+' signals and '+c.length+' rejected candidates.';
 }catch(err){status.textContent='Error: '+err.message}}
async function submitTrainingInstruction(){const box=document.getElementById('training-instruction');const out=document.getElementById('training-result');try{const result=await api('/journal/api/training-instructions',{method:'POST',body:JSON.stringify({instruction:box.value})});out.textContent=JSON.stringify(result,null,2);box.value='';}catch(err){out.textContent='Error: '+err.message}}
async function feedback(id,category){try{await api('/journal/api/feedback',{method:'POST',body:JSON.stringify({signal_id:id,category:category,comment:document.getElementById('comment-'+id).value})});await loadAll()}catch(err){alert(err.message)}}
async function candidateFeedback(id,category){const comment=prompt('Optional note for this rejected setup:')||'';try{await api('/journal/api/candidate-feedback',{method:'POST',body:JSON.stringify({candidate_id:id,category:category,comment:comment})});await loadAll()}catch(err){alert(err.message)}}
</script></main></body></html>"""
