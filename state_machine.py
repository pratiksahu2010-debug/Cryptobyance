"""Persistent per-strategy/per-symbol/per-direction signal lifecycle.

The current strategy has one completed-candle qualification predicate: its setup
and trigger conditions are identical. Therefore ARMED -> TRIGGERED occurs on the
same candle today. This module records that lifecycle explicitly without
inventing a new setup rule or changing strategy thresholds.
"""
from __future__ import annotations

from typing import Iterable

STATE_DDL = """
CREATE TABLE IF NOT EXISTS strategy_signal_states (
    strategy_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG','SHORT')),
    state TEXT NOT NULL DEFAULT 'IDLE'
      CHECK (state IN ('IDLE','ARMED','TRIGGERED','COOLDOWN')),
    last_processed_candle_open_ms BIGINT,
    last_triggered_candle_open_ms BIGINT,
    cooldown_until_candle_open_ms BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (strategy_key, symbol, direction)
);
CREATE INDEX IF NOT EXISTS ix_strategy_signal_states_state
    ON strategy_signal_states(strategy_key, state, updated_at);
CREATE TABLE IF NOT EXISTS strategy_state_events (
    id BIGSERIAL PRIMARY KEY,
    strategy_key TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('LONG','SHORT')),
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    candle_open_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_strategy_state_events_lookup
    ON strategy_state_events(strategy_key, symbol, direction, candle_open_ms DESC);
INSERT INTO schema_migrations(version) VALUES (3) ON CONFLICT DO NOTHING;
"""

VALID_STATES = frozenset({"IDLE", "ARMED", "TRIGGERED", "COOLDOWN"})
CANDLE_INTERVAL_MS = 300_000


def signal_transition_path(
    current_state: str,
    candle_open_ms: int,
    cooldown_until_candle_open_ms: int | None,
) -> list[tuple[str, str]] | None:
    """Return lifecycle edges for a valid signal, or None while cooling down.

    This helper does not evaluate trading conditions. It is called only after
    the original strategy has qualified a completed candle.
    """
    if current_state not in VALID_STATES:
        raise ValueError(f"unknown strategy state: {current_state}")
    transitions: list[tuple[str, str]] = []
    if current_state == "COOLDOWN":
        if cooldown_until_candle_open_ms is None:
            raise ValueError("COOLDOWN state requires cooldown_until_candle_open_ms")
        if int(candle_open_ms) < int(cooldown_until_candle_open_ms):
            return None
        transitions.append(("COOLDOWN", "IDLE"))
        current_state = "IDLE"
    if current_state == "IDLE":
        transitions.append(("IDLE", "ARMED"))
        current_state = "ARMED"
    if current_state == "ARMED":
        transitions.append(("ARMED", "TRIGGERED"))
        current_state = "TRIGGERED"
    if current_state == "TRIGGERED":
        transitions.append(("TRIGGERED", "COOLDOWN"))
    return transitions


async def persist_signal_lifecycle(
    conn,
    *,
    strategy_key: str,
    symbol: str,
    direction: str,
    candle_open_ms: int,
    cooldown_candles: int,
) -> bool:
    """Persist lifecycle edges and final COOLDOWN state inside caller transaction.

    Caller must hold the transaction-scoped advisory lock for this strategy,
    symbol and direction. State update and signal/outbox inserts therefore commit
    or roll back together.
    """
    row = await conn.fetchrow(
        """SELECT state,last_processed_candle_open_ms,cooldown_until_candle_open_ms
           FROM strategy_signal_states
           WHERE strategy_key=$1 AND symbol=$2 AND direction=$3
           FOR UPDATE""",
        strategy_key, symbol, direction,
    )
    if row and row["last_processed_candle_open_ms"] is not None:
        if int(candle_open_ms) <= int(row["last_processed_candle_open_ms"]):
            return False

    current_state = str(row["state"]) if row else "IDLE"
    cooldown_until = row["cooldown_until_candle_open_ms"] if row else None
    transitions = signal_transition_path(current_state, candle_open_ms, cooldown_until)
    if transitions is None:
        return False

    for from_state, to_state in transitions:
        await conn.execute(
            """INSERT INTO strategy_state_events(
                   strategy_key,symbol,direction,from_state,to_state,candle_open_ms
               ) VALUES($1,$2,$3,$4,$5,$6)""",
            strategy_key, symbol, direction, from_state, to_state, int(candle_open_ms),
        )

    cooldown_until_ms = int(candle_open_ms) + (int(cooldown_candles) + 1) * CANDLE_INTERVAL_MS
    await conn.execute(
        """INSERT INTO strategy_signal_states(
               strategy_key,symbol,direction,state,last_processed_candle_open_ms,
               last_triggered_candle_open_ms,cooldown_until_candle_open_ms,updated_at
           ) VALUES($1,$2,$3,'COOLDOWN',$4,$4,$5,NOW())
           ON CONFLICT(strategy_key,symbol,direction) DO UPDATE SET
               state='COOLDOWN',
               last_processed_candle_open_ms=EXCLUDED.last_processed_candle_open_ms,
               last_triggered_candle_open_ms=EXCLUDED.last_triggered_candle_open_ms,
               cooldown_until_candle_open_ms=EXCLUDED.cooldown_until_candle_open_ms,
               updated_at=NOW()""",
        strategy_key, symbol, direction, int(candle_open_ms), cooldown_until_ms,
    )
    return True
