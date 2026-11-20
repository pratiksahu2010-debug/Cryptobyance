"""Persistent per-symbol setup/trigger/cooldown state machine.

One row is maintained per strategy, symbol and direction in PostgreSQL or the
local SQLite backend. A newly qualified
setup is only ARMED; it cannot trigger until a later completed candle.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CANDLE_INTERVAL_MS = 300_000
VALID_STATES = frozenset({"IDLE", "ARMED", "TRIGGERED", "COOLDOWN"})


@dataclass(frozen=True, slots=True)
class StateDecision:
    state: str
    action: Literal["NONE", "ARMED", "TRIGGER", "INVALIDATED", "COOLDOWN", "REARMED"]
    invalidated: bool = False


def advance_state(
    current_state: str,
    *,
    setup_valid: bool,
    invalidation: bool,
    trigger: bool,
    cooldown_remaining: int = 0,
    invalidated_during_cooldown: bool = False,
    missed_candles: bool = False,
) -> StateDecision:
    """Pure transition rules for one side on one new CLOSED candle."""
    if current_state not in VALID_STATES:
        raise ValueError(f"unknown strategy state: {current_state}")
    if current_state == "IDLE":
        return StateDecision("ARMED", "ARMED") if setup_valid else StateDecision("IDLE", "NONE")
    if current_state == "ARMED":
        # An armed setup is valid only while every intervening completed candle
        # has been evaluated. After a restart/scan gap, its invalidation history
        # is unknown, so discard it and require a fresh setup cycle.
        if invalidation or missed_candles:
            return StateDecision("IDLE", "INVALIDATED", True)
        if trigger:
            # The caller must atomically create the signal and enter COOLDOWN.
            return StateDecision("ARMED", "TRIGGER")
        return StateDecision("ARMED", "NONE")
    if current_state == "TRIGGERED":
        # Defensive recovery for a legacy/incomplete row. Never re-alert.
        return StateDecision("COOLDOWN", "COOLDOWN", invalidated_during_cooldown)
    # COOLDOWN includes COOLDOWN-WAIT: bar count may expire, but the setup
    # must also have invalidated at least once before returning to IDLE.
    invalidated = invalidated_during_cooldown or invalidation
    if cooldown_remaining > 0 or not invalidated:
        return StateDecision("COOLDOWN", "COOLDOWN", invalidated)
    return StateDecision("IDLE", "REARMED", True)


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
    setup_invalidated BOOLEAN NOT NULL DEFAULT FALSE,
    armed_candle_open_ms BIGINT,
    cycle_id BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (strategy_key, symbol, direction)
);
ALTER TABLE strategy_signal_states ADD COLUMN IF NOT EXISTS setup_invalidated BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE strategy_signal_states ADD COLUMN IF NOT EXISTS armed_candle_open_ms BIGINT;
ALTER TABLE strategy_signal_states ADD COLUMN IF NOT EXISTS cycle_id BIGINT NOT NULL DEFAULT 0;
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
INSERT INTO schema_migrations(version) VALUES (4) ON CONFLICT DO NOTHING;
"""


async def advance_persisted_state(
    conn,
    *,
    strategy_key: str,
    symbol: str,
    direction: str,
    candle_open_ms: int,
    setup_valid: bool,
    invalidation: bool,
    trigger: bool,
    cooldown_candles: int,
) -> str:
    """Persist one symbol/side scan; return TRIGGER only when ARMED can fire.

    Caller owns a transaction. Reprocessing an identical/older candle is a no-op.
    """
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))",
                       f"{strategy_key}:{symbol}:{direction}")
    row = await conn.fetchrow(
        """SELECT * FROM strategy_signal_states
           WHERE strategy_key=$1 AND symbol=$2 AND direction=$3 FOR UPDATE""",
        strategy_key, symbol, direction,
    )
    if row and row["last_processed_candle_open_ms"] is not None:
        if int(candle_open_ms) <= int(row["last_processed_candle_open_ms"]):
            return "DUPLICATE"

    state = str(row["state"]) if row else "IDLE"
    previous_candle_open_ms = row["last_processed_candle_open_ms"] if row else None
    missed_candles = (
        previous_candle_open_ms is not None
        and int(candle_open_ms) - int(previous_candle_open_ms) > CANDLE_INTERVAL_MS
    )
    invalidated_before = bool(row["setup_invalidated"]) if row else False
    cooldown_until = row["cooldown_until_candle_open_ms"] if row else None
    remaining = 0
    if state == "COOLDOWN" and cooldown_until is not None:
        remaining = max(0, (int(cooldown_until) - int(candle_open_ms) + CANDLE_INTERVAL_MS - 1) // CANDLE_INTERVAL_MS)

    decision = advance_state(
        state, setup_valid=setup_valid, invalidation=invalidation, trigger=trigger,
        cooldown_remaining=remaining,
        invalidated_during_cooldown=invalidated_before,
        missed_candles=missed_candles,
    )
    if decision.action == "TRIGGER":
        # Keep ARMED and its prior-candle checkpoint unchanged until enqueue_signal
        # atomically commits signal history, outbox and cooldown.
        return "TRIGGER"

    next_state = decision.state
    if row is None:
        await conn.execute(
            """INSERT INTO strategy_signal_states(
                 strategy_key,symbol,direction,state,last_processed_candle_open_ms,
                 setup_invalidated,armed_candle_open_ms,cycle_id,updated_at
               ) VALUES($1,$2,$3,$4,$5,$6,$7,CASE WHEN $4='ARMED' THEN 1 ELSE 0 END,NOW())""",
            strategy_key, symbol, direction, next_state, int(candle_open_ms),
            decision.invalidated, int(candle_open_ms) if next_state == "ARMED" else None,
        )
    else:
        await conn.execute(
            """UPDATE strategy_signal_states SET state=$4,last_processed_candle_open_ms=$5,
               setup_invalidated=$6,
               armed_candle_open_ms=CASE WHEN $4='ARMED' AND state<>'ARMED' THEN $5
                                         WHEN $4='IDLE' THEN NULL ELSE armed_candle_open_ms END,
               cycle_id=cycle_id+CASE WHEN $4='ARMED' AND state<>'ARMED' THEN 1 ELSE 0 END,
               cooldown_until_candle_open_ms=CASE WHEN $4='IDLE' THEN NULL ELSE cooldown_until_candle_open_ms END,
               updated_at=NOW()
               WHERE strategy_key=$1 AND symbol=$2 AND direction=$3""",
            strategy_key, symbol, direction, next_state, int(candle_open_ms), decision.invalidated,
        )
    if next_state != state:
        await conn.execute(
            """INSERT INTO strategy_state_events(
                 strategy_key,symbol,direction,from_state,to_state,candle_open_ms
               ) VALUES($1,$2,$3,$4,$5,$6)""",
            strategy_key, symbol, direction, state, next_state, int(candle_open_ms),
        )
    return decision.action


async def persist_signal_lifecycle(
    conn, *, strategy_key: str, symbol: str, direction: str,
    candle_open_ms: int, cooldown_candles: int,
) -> bool:
    """Atomically move an ARMED setup into cooldown when its alert is enqueued."""
    row = await conn.fetchrow(
        """SELECT state,last_processed_candle_open_ms,cycle_id
           FROM strategy_signal_states
           WHERE strategy_key=$1 AND symbol=$2 AND direction=$3 FOR UPDATE""",
        strategy_key, symbol, direction,
    )
    if row is None:
        # Compatibility for pre-state-machine callers using the legacy
        # "default" strategy key. The production v2 scanner always supplies
        # its versioned key and must have an ARMED row created by setup logic.
        if strategy_key != "default":
            return False
        await conn.execute(
            """INSERT INTO strategy_signal_states(
                 strategy_key,symbol,direction,state,last_processed_candle_open_ms,
                 last_triggered_candle_open_ms,cooldown_until_candle_open_ms,
                 setup_invalidated,cycle_id,updated_at
               ) VALUES($1,$2,$3,'COOLDOWN',$4,$4,$5,FALSE,1,NOW())""",
            strategy_key, symbol, direction, int(candle_open_ms),
            int(candle_open_ms) + int(cooldown_candles) * CANDLE_INTERVAL_MS,
        )
        transitions = (("IDLE", "ARMED"), ("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN"))
        for old_state, new_state in transitions:
            await conn.execute(
                """INSERT INTO strategy_state_events(
                     strategy_key,symbol,direction,from_state,to_state,candle_open_ms
                   ) VALUES($1,$2,$3,$4,$5,$6)""",
                strategy_key, symbol, direction, old_state, new_state, int(candle_open_ms),
            )
        return True
    if row["state"] != "ARMED":
        return False
    previous = row["last_processed_candle_open_ms"]
    if previous is not None and int(candle_open_ms) <= int(previous):
        return False
    until_ms = int(candle_open_ms) + int(cooldown_candles) * CANDLE_INTERVAL_MS
    await conn.execute(
        """UPDATE strategy_signal_states SET state='COOLDOWN',
             last_processed_candle_open_ms=$4,last_triggered_candle_open_ms=$4,
             cooldown_until_candle_open_ms=$5,setup_invalidated=FALSE,updated_at=NOW()
           WHERE strategy_key=$1 AND symbol=$2 AND direction=$3""",
        strategy_key, symbol, direction, int(candle_open_ms), until_ms,
    )
    for old_state, new_state in (("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN")):
        await conn.execute(
            """INSERT INTO strategy_state_events(
                 strategy_key,symbol,direction,from_state,to_state,candle_open_ms
               ) VALUES($1,$2,$3,$4,$5,$6)""",
            strategy_key, symbol, direction, old_state, new_state, int(candle_open_ms),
        )
    return True
