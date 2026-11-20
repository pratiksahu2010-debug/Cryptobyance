"""Conservative forward outcome labeling for delivered 5m signals.

This module is intentionally pure: it does not place orders or change live rules.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

CANDLE_INTERVAL_MS = 300_000


def advance_outcome(
    outcome: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    *,
    horizon_bars: int = 12,
    cost_pct: float = 0.10,
) -> dict[str, Any] | None:
    """Advance one OPEN outcome using later, chronologically ordered candles.

    Cost is a round-trip percentage of entry notional (0.10 means 0.10%).
    A same-candle stop/target touch is a LOSS. A missing candle interval is
    AMBIGUOUS because the unobserved path cannot be reconstructed from OHLC.
    """
    if horizon_bars < 1:
        raise ValueError("horizon_bars must be at least 1")
    if cost_pct < 0:
        raise ValueError("cost_pct cannot be negative")

    side = str(outcome["direction"]).upper()
    if side not in {"LONG", "SHORT"}:
        raise ValueError("direction must be LONG or SHORT")

    entry = float(outcome["entry"])
    stop = float(outcome["stop"])
    target = float(outcome.get("target", outcome.get("target2")))
    target1 = float(outcome.get("target1") or target)
    target2 = float(outcome.get("target2") or target)
    risk = float(outcome["risk_per_unit"])
    if entry <= 0 or risk <= 0:
        raise ValueError("entry and risk_per_unit must be positive")

    atr_raw = outcome.get("atr_at_entry")
    atr = float(atr_raw) if atr_raw is not None else None
    if atr is not None and atr <= 0:
        atr = None

    best_r = float(outcome.get("best_favorable_r") or 0.0)
    worst_r = float(outcome.get("worst_adverse_r") or 0.0)
    best_pct = float(outcome.get("best_favorable_pct") or 0.0)
    worst_pct = float(outcome.get("worst_adverse_pct") or 0.0)
    best_atr_raw = outcome.get("best_favorable_atr")
    worst_atr_raw = outcome.get("worst_adverse_atr")
    best_atr = float(best_atr_raw) if best_atr_raw is not None else None
    worst_atr = float(worst_atr_raw) if worst_atr_raw is not None else None
    best_ts = outcome.get("best_favorable_candle_open_ms")
    worst_ts = outcome.get("worst_adverse_candle_open_ms")
    bars_seen = int(outcome.get("bars_observed") or 0)
    last_ts = int(outcome["last_observed_candle_open_ms"])
    data_gap = bool(outcome.get("data_gap", False))
    status = "OPEN"
    first_hit = None
    result = None
    exit_ts = None
    exit_price = None
    realized_r = None
    changed = False
    effective_horizon = int(outcome.get("outcome_horizon_bars") or horizon_bars)
    effective_cost_pct = float(outcome.get("cost_pct", cost_pct))
    cost_r = (entry * effective_cost_pct / 100.0) / risk

    ordered = sorted(candles, key=lambda candle: int(candle["timestamp"]))
    for candle in ordered:
        ts = int(candle["timestamp"])
        if ts <= last_ts or ts <= int(outcome["signal_candle_open_ms"]):
            continue

        if ts - last_ts != CANDLE_INTERVAL_MS:
            data_gap = True
            status = "AMBIGUOUS"
            first_hit = "AMBIGUOUS"
            exit_ts = ts
            bars_seen += 1
            last_ts = ts
            changed = True
            break

        high = float(candle["high"])
        low = float(candle["low"])
        close = float(candle.get("close", entry))
        if side == "LONG":
            favorable_distance = max(0.0, high - entry)
            adverse_distance = max(0.0, entry - low)
            stop_hit = low <= stop
            tp1_hit = high >= target1
            tp2_hit = high >= target2
        else:
            favorable_distance = max(0.0, entry - low)
            adverse_distance = max(0.0, high - entry)
            stop_hit = high >= stop
            tp1_hit = low <= target1
            tp2_hit = low <= target2

        favorable_r = favorable_distance / risk
        adverse_r = adverse_distance / risk
        favorable_pct = favorable_distance / entry * 100.0
        adverse_pct = adverse_distance / entry * 100.0
        if favorable_r > best_r:
            best_r, best_pct = favorable_r, favorable_pct
            best_ts = ts
            best_atr = favorable_distance / atr if atr else None
        if adverse_r > worst_r:
            worst_r, worst_pct = adverse_r, adverse_pct
            worst_ts = ts
            worst_atr = adverse_distance / atr if atr else None

        bars_seen += 1
        last_ts = ts
        changed = True

        # A stop touched in the same OHLC bar as either target is conservatively
        # classified as a loss. The intrabar order cannot be proven.
        if stop_hit:
            status = "STOP_LOSS"
            first_hit = "SL"
            result = "LOSS"
            exit_ts = ts
            exit_price = stop
            realized_r = -1.0 - cost_r
            break
        if tp1_hit or tp2_hit:
            # TP1 is nearer than TP2, so it is the first target when both are
            # crossed inside the same candle.
            if tp1_hit:
                first_hit, exit_price = "TP1", target1
            else:
                first_hit, exit_price = "TP2", target2
            status = "TARGET"
            exit_ts = ts
            gross_r = abs(float(exit_price) - entry) / risk
            realized_r = gross_r - cost_r
            result = "WIN" if realized_r > 0 else "LOSS"
            break
        if bars_seen >= effective_horizon:
            status = "TIMEOUT"
            first_hit = "TIMEOUT"
            result = "TIMEOUT"
            exit_ts = ts
            exit_price = close
            gross_r = ((close - entry) if side == "LONG" else (entry - close)) / risk
            realized_r = gross_r - cost_r
            break

    if not changed:
        return None

    return {
        "status": status,
        "first_hit": first_hit,
        "result": result,
        "best_favorable_r": best_r,
        "worst_adverse_r": worst_r,
        "best_favorable_pct": best_pct,
        "worst_adverse_pct": worst_pct,
        "best_favorable_atr": best_atr,
        "worst_adverse_atr": worst_atr,
        "bars_observed": bars_seen,
        "bars_to_resolution": bars_seen if status != "OPEN" else None,
        "last_observed_candle_open_ms": last_ts,
        "exit_candle_open_ms": exit_ts,
        "exit_price": exit_price,
        "realized_r": realized_r,
        "data_gap": data_gap,
        "best_favorable_candle_open_ms": best_ts,
        "worst_adverse_candle_open_ms": worst_ts,
    }
