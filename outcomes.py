"""Conservative forward outcome labeling for delivered 5m signals.

The original TP1 result remains the primary realized outcome. TP2 is tracked as
a separate post-TP1 milestone so research can measure continuation without
assuming a partial-exit or position-management rule.
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
    """Advance one outcome using later, chronologically ordered 5m candles.

    TP1 remains the primary outcome and realized-R basis. Once TP1 is hit,
    subsequent candles are observed separately until TP2, the original stop,
    a data gap, or the configured signal horizon. No partial exit is assumed.

    A stop and target touched in the same candle are conservatively resolved
    against the signal. A missing candle interval is marked AMBIGUOUS.
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
    tp2_bars_seen = int(outcome.get("tp2_bars_observed") or 0)
    last_ts = int(outcome["last_observed_candle_open_ms"])
    data_gap = bool(outcome.get("data_gap", False))
    status = str(outcome.get("status") or "OPEN")
    first_hit = outcome.get("first_hit")
    result = outcome.get("result")
    exit_ts = outcome.get("exit_candle_open_ms")
    exit_price = outcome.get("exit_price")
    realized_r = outcome.get("realized_r")
    tp1_ts = outcome.get("tp1_hit_candle_open_ms")
    tp2_ts = outcome.get("tp2_hit_candle_open_ms")
    tp2_tracking_status = str(outcome.get("tp2_tracking_status") or "NOT_STARTED")
    tp2_realized_r = outcome.get("tp2_realized_r")
    changed = False

    effective_horizon = int(outcome.get("outcome_horizon_bars") or horizon_bars)
    effective_cost_pct = float(outcome.get("cost_pct", cost_pct))
    cost_r = (entry * effective_cost_pct / 100.0) / risk
    tracking_tp2 = status == "TARGET" and first_hit == "TP1" and tp2_tracking_status == "OPEN"

    ordered = sorted(candles, key=lambda candle: int(candle["timestamp"]))
    for candle in ordered:
        ts = int(candle["timestamp"])
        if ts <= last_ts or ts <= int(outcome["signal_candle_open_ms"]):
            continue

        if ts - last_ts != CANDLE_INTERVAL_MS:
            data_gap = True
            last_ts = ts
            changed = True
            if tracking_tp2:
                tp2_tracking_status = "AMBIGUOUS"
            else:
                status = "AMBIGUOUS"
                first_hit = "AMBIGUOUS"
                exit_ts = ts
                bars_seen += 1
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
        if favorable_r > best_r:
            best_r, best_pct = favorable_r, favorable_distance / entry * 100.0
            best_ts = ts
            best_atr = favorable_distance / atr if atr else None
        if adverse_r > worst_r:
            worst_r, worst_pct = adverse_r, adverse_distance / entry * 100.0
            worst_ts = ts
            worst_atr = adverse_distance / atr if atr else None

        last_ts = ts
        changed = True

        if tracking_tp2:
            tp2_bars_seen += 1
            # If stop and TP2 both occur in one OHLC candle, use the
            # conservative stop-first assumption for this continuation metric.
            if stop_hit:
                tp2_tracking_status = "STOPPED"
                break
            if tp2_hit:
                tp2_tracking_status = "HIT"
                tp2_ts = ts
                tp2_realized_r = abs(target2 - entry) / risk - cost_r
                break
            if bars_seen + tp2_bars_seen >= effective_horizon:
                tp2_tracking_status = "TIMEOUT"
                break
            continue

        bars_seen += 1
        # A stop touched in the same OHLC bar as either target is a loss.
        if stop_hit:
            status = "STOP_LOSS"
            first_hit = "SL"
            result = "LOSS"
            exit_ts = ts
            exit_price = stop
            realized_r = -1.0 - cost_r
            tp2_tracking_status = "NOT_STARTED"
            break

        if tp1_hit or tp2_hit:
            # TP1 is nearer than TP2; it is the first target if both are crossed.
            if tp1_hit:
                first_hit, exit_price = "TP1", target1
                tp1_ts = ts
            else:
                first_hit, exit_price = "TP2", target2
            status = "TARGET"
            exit_ts = ts
            gross_r = abs(float(exit_price) - entry) / risk
            realized_r = gross_r - cost_r
            result = "WIN" if realized_r > 0 else "LOSS"
            if tp1_hit and tp2_hit:
                tp2_tracking_status = "HIT"
                tp2_ts = ts
                tp2_realized_r = abs(target2 - entry) / risk - cost_r
            elif tp1_hit:
                tp2_tracking_status = "OPEN"
            else:
                tp2_tracking_status = "HIT"
                tp2_ts = ts
                tp2_realized_r = realized_r
            break

        if bars_seen >= effective_horizon:
            status = "TIMEOUT"
            first_hit = "TIMEOUT"
            result = "TIMEOUT"
            exit_ts = ts
            exit_price = close
            gross_r = ((close - entry) if side == "LONG" else (entry - close)) / risk
            realized_r = gross_r - cost_r
            tp2_tracking_status = "NOT_STARTED"
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
        "tp1_hit_candle_open_ms": tp1_ts,
        "tp2_hit_candle_open_ms": tp2_ts,
        "tp2_tracking_status": tp2_tracking_status,
        "tp2_realized_r": tp2_realized_r,
        "tp2_bars_observed": tp2_bars_seen,
    }
