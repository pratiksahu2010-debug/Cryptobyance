"""One-session entry-timing research using persisted 5m OHLCV candles.

The trainer is deliberately conservative: it compares paired counterfactual
entries for the same delivered signal, charges the configured round-trip cost,
uses stop-first handling when a candle touches both levels, and never mutates
the live strategy. A policy is only a next-session recommendation.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any, Iterable

CANDLE_MS = 5 * 60 * 1000
OFFSETS = (0, 1, 2, 3)
MIN_DIRECTION_SAMPLES = 20
MIN_EXPECTANCY_LCB_R = 0.05
# Bonferroni-style conservative threshold for three delayed-entry comparisons.
CONFIDENCE_Z = 2.4


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _simulate(signal: dict, candles: dict[int, dict], offset: int) -> tuple[float, float | None] | None:
    direction = str(signal.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        return None
    sign = 1.0 if direction == "LONG" else -1.0
    signal_ts = int(signal["candle_open_ms"])
    original_entry = _number(signal.get("entry"))
    original_stop = _number(signal.get("stop"))
    original_target = _number(signal.get("target1") or signal.get("target"))
    risk = _number(signal.get("risk_per_unit"))
    if None in (original_entry, original_stop, original_target, risk) or risk <= 0:
        return None
    stop_distance = abs(original_entry - original_stop)
    reward_distance = abs(original_target - original_entry)
    if stop_distance <= 0 or reward_distance <= 0:
        return None
    if direction == "LONG" and not (original_stop < original_entry < original_target):
        return None
    if direction == "SHORT" and not (original_target < original_entry < original_stop):
        return None

    # Signal candle opens at t=0 and closes at t=5m. A 5m wait therefore
    # enters at the open of t=10m, i.e. one full candle after the close.
    entry_ts = signal_ts + (offset + 1) * CANDLE_MS
    if offset == 0:
        entry = original_entry
        first_bar_ts = signal_ts + CANDLE_MS
    else:
        entry_bar = candles.get(entry_ts)
        if entry_bar is None:
            return None
        entry = _number(entry_bar.get("open"))
        if entry is None or entry <= 0:
            return None
        signed_move = sign * (entry - original_entry)
        # Do not chase an entry that has already crossed the original stop or TP1.
        if signed_move <= -stop_distance or signed_move >= reward_distance:
            # No fill: count the signal as a zero-R opportunity, not as missing data.
            return 0.0, None
        first_bar_ts = entry_ts

    stop = entry - sign * stop_distance
    target = entry + sign * reward_distance
    horizon = max(1, min(int(signal.get("outcome_horizon_bars") or 12), 288))
    cost_pct = max(0.0, _number(signal.get("cost_pct")) or 0.0)
    cost_r = (entry * cost_pct / 100.0) / stop_distance

    observed = 0
    last_close = None
    for index in range(horizon):
        ts = first_bar_ts + index * CANDLE_MS
        candle = candles.get(ts)
        if candle is None:
            return None  # Never train across a data gap.
        high, low = _number(candle.get("high")), _number(candle.get("low"))
        close = _number(candle.get("close"))
        if high is None or low is None or close is None or high < low:
            return None
        observed += 1
        last_close = close
        stop_hit = low <= stop if direction == "LONG" else high >= stop
        target_hit = high >= target if direction == "LONG" else low <= target
        if stop_hit:
            # Conservative when both levels are inside the same 5m candle.
            return -1.0 - cost_r, (entry / original_entry - 1.0) * 100.0
        if target_hit:
            return reward_distance / stop_distance - cost_r, (entry / original_entry - 1.0) * 100.0
    if observed != horizon or last_close is None:
        return None
    timeout_r = sign * (last_close - entry) / stop_distance - cost_r
    return timeout_r, (entry / original_entry - 1.0) * 100.0


def train_entry_timing(
    signals: Iterable[dict],
    candles_by_symbol: dict[str, dict[int, dict]],
    *,
    min_samples: int = MIN_DIRECTION_SAMPLES,
) -> dict:
    """Compare 0/5/10/15m entries using same-signal paired net-R outcomes."""
    report: dict[str, dict] = {}
    all_samples = list(signals)
    for direction in ("LONG", "SHORT"):
        side_signals = [s for s in all_samples if str(s.get("direction", "")).upper() == direction]
        per_offset: dict[int, list[tuple[int, float, float | None]]] = {offset: [] for offset in OFFSETS}
        for signal in side_signals:
            candles = candles_by_symbol.get(str(signal.get("symbol")), {})
            signal_id = int(signal.get("signal_id") or 0)
            for offset in OFFSETS:
                result = _simulate(signal, candles, offset)
                if result is not None:
                    net_r, adjustment_pct = result
                    per_offset[offset].append((signal_id, net_r, adjustment_pct))

        baseline = {signal_id: value for signal_id, value, _ in per_offset[0]}
        baseline_mean = mean(baseline.values()) if baseline else None
        best = None
        comparisons = {}
        for offset in OFFSETS[1:]:
            delayed = {signal_id: (value, adjustment) for signal_id, value, adjustment in per_offset[offset]}
            paired = [(baseline[sid], delayed[sid][0], delayed[sid][1])
                      for sid in baseline.keys() & delayed.keys()]
            count = len(paired)
            if count < min_samples:
                comparisons[offset] = {"sample_count": count, "status": "INSUFFICIENT_DATA"}
                continue
            differences = [new - old for old, new, _ in paired]
            improvement = mean(differences)
            if count > 1:
                variance = sum((x - improvement) ** 2 for x in differences) / (count - 1)
                standard_error = math.sqrt(variance / count)
            else:
                standard_error = float("inf")
            lower_bound = improvement - CONFIDENCE_Z * standard_error
            candidate = {
                "delay_bars": offset,
                "delay_minutes": offset * 5,
                "sample_count": count,
                "baseline_expectancy_r": round(mean(old for old, _, _ in paired), 4),
                "candidate_expectancy_r": round(mean(new for _, new, _ in paired), 4),
                "mean_improvement_r": round(improvement, 4),
                "improvement_lower_bound_r": round(lower_bound, 4),
                "median_entry_adjustment_pct": round(median(adjustment for _, _, adjustment in paired if adjustment is not None), 5) if any(adjustment is not None for _, _, adjustment in paired) else 0.0,
                "status": "PASS" if lower_bound >= MIN_EXPECTANCY_LCB_R and mean(new for _, new, _ in paired) > 0 else "NO_CONFIDENT_IMPROVEMENT",
            }
            comparisons[offset] = candidate
            if candidate["status"] == "PASS" and (
                best is None or candidate["improvement_lower_bound_r"] > best["improvement_lower_bound_r"]
            ):
                best = candidate

        report[direction] = {
            "sample_count": len(baseline),
            "minimum_samples": min_samples,
            "baseline_expectancy_r": round(baseline_mean, 4) if baseline_mean is not None else None,
            "comparisons": {str(k): v for k, v in comparisons.items()},
            "policy": best,
            "status": "POLICY_READY" if best else (
                "INSUFFICIENT_DATA" if len(baseline) < min_samples else "KEEP_IMMEDIATE_ENTRY"
            ),
        }
    return {
        "training_day": None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "offsets_minutes": [offset * 5 for offset in OFFSETS],
        "directions": report,
        "live_strategy_changed": False,
        "alerts_delayed": False,
    }
