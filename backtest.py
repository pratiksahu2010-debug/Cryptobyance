"""Deterministic, no-lookahead replay/backtest for the production 5m state machine.

Input CSVs must contain UTC Unix-millisecond candle-open timestamps and OHLCV.
No exchange, broker, or Telegram calls are made by this module.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from config import settings, CONFIG_VERSION
from trainer import build_training_proposal
from outcomes import advance_outcome
from state_machine import CANDLE_INTERVAL_MS, advance_state
from strategy import evaluate_setup, evaluate_state_inputs

CONTEXT_INTERVAL_MS = 15 * 60_000
OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume"]


def validate_history_frame(frame: pd.DataFrame, interval_ms: int, label: str) -> pd.DataFrame:
    """Validate a fully contiguous historical candle file; gaps fail closed."""
    missing = [column for column in OHLCV_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"{label} CSV missing columns: {', '.join(missing)}")
    result = frame[OHLCV_COLUMNS].copy()
    for column in OHLCV_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    if result.empty or result[OHLCV_COLUMNS].isna().any().any():
        raise ValueError(f"{label} CSV is empty or contains non-numeric OHLCV values")
    values = result[OHLCV_COLUMNS].to_numpy(dtype=float)
    if not all(math.isfinite(float(value)) for value in values.flat):
        raise ValueError(f"{label} CSV contains non-finite values")
    result = result.sort_values("timestamp", kind="stable").reset_index(drop=True)
    timestamps = result["timestamp"].to_numpy(dtype=float)
    if not (timestamps == timestamps.astype("int64")).all():
        raise ValueError(f"{label} timestamps must be integer Unix milliseconds")
    result["timestamp"] = result["timestamp"].astype("int64")
    if result["timestamp"].duplicated().any():
        raise ValueError(f"{label} CSV contains duplicate candle timestamps")
    if (result["timestamp"] % interval_ms != 0).any():
        raise ValueError(f"{label} candle timestamps must align to {interval_ms}ms UTC boundaries")
    if len(result) > 1 and not (result["timestamp"].diff().dropna() == interval_ms).all():
        raise ValueError(f"{label} CSV has missing or overlapping candles; replay requires contiguous history")
    if (result[["open", "high", "low", "close"]] <= 0).any().any() or (result["volume"] < 0).any():
        raise ValueError(f"{label} CSV contains invalid prices or volume")
    if (result["high"] < result[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError(f"{label} CSV contains an inconsistent high")
    if (result["low"] > result[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError(f"{label} CSV contains an inconsistent low")
    return result


def completed_context_15m(candles_15m: pd.DataFrame, current_5m_close_ms: int) -> pd.DataFrame:
    """Only expose 15m candles whose close is at/before the current 5m close."""
    return candles_15m.loc[
        candles_15m["timestamp"] + CONTEXT_INTERVAL_MS <= int(current_5m_close_ms)
    ].copy().reset_index(drop=True)


def _remaining_cooldown(until_ms: int | None, candle_open_ms: int) -> int:
    if until_ms is None:
        return 0
    return max(0, (int(until_ms) - int(candle_open_ms) + CANDLE_INTERVAL_MS - 1) // CANDLE_INTERVAL_MS)


def build_replay_training_preview(trades: list[dict]) -> dict[str, Any]:
    """Pass resolved synthetic replay outcomes through the same offline trainer."""
    rows = []
    for trade in trades:
        result = str(trade.get("result") or "").upper()
        realized_r = trade.get("realized_r")
        if result not in {"WIN", "LOSS", "TIMEOUT"} or realized_r is None:
            continue
        entry = trade.get("entry")
        vwap = trade.get("vwap")
        ema9 = trade.get("ema9")
        ema21 = trade.get("ema21")
        features = dict(trade.get("diagnostics") or {})
        features.update({
            "adx14": trade.get("adx", features.get("adx14")),
            "rsi14": trade.get("rsi", features.get("rsi14")),
            "ema9": ema9, "ema21": ema21, "vwap": vwap,
            "close": entry,
            "hour_utc": (int(trade["signal_candle_open_ms"]) // 3_600_000) % 24,
            "cooldown_candles": trade.get("cooldown_candles"),
            "atr_pct_of_price": (
                100.0 * float(trade["atr"]) / float(entry)
                if trade.get("atr") is not None and entry not in (None, 0) else None
            ),
        })
        if ema9 is not None and ema21 not in (None, 0):
            features["ema_spread_pct"] = 100.0 * (float(ema9) - float(ema21)) / float(ema21)
        if entry is not None and vwap not in (None, 0):
            features["price_vwap_distance_pct"] = 100.0 * (float(entry) - float(vwap)) / float(vwap)
        rows.append({
            "signal_id": int(trade["signal_id"]),
            "candle_open_ms": int(trade["signal_candle_open_ms"]),
            "direction": trade["side"],
            "features": features,
            "auto_result": result,
            "realized_r": float(realized_r),
            "manual_label": None,
        })
    current_config = {
        "adx_min": settings.adx_min,
        "rsi_long_min": settings.rsi_long_min,
        "rsi_long_max": settings.rsi_long_max,
        "rsi_short_min": settings.rsi_short_min,
        "rsi_short_max": settings.rsi_short_max,
    }
    proposal = build_training_proposal(
        rows, current_config, base_config_version=CONFIG_VERSION,
        min_samples=settings.training_min_samples,
        min_bucket_samples=settings.training_min_bucket_samples,
    )
    return {
        "status": proposal.get("status"),
        "reason": proposal.get("reason"),
        "sample_count": proposal.get("summary", {}).get("sample_count", 0),
        "manual_label_conflicts": proposal.get("summary", {}).get("manual_label_conflicts", 0),
        "expected_effect": proposal.get("expected_effect"),
        "walk_forward": proposal.get("walk_forward"),
        "synthetic_replay_data": True,
        "persisted_to_database": False,
        "live_settings_changed": False,
    }


def run_backtest(
    candles_5m: pd.DataFrame,
    candles_15m: pd.DataFrame,
    *,
    cooldown_candles: int | None = None,
    horizon_bars: int | None = None,
    cost_pct: float | None = None,
) -> dict[str, Any]:
    """Replay the live state machine and label each emitted signal on later bars."""
    five = validate_history_frame(candles_5m, CANDLE_INTERVAL_MS, "5m")
    fifteen = validate_history_frame(candles_15m, CONTEXT_INTERVAL_MS, "15m")
    cooldown = int(cooldown_candles if cooldown_candles is not None else settings.signal_cooldown_candles)
    horizon = int(horizon_bars if horizon_bars is not None else settings.outcome_horizon_bars)
    cost = float(cost_pct if cost_pct is not None else settings.outcome_cost_pct)
    if not 1 <= cooldown <= 100:
        raise ValueError("cooldown_candles must be between 1 and 100")
    if not 1 <= horizon <= 288:
        raise ValueError("horizon_bars must be between 1 and 288")
    if not 0 <= cost <= 5 or not math.isfinite(cost):
        raise ValueError("cost_pct must be between 0 and 5")
    if len(five) < 60 or len(fifteen) < 35:
        raise ValueError("replay requires at least 60 completed 5m and 35 completed 15m candles")

    states = {"LONG": "IDLE", "SHORT": "IDLE"}
    invalidated_in_cooldown = {"LONG": False, "SHORT": False}
    cooldown_until = {"LONG": None, "SHORT": None}
    armed_at = {"LONG": None, "SHORT": None}
    signals: list[dict] = []

    for index in range(len(five)):
        current_5m = five.iloc[: index + 1].copy()
        candle_open_ms = int(current_5m.iloc[-1]["timestamp"])
        candle_close_ms = candle_open_ms + CANDLE_INTERVAL_MS
        current_15m = completed_context_15m(fifteen, candle_close_ms)
        if len(current_15m) < 35:
            continue

        armed_sides = {side for side, state in states.items() if state == "ARMED"}
        inputs = evaluate_state_inputs(current_5m, current_15m, armed_sides)
        if inputs is None:
            continue

        for side in ("LONG", "SHORT"):
            prior_state = states[side]
            side_inputs = inputs[side]
            decision = advance_state(
                prior_state,
                setup_valid=side_inputs["setup_valid"],
                invalidation=side_inputs["invalidation"],
                trigger=side_inputs["trigger"],
                cooldown_remaining=_remaining_cooldown(cooldown_until[side], candle_open_ms),
                invalidated_during_cooldown=invalidated_in_cooldown[side],
                missed_candles=False,
            )
            if decision.action == "TRIGGER":
                setup = evaluate_setup(current_5m, current_15m)
                if setup is not None and setup.get("side") == side:
                    signal = dict(setup)
                    signal["signal_id"] = len(signals) + 1
                    signal["signal_candle_open_ms"] = candle_open_ms
                    signal["armed_candle_open_ms"] = armed_at[side]
                    signal["bars_since_armed"] = (
                        (candle_open_ms - armed_at[side]) // CANDLE_INTERVAL_MS
                        if armed_at[side] is not None else None
                    )
                    signals.append(signal)
                    # Mirror enqueue_signal -> persist_signal_lifecycle.
                    states[side] = "COOLDOWN"
                    cooldown_until[side] = candle_open_ms + cooldown * CANDLE_INTERVAL_MS
                    invalidated_in_cooldown[side] = False
                    armed_at[side] = None
                continue

            states[side] = decision.state
            if decision.action == "ARMED" and prior_state != "ARMED":
                armed_at[side] = candle_open_ms
            if decision.action == "INVALIDATED" or decision.state == "IDLE":
                armed_at[side] = None
                cooldown_until[side] = None
                invalidated_in_cooldown[side] = False
            elif prior_state == "COOLDOWN":
                invalidated_in_cooldown[side] = decision.invalidated

    trade_rows = []
    later_candles = [
        {"timestamp": int(row.timestamp), "high": float(row.high),
         "low": float(row.low), "close": float(row.close)}
        for row in five.itertuples(index=False)
    ]
    for signal in signals:
        signal_open = int(signal["signal_candle_open_ms"])
        outcome = {
            "direction": signal["side"],
            "signal_candle_open_ms": signal_open,
            "entry": float(signal["entry"]),
            "stop": float(signal["stop"]),
            "target": float(signal["target"]),
            "target1": float(signal["target1"]),
            "target2": float(signal["target2"]),
            "atr_at_entry": float(signal["atr"]),
            "risk_per_unit": float(signal["risk_distance"]),
            "outcome_horizon_bars": horizon,
            "cost_pct": cost,
            "best_favorable_r": 0.0,
            "worst_adverse_r": 0.0,
            "best_favorable_pct": 0.0,
            "worst_adverse_pct": 0.0,
            "best_favorable_atr": None,
            "worst_adverse_atr": None,
            "bars_observed": 0,
            "last_observed_candle_open_ms": signal_open,
            "data_gap": False,
            "best_favorable_candle_open_ms": None,
            "worst_adverse_candle_open_ms": None,
        }
        progress = advance_outcome(outcome, later_candles, horizon_bars=horizon, cost_pct=cost)
        trade = {
            "signal_id": signal["signal_id"],
            "signal_candle_open_ms": signal_open,
            "side": signal["side"],
            "entry": signal["entry"],
            "stop": signal["stop"],
            "target1": signal["target1"],
            "target2": signal["target2"],
            "risk_distance": signal["risk_distance"],
            "risk_model": signal.get("risk_model"),
            "adx": signal.get("adx"),
            "rsi": signal.get("rsi"),
            "ema9": signal.get("ema9"),
            "ema21": signal.get("ema21"),
            "vwap": signal.get("vwap"),
            "atr": signal.get("atr"),
            "cooldown_candles": cooldown,
            "context": signal.get("context"),
            "diagnostics": signal.get("diagnostics"),
            "status": "OPEN",
            "first_hit": None,
            "result": None,
            "realized_r": None,
            "bars_to_resolution": None,
            "best_favorable_r": 0.0,
            "worst_adverse_r": 0.0,
            "best_favorable_pct": 0.0,
            "worst_adverse_pct": 0.0,
            "best_favorable_atr": None,
            "worst_adverse_atr": None,
            "data_gap": False,
        }
        if progress:
            trade.update(progress)
        trade_rows.append(trade)

    resolved = [row for row in trade_rows if row["status"] in {"TARGET", "STOP_LOSS", "TIMEOUT"}]
    wins = sum(row["result"] == "WIN" for row in resolved)
    losses = sum(row["result"] == "LOSS" for row in resolved)
    timeouts = sum(row["result"] == "TIMEOUT" for row in resolved)
    realized = [float(row["realized_r"]) for row in resolved if row["realized_r"] is not None]
    positive = sum(value for value in realized if value > 0)
    negative = abs(sum(value for value in realized if value < 0))
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in realized:
        cumulative += value
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)

    def average(field: str) -> float | None:
        values = [float(row[field]) for row in resolved if row.get(field) is not None]
        return round(sum(values) / len(values), 4) if values else None

    summary = {
        "signals": len(trade_rows),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "ambiguous": sum(row["status"] == "AMBIGUOUS" for row in trade_rows),
        "open": sum(row["status"] == "OPEN" for row in trade_rows),
        "win_rate_resolved_pct": round(100 * wins / len(resolved), 2) if resolved else None,
        "expectancy_net_r": round(sum(realized) / len(realized), 4) if realized else None,
        "profit_factor": round(positive / negative, 4) if negative else (None if not positive else "inf"),
        "max_drawdown_r": round(max_drawdown, 4),
        "average_mfe_r": average("best_favorable_r"),
        "average_mae_r": average("worst_adverse_r"),
        "average_mfe_pct": average("best_favorable_pct"),
        "average_mae_pct": average("worst_adverse_pct"),
        "average_bars_to_resolution": average("bars_to_resolution"),
        "cooldown_candles": cooldown,
        "outcome_horizon_bars": horizon,
        "estimated_round_trip_cost_pct": cost,
        "assumed_entry": "signal candle close",
        "no_lookahead": True,
    }
    training_preview = build_replay_training_preview(trade_rows)
    summary["training_preview"] = {
        "status": training_preview["status"],
        "sample_count": training_preview["sample_count"],
        "reason": training_preview["reason"],
        "expected_effect": training_preview["expected_effect"],
    }
    return {"summary": summary, "trades": trade_rows, "training_preview": training_preview}


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay/backtest the production CryptoByance 5m state machine.")
    parser.add_argument("--candles-5m", required=True, help="CSV with timestamp,open,high,low,close,volume (UTC ms)")
    parser.add_argument("--candles-15m", required=True, help="CSV with timestamp,open,high,low,close,volume (UTC ms)")
    parser.add_argument("--trades-out", help="Optional CSV path for signal and outcome rows")
    parser.add_argument("--summary-out", help="Optional JSON path for summary metrics")
    parser.add_argument("--cooldown-bars", type=int, default=settings.signal_cooldown_candles)
    parser.add_argument("--horizon-bars", type=int, default=settings.outcome_horizon_bars)
    parser.add_argument("--cost-pct", type=float, default=settings.outcome_cost_pct)
    args = parser.parse_args()

    result = run_backtest(
        pd.read_csv(args.candles_5m),
        pd.read_csv(args.candles_15m),
        cooldown_candles=args.cooldown_bars,
        horizon_bars=args.horizon_bars,
        cost_pct=args.cost_pct,
    )
    rendered = json.dumps(result["summary"], indent=2, ensure_ascii=False)
    print(rendered)
    if args.trades_out:
        Path(args.trades_out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(result["trades"]).to_csv(args.trades_out, index=False)
    if args.summary_out:
        Path(args.summary_out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.summary_out).write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
