"""Point-in-time feature snapshots for delivered signal research."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


def _finite_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if pd.notna(number) and number not in (float("inf"), float("-inf")) else None


def build_indicator_snapshot(
    indicator_frame: pd.DataFrame,
    candles_5m: pd.DataFrame,
    signal: dict,
    *,
    candle_open_ms: int,
    config_version: str,
    cooldown_candles: int,
    state_metadata: dict | None = None,
) -> dict[str, Any]:
    """Capture only values known at the signal candle close.

    The shadow diagnostics are continuous, direction-normalized measurements.
    They are observational only: they never veto, delay, or alter a live signal.
    """
    if len(indicator_frame) < 2 or candles_5m.empty:
        raise ValueError("two indicator rows and a current candle are required")
    current = indicator_frame.iloc[-1]
    previous = indicator_frame.iloc[-2]
    ema9 = float(current.ema9)
    ema21 = float(current.ema21)
    vwap = float(current.vwap)
    close = float(current.close)
    atr_value = float(current.atr) if pd.notna(current.atr) else None
    if ema21 == 0 or vwap == 0:
        raise ValueError("EMA21 and VWAP must be non-zero for percentage features")

    close_series = candles_5m["close"].astype(float)
    macd_line = close_series.ewm(span=12, adjust=False).mean() - close_series.ewm(span=26, adjust=False).mean()
    macd_signal = macd_line.ewm(span=9, adjust=False).mean()
    macd_histogram = macd_line - macd_signal
    volume_mean = candles_5m["volume"].rolling(20, min_periods=20).mean().iloc[-1]
    relative_volume = (
        float(candles_5m["volume"].iloc[-1] / volume_mean)
        if pd.notna(volume_mean) and float(volume_mean) > 0 else None
    )

    side = str(signal.get("side") or signal.get("direction") or "").upper()
    direction_sign = 1.0 if side == "LONG" else -1.0 if side == "SHORT" else None
    prior3 = indicator_frame.iloc[-4] if len(indicator_frame) >= 4 else None
    shadow_diagnostics = {
        "mode": "OBSERVATION_ONLY",
        "lookback_bars": 3 if prior3 is not None else None,
        "direction": side if direction_sign is not None else None,
        "extension_in_signal_direction_atr": (
            direction_sign * (close - vwap) / atr_value
            if direction_sign is not None and atr_value is not None and atr_value > 0 else None
        ),
        "rsi_change_in_signal_direction_3bars": (
            direction_sign * (float(current.rsi) - float(prior3.rsi))
            if direction_sign is not None and prior3 is not None
            and pd.notna(current.rsi) and pd.notna(prior3.rsi) else None
        ),
        "di_spread_in_signal_direction": (
            direction_sign * (float(current.plus_di) - float(current.minus_di))
            if direction_sign is not None else None
        ),
        "di_spread_change_in_signal_direction_3bars": (
            direction_sign * (
                (float(current.plus_di) - float(current.minus_di))
                - (float(prior3.plus_di) - float(prior3.minus_di))
            )
            if direction_sign is not None and prior3 is not None else None
        ),
        "ema9_slope_in_signal_direction_atr_3bars": (
            direction_sign * (ema9 - float(prior3.ema9)) / atr_value
            if direction_sign is not None and prior3 is not None
            and atr_value is not None and atr_value > 0 else None
        ),
        "ema21_slope_in_signal_direction_atr_3bars": (
            direction_sign * (ema21 - float(prior3.ema21)) / atr_value
            if direction_sign is not None and prior3 is not None
            and atr_value is not None and atr_value > 0 else None
        ),
        "macd_histogram_in_signal_direction": (
            direction_sign * float(macd_histogram.iloc[-1])
            if direction_sign is not None else None
        ),
        "macd_histogram_change_in_signal_direction_3bars": (
            direction_sign * (float(macd_histogram.iloc[-1]) - float(macd_histogram.iloc[-4]))
            if direction_sign is not None and len(macd_histogram) >= 4 else None
        ),
    }

    state_metadata = state_metadata or {}
    armed_open_ms = state_metadata.get("armed_candle_open_ms")
    bars_since_armed = (
        max(0, (int(candle_open_ms) - int(armed_open_ms)) // 300_000)
        if armed_open_ms is not None else None
    )
    timestamp = datetime.fromtimestamp(int(candle_open_ms) / 1000, timezone.utc)

    return {
        "version": 3,
        "ema9": ema9,
        "ema21": ema21,
        "ema_spread_pct": 100.0 * (ema9 - ema21) / ema21,
        "previous_rsi14": float(previous.rsi) if pd.notna(previous.rsi) else None,
        "rsi14": float(current.rsi) if pd.notna(current.rsi) else None,
        "adx14": float(current.adx) if pd.notna(current.adx) else None,
        "plus_di": float(current.plus_di) if pd.notna(current.plus_di) else None,
        "minus_di": float(current.minus_di) if pd.notna(current.minus_di) else None,
        "vwap": vwap,
        "price_vwap_distance_pct": 100.0 * (close - vwap) / vwap,
        "price_vwap_distance_atr": (
            (close - vwap) / atr_value if atr_value is not None and atr_value > 0 else None
        ),
        "atr14": atr_value,
        "atr_pct_of_price": 100.0 * atr_value / close if atr_value is not None and close > 0 else None,
        "relative_volume_20": relative_volume,
        "macd_line": float(macd_line.iloc[-1]),
        "macd_signal": float(macd_signal.iloc[-1]),
        "macd_histogram": float(macd_histogram.iloc[-1]),
        "shadow_diagnostics": shadow_diagnostics,
        "open": float(current.open),
        "high": float(current.high),
        "low": float(current.low),
        "close": close,
        "volume": float(current.volume),
        "time_of_day_utc": timestamp.strftime("%H:%M"),
        "time_of_day_utc_minute": timestamp.hour * 60 + timestamp.minute,
        "day_of_week_utc": timestamp.weekday(),
        "index_trend": None,
        "market_trend_15m": signal.get("context"),
        "context_15m": signal.get("context"),
        "context_rsi14": signal.get("context_rsi14"),
        "context_rsi_aligned": signal.get("context_rsi_aligned"),
        "context_rsi_score": signal.get("context_rsi_score"),
        "bars_since_armed": bars_since_armed,
        "armed_candle_open_ms": int(armed_open_ms) if armed_open_ms is not None else None,
        "state_cycle_id": state_metadata.get("cycle_id"),
        "config_version": str(config_version),
        "cooldown_candles": int(cooldown_candles),
    }
