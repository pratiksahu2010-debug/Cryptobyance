"""Pure candle validation and single-engine LONG/SHORT strategy logic.

This module has no exchange, network, Telegram, or application lifecycle code.
"""
from __future__ import annotations

import time
from contextlib import contextmanager

import numpy as np
import pandas as pd

from config import settings

ADX_MIN = settings.adx_min
EMA_FAST = settings.ema_fast
EMA_SLOW = settings.ema_slow
RSI_PERIOD = settings.rsi_period
ADX_PERIOD = settings.adx_period
ATR_STOP_MULT = settings.atr_stop_multiplier
MIN_RR = settings.min_risk_reward

# Global entry-quality guards: avoid chasing an established/extended move.
MAX_EMA_ALIGNMENT_AGE_BARS = 2
MAX_ENTRY_EXTENSION_ATR = 1.0


@contextmanager
def using_runtime_settings(runtime_settings):
    """Temporarily evaluate strategy functions with another immutable config.

    The context must contain no await points: strategy evaluation is synchronous,
    so the event loop cannot interleave another symbol scan while globals are
    overridden. All module-level strategy constants are restored in finally.
    """
    global settings, ADX_MIN, EMA_FAST, EMA_SLOW, RSI_PERIOD, ADX_PERIOD
    global ATR_STOP_MULT, MIN_RR
    previous = (
        settings, ADX_MIN, EMA_FAST, EMA_SLOW, RSI_PERIOD, ADX_PERIOD,
        ATR_STOP_MULT, MIN_RR,
    )
    try:
        settings = runtime_settings
        ADX_MIN = runtime_settings.adx_min
        EMA_FAST = runtime_settings.ema_fast
        EMA_SLOW = runtime_settings.ema_slow
        RSI_PERIOD = runtime_settings.rsi_period
        ADX_PERIOD = runtime_settings.adx_period
        ATR_STOP_MULT = runtime_settings.atr_stop_multiplier
        MIN_RR = runtime_settings.min_risk_reward
        yield
    finally:
        (settings, ADX_MIN, EMA_FAST, EMA_SLOW, RSI_PERIOD, ADX_PERIOD,
         ATR_STOP_MULT, MIN_RR) = previous


def candles_to_frame(raw: list) -> pd.DataFrame:
    # CoinDCX futures REST and socket candles are normalized to six OHLCV fields.
    if not raw:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    width = len(raw[0])
    if width == 6:
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
    elif width >= 12:
        columns = [
            "timestamp", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades", "taker_buy_base",
            "taker_buy_quote", "ignore"
        ] + [f"extra_{i}" for i in range(width - 12)]
    else:
        raise ValueError(f"unsupported OHLCV row width: {width}")
    df = pd.DataFrame(raw, columns=columns)
    # Construct converted columns in a fresh frame. Avoid mutating the
    # original frame through assign/setitem paths under pandas Copy-on-Write.
    numeric_columns = ("timestamp", "open", "high", "low", "close", "volume")
    converted = pd.DataFrame({
        col: pd.to_numeric(df[col], errors="coerce")
        for col in numeric_columns
    }, index=df.index)
    extras = df.drop(columns=list(numeric_columns))
    df = pd.concat([converted, extras], axis=1)
    return df.dropna(subset=["timestamp", "close", "volume"]).reset_index(drop=True)

def timeframe_milliseconds(timeframe: str) -> int:
    """Convert supported minute timeframes to milliseconds."""
    if not timeframe.endswith("m") or not timeframe[:-1].isdigit() or int(timeframe[:-1]) <= 0:
        raise ValueError(f"unsupported timeframe for validation: {timeframe}")
    return int(timeframe[:-1]) * 60_000

def validate_ohlcv(
    raw: list, timeframe: str, now_ms: int | None = None,
    max_age_ms: int | None = None,
) -> pd.DataFrame:
    """Return sorted, unique, valid and fresh completed OHLCV candles."""
    interval_ms = timeframe_milliseconds(timeframe)
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    df = candles_to_frame(raw)
    if df.empty:
        raise ValueError(f"empty {timeframe} OHLCV response")
    # Identical repeated rows are harmless; conflicting rows at one exchange
    # timestamp are ambiguous and must not silently select an arbitrary price.
    duplicate_rows = df.loc[df["timestamp"].duplicated(keep=False)]
    if not duplicate_rows.empty:
        value_columns = ["open", "high", "low", "close", "volume"]
        conflicts = duplicate_rows.groupby("timestamp")[value_columns].nunique(dropna=False).gt(1).any(axis=1)
        if conflicts.any():
            raise ValueError(f"conflicting duplicate timestamps in {timeframe} OHLCV")
    df = df.sort_values("timestamp", kind="stable").drop_duplicates("timestamp", keep="last").reset_index(drop=True)
    numeric = ["timestamp", "open", "high", "low", "close", "volume"]
    if not np.isfinite(df[numeric].to_numpy(dtype=float)).all():
        raise ValueError(f"non-finite values in {timeframe} OHLCV")
    if (df[["open", "high", "low", "close"]] <= 0).any().any() or (df["volume"] < 0).any():
        raise ValueError(f"invalid price or volume in {timeframe} OHLCV")
    if (df["high"] < df[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError(f"inconsistent high in {timeframe} OHLCV")
    if (df["low"] > df[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError(f"inconsistent low in {timeframe} OHLCV")
    if (not np.equal(df["timestamp"].to_numpy(dtype=float), np.floor(df["timestamp"].to_numpy(dtype=float))).all()):
        raise ValueError(f"fractional timestamp in {timeframe} OHLCV")
    timestamps = df["timestamp"].astype("int64")
    if (timestamps % interval_ms != 0).any():
        raise ValueError(f"misaligned timestamp in {timeframe} OHLCV")
    if int(timestamps.iloc[-1]) > now_ms + 5_000:
        raise ValueError(f"future timestamp in {timeframe} OHLCV")
    # Exclude forming bars using the exchange-cycle clock, not row position.
    df = df.loc[df["timestamp"] + interval_ms <= now_ms - 1_500].copy().reset_index(drop=True)
    if df.empty:
        raise ValueError(f"no completed {timeframe} candles available")
    latest_close_ms = int(df.iloc[-1].timestamp) + interval_ms
    freshness_limit_ms = interval_ms + 30_000 if max_age_ms is None else int(max_age_ms)
    if freshness_limit_ms < 0:
        raise ValueError("max_age_ms must be non-negative")
    if now_ms - latest_close_ms > freshness_limit_ms:
        raise ValueError(
            f"stale {timeframe} candles: latest close is "
            f"{(now_ms-latest_close_ms)/1000:.0f}s old (max {freshness_limit_ms/1000:.0f}s)"
        )
    if len(df) >= 3 and not (df.timestamp.astype("int64").diff().iloc[-2:] == interval_ms).all():
        raise ValueError(f"missing interval in latest {timeframe} candles")
    return df

def has_contiguous_tail(df: pd.DataFrame, timeframe: str, required_bars: int) -> bool:
    """Require an uninterrupted, exchange-aligned tail for indicator calculations."""
    if required_bars < 2 or len(df) < required_bars or "timestamp" not in df:
        return False
    interval_ms = timeframe_milliseconds(timeframe)
    timestamps = pd.to_numeric(df["timestamp"].tail(required_bars), errors="coerce")
    if timestamps.isna().any():
        return False
    values = timestamps.to_numpy(dtype="int64")
    return bool((values[1:] - values[:-1] == interval_ms).all())


def indicators(df: pd.DataFrame, include_rsi: bool = True) -> pd.DataFrame:
    """Calculate indicators without mutating a possibly view-backed input frame.

    All derived columns are built as independent Series and concatenated once.
    This avoids pandas Copy-on-Write chained-assignment warnings while keeping
    the existing indicator formulas and values unchanged.
    """
    base = df.copy(deep=True)

    close = base["close"]
    high = base["high"]
    low = base["low"]
    volume = base["volume"]

    ema9 = close.ewm(span=EMA_FAST, adjust=False).mean()
    ema21 = close.ewm(span=EMA_SLOW, adjust=False).mean()

    if include_rsi:
        delta = close.diff()
        gain = delta.clip(lower=0).ewm(
            alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD
        ).mean()
        loss = (-delta.clip(upper=0)).ewm(
            alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD
        ).mean()
        rs = gain / loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.mask((loss == 0) & (gain > 0), 100)
        rsi = rsi.mask((loss == 0) & (gain == 0), 50)
    else:
        rsi = pd.Series(np.nan, index=base.index, dtype=float)

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low, (high - prev_close).abs(), (low - prev_close).abs()
    ], axis=1).max(axis=1)
    up = high.diff()
    down = -low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr = tr.ewm(
        alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD
    ).mean()
    plus_di = 100 * plus_dm.ewm(
        alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD
    ).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(
        alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD
    ).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    adx = dx.ewm(
        alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD
    ).mean()

    typical = (high + low + close) / 3
    utc_day = pd.to_datetime(base["timestamp"], unit="ms", utc=True).dt.date
    pv = typical * volume
    vwap = pv.groupby(utc_day).cumsum() / volume.groupby(utc_day).cumsum().replace(0, np.nan)

    derived = pd.DataFrame({
        "ema9": ema9,
        "ema21": ema21,
        "rsi": rsi,
        "adx": adx,
        "plus_di": plus_di,
        "minus_di": minus_di,
        "atr": atr,
        "vwap": vwap,
    }, index=base.index)
    return pd.concat([base, derived], axis=1)


def ema_alignment_age_bars(frame: pd.DataFrame, side: str) -> int | None:
    """Return closed 5m bars since the latest directional EMA9/EMA21 crossover.

    The crossover candle has age 0. A missing crossover in the supplied
    history returns None rather than treating a long-established trend as new.
    """
    if side not in {"LONG", "SHORT"} or not {"ema9", "ema21"}.issubset(frame.columns):
        return None
    spread = pd.to_numeric(frame["ema9"], errors="coerce") - pd.to_numeric(frame["ema21"], errors="coerce")
    previous = spread.shift(1)
    if side == "LONG":
        crosses = (previous <= 0) & (spread > 0)
    else:
        crosses = (previous >= 0) & (spread < 0)
    locations = np.flatnonzero(crosses.fillna(False).to_numpy())
    if len(locations) == 0:
        return None
    return int(len(frame) - 1 - locations[-1])


def evaluate_setup(
    df5: pd.DataFrame,
    df15: pd.DataFrame | None = None,
    rejection: dict | None = None,
) -> dict | None:
    """Evaluate one completed 5m candle independently of prior scanner state.

    EMA/VWAP/ADX/DI determine direction; the existing RSI band qualifies entry.
    15m remains optional descriptive context and never gates the 5m decision.
    When supplied, rejection receives an auditable reason on every no-signal
    path. No previous ARMED state or RSI crossing event is required.
    """
    def reject(reason: str):
        if rejection is not None:
            rejection["reason"] = reason
        return None

    if not has_contiguous_tail(df5, "5m", 60):
        return reject("insufficient_5m_history")

    a = indicators(df5.copy(), include_rsi=True)
    if len(a) < 35:
        return reject("insufficient_indicator_history")
    c = a.iloc[-1]
    long_alignment_age = ema_alignment_age_bars(a, "LONG")
    short_alignment_age = ema_alignment_age_bars(a, "SHORT")
    needed = [c.ema9, c.ema21, c.vwap, c.rsi, c.adx, c.atr, c.plus_di, c.minus_di]
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in needed):
        return reject("invalid_indicator_values")
    if float(c.atr) <= 0:
        return reject("invalid_atr")
    if rejection is not None:
        rejection.update({
            "rsi": float(c.rsi), "adx": float(c.adx), "atr": float(c.atr),
            "ema9": float(c.ema9), "ema21": float(c.ema21),
            "close": float(c.close), "vwap": float(c.vwap),
            "plus_di": float(c.plus_di), "minus_di": float(c.minus_di),
        })

    long_direction_ok = (
        float(c.ema9) > float(c.ema21)
        and float(c.close) > float(c.vwap)
        and float(c.adx) >= ADX_MIN
        and float(c.plus_di) > float(c.minus_di)
    )
    short_direction_ok = (
        float(c.ema9) < float(c.ema21)
        and float(c.close) < float(c.vwap)
        and float(c.adx) >= ADX_MIN
        and float(c.minus_di) > float(c.plus_di)
    )
    if not (long_direction_ok or short_direction_ok):
        return reject("directional_setup_not_met")
    side = "LONG" if long_direction_ok else "SHORT"
    if rejection is not None:
        rejection["direction"] = side

    # Apply the existing RSI band on this same completed candle. This is a
    # level condition, not a crossover event or a dependency on prior state.
    rsi_value = float(c.rsi)
    if side == "LONG" and not (settings.rsi_long_min <= rsi_value <= settings.rsi_long_max):
        return reject("rsi_outside_long_entry_band")
    if side == "SHORT" and not (settings.rsi_short_min <= rsi_value <= settings.rsi_short_max):
        return reject("rsi_outside_short_entry_band")

    # 15m is descriptive only. It never vetoes, delays, or cancels a valid 5m setup.
    context_row = None
    prev15 = None
    context = "15m unavailable"
    if df15 is not None and has_contiguous_tail(df15, "15m", 35):
        candidate_context = indicators(df15.copy(), include_rsi=False)
        if len(candidate_context) >= 35:
            candidate_row = candidate_context.iloc[-1]
            if all(pd.notna(v) and np.isfinite(float(v))
                   for v in (candidate_row.ema9, candidate_row.ema21)):
                context_row = candidate_row
                prev15 = candidate_context.iloc[-4]
                if float(candidate_row.ema9) > float(candidate_row.ema21):
                    context = "15m bullish"
                elif float(candidate_row.ema9) < float(candidate_row.ema21):
                    context = "15m bearish"
                else:
                    context = "15m neutral"

    entry = float(c.close)
    atr_value = float(c.atr)
    swing = a.iloc[-(settings.swing_lookback_bars + 1):-1]
    if len(swing) < settings.swing_lookback_bars:
        return reject("insufficient_swing_history")
    stop = float(swing["low"].min()) if side == "LONG" else float(swing["high"].max())
    risk = entry - stop if side == "LONG" else stop - entry
    if not np.isfinite(risk) or risk <= 0:
        return reject("invalid_stop_risk")
    target1 = entry + risk * settings.tp1_r if side == "LONG" else entry - risk * settings.tp1_r
    target2 = entry + risk * settings.tp2_r if side == "LONG" else entry - risk * settings.tp2_r
    prev5 = a.iloc[-4]

    diagnostics = {
        "version": 5,
        "ema_spread_atr": (float(c.ema9) - float(c.ema21)) / atr_value,
        "ema_alignment_age_bars": long_alignment_age if side == "LONG" else short_alignment_age,
        "ema9_slope_atr_3bars": (float(c.ema9) - float(prev5.ema9)) / atr_value,
        "ema21_slope_atr_3bars": (float(c.ema21) - float(prev5.ema21)) / atr_value,
        "price_vwap_distance_atr": (entry - float(c.vwap)) / atr_value,
        "rsi14": float(c.rsi),
        "adx14": float(c.adx),
        "atr_pct_of_price": 100.0 * atr_value / entry,
        "context_available": context_row is not None,
        **({
            "context_ema_spread_pct": 100.0 * (float(context_row.ema9) - float(context_row.ema21)) / float(context_row.ema21),
            "context_ema9_slope_pct_3bars": (
                100.0 * (float(context_row.ema9) - float(prev15.ema9)) / float(prev15.ema9)
                if prev15 is not None and pd.notna(prev15.ema9) and float(prev15.ema9) != 0
                else 0.0
            ),
        } if context_row is not None else {}),
    }
    if not all(
        value is None or isinstance(value, (bool, np.bool_)) or np.isfinite(float(value))
        for value in diagnostics.values()
    ):
        return reject("non_finite_diagnostics")

    if rejection is not None:
        rejection["reason"] = "VALID"
    return {
        "side": side, "entry": entry, "stop": stop,
        "target": target2, "target1": target1, "target2": target2,
        "risk_model": "swing_structure", "risk_distance": risk,
        "risk_pct": 100.0 * risk / entry,
        "target1_r": settings.tp1_r, "target2_r": settings.tp2_r,
        "rr": settings.tp2_r, "rsi": float(c.rsi), "adx": float(c.adx),
        "ema9": float(c.ema9), "ema21": float(c.ema21), "vwap": float(c.vwap),
        "plus_di": float(c.plus_di), "minus_di": float(c.minus_di),
        "atr": atr_value, "candle": int(c.timestamp),
        "candle_close": int(c.timestamp) + timeframe_milliseconds("5m"),
        "context": context,
        "diagnostics": diagnostics,
    }


def evaluate_signal(df5: pd.DataFrame, df15: pd.DataFrame | None = None) -> dict | None:
    """Backward-compatible standalone evaluator using direct current-candle rules.

    Keep the pre-v2 public helper's ATR stop and configured risk/reward contract
    for existing integrations. The production scanner uses evaluate_setup and
    the swing-based TP1/TP2 levels.
    """
    signal = evaluate_setup(df5, df15)
    if signal is None:
        return None
    risk = float(signal["atr"]) * ATR_STOP_MULT
    entry = float(signal["entry"])
    side = signal["side"]
    signal["stop"] = entry - risk if side == "LONG" else entry + risk
    signal["target"] = entry + risk * MIN_RR if side == "LONG" else entry - risk * MIN_RR
    # Keep the legacy single-target contract, but make all supplemental
    # target fields use the same ATR risk model rather than leaking the
    # swing-based levels produced by evaluate_setup().
    signal["target1"] = entry + risk * settings.tp1_r if side == "LONG" else entry - risk * settings.tp1_r
    signal["target2"] = entry + risk * settings.tp2_r if side == "LONG" else entry - risk * settings.tp2_r
    signal["risk_model"] = "legacy_atr"
    signal["risk_distance"] = risk
    signal["risk_pct"] = 100.0 * risk / entry
    signal["target1_r"] = settings.tp1_r
    signal["target2_r"] = settings.tp2_r
    signal["rr"] = MIN_RR
    return signal


def evaluate_state_inputs(df5: pd.DataFrame, df15: pd.DataFrame | None = None, armed_sides: set[str] | None = None) -> dict | None:
    """Legacy/shadow lifecycle inputs; the live scanner does not call this.

    The live signal path uses evaluate_setup on each completed 5m candle and
    does not require ARMED state or an RSI crossing event.
    """
    if not has_contiguous_tail(df5, "5m", 60):
        return None
    armed_sides = armed_sides or set()
    a = indicators(df5.copy(), include_rsi=bool(armed_sides))
    if len(a) < 35:
        return None
    c, p = a.iloc[-1], a.iloc[-2]
    required = (c.ema9, c.ema21, c.vwap, c.adx, c.plus_di, c.minus_di)
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in required):
        return None

    # Keep crossover age for diagnostics only. Persistent EMA alignment is
    # evaluated from the current closed 5m candle and is not age-limited.
    common = float(c.adx) >= ADX_MIN
    long_setup = (
        common and float(c.plus_di) > float(c.minus_di)
        and float(c.ema9) > float(c.ema21) and float(c.close) > float(c.vwap)
    )
    short_setup = (
        common and float(c.minus_di) > float(c.plus_di)
        and float(c.ema9) < float(c.ema21) and float(c.close) < float(c.vwap)
    )
    long_invalid = (
        float(c.ema9) <= float(c.ema21) or float(c.close) <= float(c.vwap)
        or float(c.adx) < ADX_MIN or float(c.plus_di) <= float(c.minus_di)
    )
    short_invalid = (
        float(c.ema9) >= float(c.ema21) or float(c.close) >= float(c.vwap)
        or float(c.adx) < ADX_MIN or float(c.minus_di) <= float(c.plus_di)
    )
    long_trigger = (
        long_setup and "LONG" in armed_sides and pd.notna(p.rsi) and pd.notna(c.rsi)
        and float(p.rsi) < settings.rsi_long_min
        and settings.rsi_long_min <= float(c.rsi) <= settings.rsi_long_max
    )
    short_trigger = (
        short_setup and "SHORT" in armed_sides and pd.notna(p.rsi) and pd.notna(c.rsi)
        and float(p.rsi) > settings.rsi_short_max
        and settings.rsi_short_min <= float(c.rsi) <= settings.rsi_short_max
    )
    return {
        "LONG": {"setup_valid": bool(long_setup), "invalidation": bool(long_invalid),
                 "trigger": bool(long_trigger)},
        "SHORT": {"setup_valid": bool(short_setup), "invalidation": bool(short_invalid),
                  "trigger": bool(short_trigger)},
    }
