"""Pure candle validation and single-engine LONG/SHORT strategy logic.

This module has no exchange, network, Telegram, or application lifecycle code.
"""
from __future__ import annotations

import time

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
MAX_EMA_ALIGNMENT_AGE_BARS = 3
MAX_ENTRY_EXTENSION_ATR = 1.0


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


def evaluate_setup(df5: pd.DataFrame, df15: pd.DataFrame | None = None) -> dict | None:
    """Simple completed-5m EMA/VWAP/RSI/ADX strategy with a hard 15m EMA veto.

    LONG: 5m EMA9 > EMA21, close > VWAP, RSI14 > 50, ADX14 >= 20.
    SHORT: exact inverse. 15m EMA9/EMA21 must agree with the signal direction.
    ATR14 defines the protective stop distance and configured R-multiple target.
    No extra slope, alignment-age, DI, or VWAP-extension entry gates are applied.
    """
    if (not has_contiguous_tail(df5, "5m", 60)
            or df15 is None
            or not has_contiguous_tail(df15, "15m", 35)):
        return None

    armed_sides = armed_sides or set()
    a = indicators(df5.copy(), include_rsi=bool(armed_sides))
    if len(a) < 35:
        return None
    c = a.iloc[-1]
    needed = [c.ema9, c.ema21, c.vwap, c.rsi, c.adx, c.atr]
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in needed):
        return None
    if float(c.atr) <= 0:
        return None

    long_ok = (
        float(c.ema9) > float(c.ema21)
        and float(c.close) > float(c.vwap)
        and float(c.rsi) > 50.0
        and float(c.adx) >= ADX_MIN
    )
    short_ok = (
        float(c.ema9) < float(c.ema21)
        and float(c.close) < float(c.vwap)
        and float(c.rsi) < 50.0
        and float(c.adx) >= ADX_MIN
    )
    if not (long_ok or short_ok):
        return None
    side = "LONG" if long_ok else "SHORT"

    # Mandatory 15m trend veto: no signal against the higher-timeframe EMA bias.
    context_frame = indicators(df15.copy())
    if len(context_frame) < 35:
        return None
    context_row = context_frame.iloc[-1]
    if not all(pd.notna(v) and np.isfinite(float(v))
               for v in (context_row.ema9, context_row.ema21)):
        return None
    if float(context_row.ema9) > float(context_row.ema21):
        context = "15m bullish"
    elif float(context_row.ema9) < float(context_row.ema21):
        context = "15m bearish"
    else:
        return None
    if (side == "LONG" and context != "15m bullish") or (side == "SHORT" and context != "15m bearish"):
        return None

    entry = float(c.close)
    atr_value = float(c.atr)
    swing = df5.iloc[-(settings.swing_lookback_bars + 1):-1]
    if len(swing) < settings.swing_lookback_bars:
        return None
    stop = float(swing["low"].min()) if side == "LONG" else float(swing["high"].max())
    risk = entry - stop if side == "LONG" else stop - entry
    if not np.isfinite(risk) or risk <= 0:
        return None
    target1 = entry + risk * settings.tp1_r if side == "LONG" else entry - risk * settings.tp1_r
    target2 = entry + risk * settings.tp2_r if side == "LONG" else entry - risk * settings.tp2_r
    prev5 = a.iloc[-4]
    prev15 = context_frame.iloc[-4]
    diagnostics = {
        "version": 2,
        "ema_spread_atr": (float(c.ema9) - float(c.ema21)) / atr_value,
        "ema9_slope_atr_3bars": (float(c.ema9) - float(prev5.ema9)) / atr_value,
        "ema21_slope_atr_3bars": (float(c.ema21) - float(prev5.ema21)) / atr_value,
        "price_vwap_distance_atr": (entry - float(c.vwap)) / atr_value,
        "rsi14": float(c.rsi),
        "adx14": float(c.adx),
        "atr_pct_of_price": 100.0 * atr_value / entry,
        "context_ema_spread_pct": 100.0 * (float(context_row.ema9) - float(context_row.ema21)) / float(context_row.ema21),
        "context_ema9_slope_pct_3bars": 100.0 * (float(context_row.ema9) - float(prev15.ema9)) / float(prev15.ema9),
    }
    if not all(np.isfinite(float(value)) for value in diagnostics.values()):
        return None

    return {
        "side": side, "entry": entry, "stop": stop,
        "target": target2, "target1": target1, "target2": target2,
        "rr": settings.tp2_r, "rsi": float(c.rsi), "adx": float(c.adx),
        "ema9": float(c.ema9), "ema21": float(c.ema21), "vwap": float(c.vwap),
        "plus_di": float(c.plus_di) if pd.notna(c.plus_di) else None,
        "minus_di": float(c.minus_di) if pd.notna(c.minus_di) else None,
        "atr": atr_value, "vwap": float(c.vwap), "candle": int(c.timestamp),
        "candle_close": int(c.timestamp) + timeframe_milliseconds("5m"),
        "context": context, "diagnostics": diagnostics,
    }


def evaluate_signal(df5: pd.DataFrame, df15: pd.DataFrame | None = None) -> dict | None:
    """Strict same-candle 5m signal evaluator; never queues delayed candidates."""
    return evaluate_setup(df5, df15)


def evaluate_state_inputs(df5: pd.DataFrame, df15: pd.DataFrame | None = None, armed_sides: set[str] | None = None) -> dict:
    """Return per-side setup, invalidation and RSI-cross inputs from closed bars.

    The caller persists state separately for LONG and SHORT. 15m EMA direction
    remains the existing higher-timeframe veto; it is not a substitute for the
    explicit 5m DI/ADX setup conditions.
    """
    if not has_contiguous_tail(df5, "5m", 60) or df15 is None or not has_contiguous_tail(df15, "15m", 35):
        return None
    armed_sides = armed_sides or set()
    a = indicators(df5.copy(), include_rsi=bool(armed_sides))
    if len(a) < 35:
        return None
    c, p = a.iloc[-1], a.iloc[-2]
    required = (c.ema9, c.ema21, c.vwap, c.adx, c.plus_di, c.minus_di)
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in required):
        return {"LONG": {"setup_valid": False, "invalidation": True, "trigger": False},
                "SHORT": {"setup_valid": False, "invalidation": True, "trigger": False}}
    ctx = indicators(df15.copy(), include_rsi=False).iloc[-1]
    context_long = pd.notna(ctx.ema9) and pd.notna(ctx.ema21) and float(ctx.ema9) > float(ctx.ema21)
    context_short = pd.notna(ctx.ema9) and pd.notna(ctx.ema21) and float(ctx.ema9) < float(ctx.ema21)
    common = float(c.adx) >= ADX_MIN
    long_setup = common and float(c.plus_di) > float(c.minus_di) and float(c.ema9) > float(c.ema21) and float(c.close) > float(c.vwap) and context_long
    short_setup = common and float(c.minus_di) > float(c.plus_di) and float(c.ema9) < float(c.ema21) and float(c.close) < float(c.vwap) and context_short
    long_invalid = (float(c.ema9) <= float(c.ema21) or float(c.close) <= float(c.vwap)
                    or float(c.adx) < ADX_MIN or float(c.plus_di) <= float(c.minus_di))
    short_invalid = (float(c.ema9) >= float(c.ema21) or float(c.close) >= float(c.vwap)
                     or float(c.adx) < ADX_MIN or float(c.minus_di) <= float(c.plus_di))
    long_trigger = (long_setup and "LONG" in armed_sides and pd.notna(p.rsi) and pd.notna(c.rsi)
                    and float(p.rsi) < settings.rsi_long_min
                    and settings.rsi_long_min <= float(c.rsi) <= settings.rsi_long_max)
    short_trigger = (short_setup and "SHORT" in armed_sides and pd.notna(p.rsi) and pd.notna(c.rsi)
                     and float(p.rsi) > settings.rsi_short_max
                     and settings.rsi_short_min <= float(c.rsi) <= settings.rsi_short_max)
    return {
        "LONG": {"setup_valid": bool(long_setup), "invalidation": bool(long_invalid),
                 "trigger": bool(long_trigger)},
        "SHORT": {"setup_valid": bool(short_setup), "invalidation": bool(short_invalid),
                  "trigger": bool(short_trigger)},
    }
