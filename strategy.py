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
    for col in ("timestamp", "open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["timestamp", "close", "volume"]).reset_index(drop=True)

def timeframe_milliseconds(timeframe: str) -> int:
    """Convert supported minute timeframes to milliseconds."""
    if not timeframe.endswith("m") or not timeframe[:-1].isdigit() or int(timeframe[:-1]) <= 0:
        raise ValueError(f"unsupported timeframe for validation: {timeframe}")
    return int(timeframe[:-1]) * 60_000

def validate_ohlcv(raw: list, timeframe: str, now_ms: int | None = None) -> pd.DataFrame:
    """Return sorted, unique, valid and fresh completed OHLCV candles."""
    interval_ms = timeframe_milliseconds(timeframe)
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    df = candles_to_frame(raw)
    if df.empty:
        raise ValueError(f"empty {timeframe} OHLCV response")
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
    if now_ms - latest_close_ms > interval_ms + 30_000:
        raise ValueError(f"stale {timeframe} candles: latest close is {(now_ms-latest_close_ms)/1000:.0f}s old")
    if len(df) >= 3 and not (df.timestamp.astype("int64").diff().iloc[-2:] == interval_ms).all():
        raise ValueError(f"missing interval in latest {timeframe} candles")
    return df

def indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate indicators on OHLCV bars; VWAP resets at UTC day boundary."""
    x = df.copy()
    x["ema9"] = x.close.ewm(span=EMA_FAST, adjust=False).mean()
    x["ema21"] = x.close.ewm(span=EMA_SLOW, adjust=False).mean()

    delta = x.close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()
    rs = gain / loss.replace(0, np.nan)
    x["rsi"] = 100 - (100 / (1 + rs))
    x.loc[(loss == 0) & (gain > 0), "rsi"] = 100
    x.loc[(loss == 0) & (gain == 0), "rsi"] = 50

    prev_close = x.close.shift(1)
    tr = pd.concat([
        x.high - x.low, (x.high - prev_close).abs(), (x.low - prev_close).abs()
    ], axis=1).max(axis=1)
    up = x.high.diff()
    down = -x.low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    x["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()
    x["atr"] = atr

    typical = (x.high + x.low + x.close) / 3
    utc_day = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.date
    pv = typical * x.volume
    x["vwap"] = pv.groupby(utc_day).cumsum() / x.volume.groupby(utc_day).cumsum().replace(0, np.nan)
    return x

def evaluate_setup(df5: pd.DataFrame, df15: pd.DataFrame | None = None) -> dict | None:
    """Return a setup only on the latest completed 5m EMA9/21 crossover.

    15m data is optional and informational only. All entry gates (VWAP, RSI,
    ADX and directional DI) are evaluated on the same completed 5m candle.
    """
    if len(df5) < 60:
        return None
    a = indicators(df5.copy())
    if len(a) < 35:
        return None
    c, p = a.iloc[-1], a.iloc[-2]
    needed = [c.ema9, c.ema21, p.ema9, p.ema21, c.vwap, c.rsi,
              c.adx, c.atr, c.plus_di, c.minus_di]
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in needed):
        return None

    # Fresh cross on this candle only: an already-aligned EMA pair is not an entry.
    long_cross = p.ema9 <= p.ema21 and c.ema9 > c.ema21
    short_cross = p.ema9 >= p.ema21 and c.ema9 < c.ema21
    long_ok = (long_cross and c.close > c.vwap and c.rsi > 50
               and c.adx >= ADX_MIN and c.plus_di > c.minus_di)
    short_ok = (short_cross and c.close < c.vwap and c.rsi < 50
                and c.adx >= ADX_MIN and c.minus_di > c.plus_di)
    if not (long_ok or short_ok):
        return None

    side = "LONG" if long_ok else "SHORT"
    entry, atr_value = float(c.close), float(c.atr)
    if not np.isfinite(atr_value) or atr_value <= 0:
        return None
    risk = atr_value * ATR_STOP_MULT
    context = "15m unavailable"
    if df15 is not None and len(df15) >= 35:
        context_frame = indicators(df15.copy())
        context_row = context_frame.iloc[-1]
        if pd.notna(context_row.ema9) and pd.notna(context_row.ema21):
            context = ("15m bullish" if context_row.ema9 > context_row.ema21
                       else "15m bearish" if context_row.ema9 < context_row.ema21
                       else "15m neutral")
    return {
        "side": side, "entry": entry,
        "stop": entry - risk if side == "LONG" else entry + risk,
        "target": entry + risk * MIN_RR if side == "LONG" else entry - risk * MIN_RR,
        "rr": MIN_RR, "rsi": float(c.rsi), "adx": float(c.adx),
        "plus_di": float(c.plus_di), "minus_di": float(c.minus_di),
        "atr": atr_value, "vwap": float(c.vwap), "candle": int(c.timestamp),
        "candle_close": int(c.timestamp) + timeframe_milliseconds("5m"),
        "context": context,
    }


def evaluate_signal(df5: pd.DataFrame, df15: pd.DataFrame | None = None) -> dict | None:
    """Strict same-candle 5m signal evaluator; never queues delayed candidates."""
    return evaluate_setup(df5, df15)
