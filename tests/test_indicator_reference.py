import math
import unittest

import numpy as np
import pandas as pd

from strategy import ADX_PERIOD, EMA_FAST, EMA_SLOW, RSI_PERIOD, indicators


def wilder_reference(values, period):
    """Independent recursive Wilder smoother with pandas-compatible first-value seed."""
    result = [math.nan] * len(values)
    average = None
    observations = 0
    for index, value in enumerate(values):
        if value is None or not math.isfinite(float(value)):
            continue
        value = float(value)
        if average is None:
            average = value
        else:
            average += (value - average) / period
        observations += 1
        if observations >= period:
            result[index] = average
    return np.asarray(result, dtype=float)


def reference_indicators(frame):
    close = frame["close"].to_numpy(dtype=float)
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    delta = np.diff(close, prepend=np.nan)
    gains = np.where(np.isnan(delta), np.nan, np.maximum(delta, 0.0))
    losses = np.where(np.isnan(delta), np.nan, np.maximum(-delta, 0.0))
    avg_gain = wilder_reference(gains, RSI_PERIOD)
    avg_loss = wilder_reference(losses, RSI_PERIOD)
    rsi = np.full(len(close), np.nan)
    valid = np.isfinite(avg_gain) & np.isfinite(avg_loss)
    for i in np.where(valid)[0]:
        if avg_loss[i] == 0 and avg_gain[i] > 0:
            rsi[i] = 100.0
        elif avg_loss[i] == 0 and avg_gain[i] == 0:
            rsi[i] = 50.0
        else:
            rs = avg_gain[i] / avg_loss[i]
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)

    prev_close = np.roll(close, 1)
    prev_close[0] = np.nan
    true_range = np.nanmax(np.vstack([
        high-low, np.abs(high-prev_close), np.abs(low-prev_close)
    ]), axis=0)
    up = np.diff(high, prepend=np.nan)
    down = -np.diff(low, prepend=np.nan)
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    atr = wilder_reference(true_range, ADX_PERIOD)
    smooth_plus = wilder_reference(plus_dm, ADX_PERIOD)
    smooth_minus = wilder_reference(minus_dm, ADX_PERIOD)
    plus_di = 100.0 * smooth_plus / atr
    minus_di = 100.0 * smooth_minus / atr
    denominator = plus_di + minus_di
    dx = np.where(denominator > 0, 100.0 * np.abs(plus_di-minus_di) / denominator, np.nan)
    adx = wilder_reference(dx, ADX_PERIOD)

    def ema(values, span):
        alpha = 2.0 / (span + 1.0)
        out = []
        previous = None
        for value in values:
            previous = float(value) if previous is None else alpha*float(value)+(1-alpha)*previous
            out.append(previous)
        return np.asarray(out)

    return {
        "rsi": rsi, "atr": atr, "plus_di": plus_di, "minus_di": minus_di,
        "adx": adx, "ema9": ema(close, EMA_FAST), "ema21": ema(close, EMA_SLOW),
    }


class WilderIndicatorReferenceTests(unittest.TestCase):
    def test_rsi_adx_di_atr_and_ema_match_independent_wilder_reference(self):
        count = 240
        index = np.arange(count, dtype=float)
        close = 100 + 0.035*index + 1.8*np.sin(index/5.3) + 0.6*np.sin(index/1.9)
        high = close + 0.4 + 0.15*(1 + np.sin(index/3.1))
        low = close - 0.35 - 0.12*(1 + np.cos(index/4.2))
        frame = pd.DataFrame({
            "timestamp": 1_795_000_000_000 + np.arange(count, dtype=np.int64)*300_000,
            "open": close - 0.05, "high": high, "low": low,
            "close": close, "volume": 100 + (index % 17)*3,
        })
        expected = reference_indicators(frame)
        actual = indicators(frame)
        last = count - 1
        for field in ("rsi", "atr", "plus_di", "minus_di", "adx", "ema9", "ema21"):
            self.assertTrue(math.isfinite(float(actual[field].iloc[last])), field)
            self.assertAlmostEqual(
                float(actual[field].iloc[last]), float(expected[field][last]),
                places=8, msg=field,
            )

    def test_vwap_resets_at_utc_midnight_and_idle_path_skips_rsi(self):
        start = pd.Timestamp("2026-11-19T23:00:00Z").value // 1_000_000
        timestamps = start + np.arange(24, dtype=np.int64)*300_000
        prices = np.arange(100, 124, dtype=float)
        frame = pd.DataFrame({
            "timestamp": timestamps, "open": prices, "high": prices+1,
            "low": prices-1, "close": prices, "volume": np.ones(24),
        })
        original = frame.copy(deep=True)
        with_rsi = indicators(frame, include_rsi=True)
        without_rsi = indicators(frame, include_rsi=False)
        self.assertTrue(without_rsi["rsi"].isna().all())
        self.assertTrue(frame.equals(original))
        utc_dates = pd.to_datetime(timestamps, unit="ms", utc=True).date
        current_day = utc_dates[-1]
        mask = utc_dates == current_day
        typical = (frame.loc[mask, "high"] + frame.loc[mask, "low"] + frame.loc[mask, "close"]) / 3
        expected_vwap = float(typical.mean())
        self.assertAlmostEqual(float(with_rsi["vwap"].iloc[-1]), expected_vwap, places=10)


if __name__ == "__main__":
    unittest.main()
