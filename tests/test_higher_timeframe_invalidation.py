"""Regression tests for higher-timeframe trend-reversal invalidation."""
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from strategy import evaluate_state_inputs


class HigherTimeframeInvalidationTests(unittest.TestCase):
    def _five_minute_inputs(self, side):
        n = 60
        is_long = side == "LONG"
        close = 102.0 if is_long else 98.0
        frame = pd.DataFrame({
            "timestamp": 1_800_000_000_000 + np.arange(n) * 300_000,
            "open": np.full(n, close), "high": np.full(n, close + 1),
            "low": np.full(n, close - 1), "close": np.full(n, close),
            "volume": np.full(n, 100.0),
            "ema9": np.full(n, 101.0 if is_long else 99.0),
            "ema21": np.full(n, 100.0), "vwap": np.full(n, 100.0),
            "adx": np.full(n, 25.0),
            "plus_di": np.full(n, 30.0 if is_long else 10.0),
            "minus_di": np.full(n, 10.0 if is_long else 30.0),
            "rsi": np.full(n, 60.0 if is_long else 40.0),
        })
        return frame

    def _fifteen_minute_raw(self, trend):
        n = 35
        if trend == "BULLISH":
            close = np.linspace(100.0, 110.0, n)
        elif trend == "BEARISH":
            close = np.linspace(110.0, 100.0, n)
        else:
            close = np.full(n, 100.0)
        return pd.DataFrame({
            "timestamp": 1_800_000_000_000 + np.arange(n) * 900_000,
            "open": close, "high": close + 1, "low": close - 1,
            "close": close, "volume": np.full(n, 100.0),
        })

    def _evaluate(self, side, context_trend):
        five = self._five_minute_inputs(side)
        fifteen = self._fifteen_minute_raw(context_trend)
        with patch("strategy.indicators", return_value=five):
            return evaluate_state_inputs(five, fifteen, armed_sides={side})

    def test_bearish_15m_reversal_invalidates_armed_long(self):
        result = self._evaluate("LONG", "BEARISH")
        self.assertTrue(result["LONG"]["invalidation"])
        self.assertFalse(result["LONG"]["setup_valid"])

    def test_bullish_15m_reversal_invalidates_armed_short(self):
        result = self._evaluate("SHORT", "BULLISH")
        self.assertTrue(result["SHORT"]["invalidation"])
        self.assertFalse(result["SHORT"]["setup_valid"])

    def test_aligned_15m_trend_does_not_invalidate_matching_setup(self):
        long_result = self._evaluate("LONG", "BULLISH")
        short_result = self._evaluate("SHORT", "BEARISH")
        self.assertFalse(long_result["LONG"]["invalidation"])
        self.assertTrue(long_result["LONG"]["setup_valid"])
        self.assertFalse(short_result["SHORT"]["invalidation"])
        self.assertTrue(short_result["SHORT"]["setup_valid"])

    def test_flat_15m_ema_context_invalidates_both_directions(self):
        self.assertTrue(self._evaluate("LONG", "FLAT")["LONG"]["invalidation"])
        self.assertTrue(self._evaluate("SHORT", "FLAT")["SHORT"]["invalidation"])

    def test_invalid_15m_data_defers_without_false_invalidation(self):
        five = self._five_minute_inputs("LONG")
        fifteen = self._fifteen_minute_raw("BULLISH")
        fifteen["close"] = np.nan
        with patch("strategy.indicators", return_value=five):
            self.assertIsNone(evaluate_state_inputs(five, fifteen, armed_sides={"LONG"}))


if __name__ == "__main__":
    unittest.main()
