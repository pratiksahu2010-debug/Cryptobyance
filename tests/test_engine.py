import unittest
import numpy as np
import pandas as pd

from main import SYMBOLS, candles_to_frame, indicators, evaluate_signal

class EngineTests(unittest.TestCase):
    def test_universe_has_exactly_60_unique_symbols(self):
        self.assertEqual(len(SYMBOLS), 60)
        self.assertEqual(len(set(SYMBOLS)), 60)

    def test_indicators_produce_expected_columns(self):
        n = 400
        close = np.linspace(100, 150, n) + np.sin(np.arange(n) / 5)
        df = pd.DataFrame({
            "timestamp": np.arange(n) * 300000 + 1700000000000,
            "open": close - .2, "high": close + .5, "low": close - .5,
            "close": close, "volume": np.full(n, 100.0),
        })
        result = indicators(df)
        for col in ("ema9", "ema21", "rsi", "adx", "atr", "vwap"):
            self.assertIn(col, result.columns)
        self.assertTrue(np.isfinite(result.iloc[-1]["adx"]))

    def test_candle_parser(self):
        raw = [[1700000000000, "1", "2", ".5", "1.5", "10",
                1700000299999, "15", 5, "6", "9", "0"]]
        frame = candles_to_frame(raw)
        self.assertEqual(float(frame.iloc[0].close), 1.5)
        self.assertEqual(float(frame.iloc[0].volume), 10)

if __name__ == "__main__":
    unittest.main()
