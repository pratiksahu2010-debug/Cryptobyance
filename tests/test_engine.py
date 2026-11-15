import unittest
import numpy as np
import pandas as pd

from main import SYMBOLS, candles_to_frame, indicators, evaluate_signal, validate_ohlcv

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

    def test_ccxt_six_column_candle_parser(self):
        raw = [[1700000000000, 1, 2, .5, 1.5, 10]]
        frame = candles_to_frame(raw)
        self.assertEqual(len(frame), 1)
        self.assertEqual(float(frame.iloc[0].close), 1.5)
        self.assertEqual(float(frame.iloc[0].volume), 10)

    def test_validator_sorts_deduplicates_and_excludes_forming_bar(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = []
        for i in range(5):
            price = 100 + i
            raw.append([base + i * interval, price, price + 2, price - 1, price + 1, 10])
        raw = list(reversed(raw))
        # Append the duplicate last so keep="last" is tested independently of sort order.
        raw.append([base + interval, 101, 103, 100, 102, 25])
        now_ms = base + 4 * interval + 2000
        frame = validate_ohlcv(raw, "5m", now_ms)
        self.assertEqual(len(frame), 4)
        self.assertEqual(frame.timestamp.tolist(), sorted(frame.timestamp.tolist()))
        self.assertEqual(float(frame.iloc[1].volume), 25)
        self.assertEqual(int(frame.iloc[-1].timestamp), base + 3 * interval)

    def test_validator_rejects_stale_candles(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 102, 99, 101, 10] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "stale"):
            validate_ohlcv(raw, "5m", base + 8 * interval)

    def test_validator_rejects_recent_missing_interval(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 102, 99, 101, 10] for i in (0, 1, 3, 4)]
        with self.assertRaisesRegex(ValueError, "missing interval"):
            validate_ohlcv(raw, "5m", base + 5 * interval + 2000)

    def test_validator_rejects_inconsistent_ohlc(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 99, 98, 101, 10] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "inconsistent high"):
            validate_ohlcv(raw, "5m", base + 4 * interval + 2000)

    def _trend_frame(self, direction: int, interval_ms: int) -> pd.DataFrame:
        n = 400
        base = np.linspace(100, 200, n) if direction > 0 else np.linspace(200, 100, n)
        close = base
        return pd.DataFrame({
            "timestamp": 1700000000000 + np.arange(n) * interval_ms,
            "open": close - direction * .1,
            "high": close + .5,
            "low": close - .5,
            "close": close,
            "volume": np.full(n, 100.0),
        })

    def test_strict_engine_emits_long_for_aligned_uptrend(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertGreaterEqual(signal["adx"], 17)

    def test_strict_engine_emits_short_for_aligned_downtrend(self):
        signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")
        self.assertGreaterEqual(signal["adx"], 17)

if __name__ == "__main__":
    unittest.main()
