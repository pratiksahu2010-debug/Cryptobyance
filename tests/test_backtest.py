import unittest

import pandas as pd

from backtest import completed_context_15m, run_backtest, validate_history_frame


def frame(interval_ms, count, *, start=0):
    rows = []
    for index in range(count):
        rows.append({
            "timestamp": start + index * interval_ms,
            "open": 100.0, "high": 101.0, "low": 99.0,
            "close": 100.0, "volume": 10.0,
        })
    return pd.DataFrame(rows)


class BacktestTests(unittest.TestCase):
    def test_historical_frame_requires_contiguous_candles(self):
        candles = frame(300_000, 4)
        candles.loc[3, "timestamp"] += 300_000
        with self.assertRaisesRegex(ValueError, "missing or overlapping"):
            validate_history_frame(candles, 300_000, "5m")

    def test_historical_frame_rejects_duplicate_timestamps(self):
        candles = frame(300_000, 4)
        candles.loc[3, "timestamp"] = candles.loc[2, "timestamp"]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_history_frame(candles, 300_000, "5m")

    def test_context_filter_excludes_unclosed_15m_candle(self):
        candles = frame(900_000, 4)
        available = completed_context_15m(candles, current_5m_close_ms=1_800_000)
        self.assertEqual(available["timestamp"].tolist(), [0, 900_000])

    def test_flat_market_replay_produces_no_false_signals(self):
        five = frame(300_000, 200)
        fifteen = frame(900_000, 40)
        result = run_backtest(five, fifteen, cooldown_candles=4, horizon_bars=12, cost_pct=0.10)
        self.assertEqual(result["summary"]["signals"], 0)
        self.assertTrue(result["summary"]["no_lookahead"])
        self.assertEqual(result["trades"], [])


if __name__ == "__main__":
    unittest.main()
