import unittest

import pandas as pd

from feature_snapshot import build_indicator_snapshot


class FeatureSnapshotTests(unittest.TestCase):
    def test_snapshot_includes_previous_rsi_time_context_and_state_cycle(self):
        candles = pd.DataFrame({
            "timestamp": [1_800_000 + i * 300_000 for i in range(20)],
            "open": [100.0] * 20, "high": [102.0] * 20,
            "low": [99.0] * 20, "close": [101.0] * 20,
            "volume": [10.0 + i for i in range(20)],
        })
        indicator_frame = candles.copy()
        indicator_frame["ema9"] = [100.0] * 19 + [101.0]
        indicator_frame["ema21"] = [100.0] * 20
        indicator_frame["rsi"] = [49.0] * 19 + [56.0]
        indicator_frame["adx"] = [25.0] * 20
        indicator_frame["plus_di"] = [30.0] * 20
        indicator_frame["minus_di"] = [15.0] * 20
        indicator_frame["vwap"] = [100.0] * 20
        indicator_frame["atr"] = [2.0] * 20
        candle_open = int(candles.iloc[-1]["timestamp"])
        signal = {
            "context": "15m bullish",
            "context_rsi14": 54.0,
            "context_rsi_aligned": True,
            "context_rsi_score": 1,
        }
        snapshot = build_indicator_snapshot(
            indicator_frame, candles, signal,
            candle_open_ms=candle_open,
            config_version="v3",
            cooldown_candles=4,
            state_metadata={
                "armed_candle_open_ms": candle_open - 600_000,
                "cycle_id": 17,
            },
        )
        self.assertEqual(snapshot["previous_rsi14"], 49.0)
        self.assertEqual(snapshot["rsi14"], 56.0)
        self.assertAlmostEqual(snapshot["ema_spread_pct"], 1.0)
        self.assertAlmostEqual(snapshot["price_vwap_distance_pct"], 1.0)
        self.assertAlmostEqual(snapshot["atr_pct_of_price"], 100.0 * 2.0 / 101.0)
        self.assertEqual(snapshot["bars_since_armed"], 2)
        self.assertEqual(snapshot["state_cycle_id"], 17)
        self.assertEqual(snapshot["config_version"], "v3")
        self.assertEqual(snapshot["cooldown_candles"], 4)
        self.assertEqual(snapshot["market_trend_15m"], "15m bullish")
        self.assertIsNone(snapshot["index_trend"])
        self.assertIn("time_of_day_utc", snapshot)
        self.assertIn("day_of_week_utc", snapshot)
        self.assertIsNotNone(snapshot["relative_volume_20"])


if __name__ == "__main__":
    unittest.main()
