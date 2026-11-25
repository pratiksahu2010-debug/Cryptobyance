import unittest

import pandas as pd

from feature_snapshot import build_indicator_snapshot


class FeatureSnapshotTests(unittest.TestCase):
    def _frames(self):
        candles = pd.DataFrame({
            "timestamp": [1_800_000 + i * 300_000 for i in range(20)],
            "open": [100.0] * 20, "high": [102.0] * 20,
            "low": [99.0] * 20, "close": [100.0 + i * 0.1 for i in range(20)],
            "volume": [10.0 + i for i in range(20)],
        })
        indicator_frame = candles.copy()
        indicator_frame["ema9"] = [99.0 + i * 0.1 for i in range(20)]
        indicator_frame["ema21"] = [98.0 + i * 0.1 for i in range(20)]
        indicator_frame["rsi"] = [50.0 + i for i in range(20)]
        indicator_frame["adx"] = [25.0] * 20
        indicator_frame["plus_di"] = [30.0 + i for i in range(20)]
        indicator_frame["minus_di"] = [15.0] * 20
        indicator_frame["vwap"] = [100.0] * 20
        indicator_frame["atr"] = [2.0] * 20
        return candles, indicator_frame

    def test_snapshot_includes_previous_rsi_time_context_and_state_cycle(self):
        candles, indicator_frame = self._frames()
        candle_open = int(candles.iloc[-1]["timestamp"])
        signal = {
            "side": "LONG", "context": "15m bullish",
            "context_rsi14": 54.0, "context_rsi_aligned": True,
            "context_rsi_score": 1,
        }
        snapshot = build_indicator_snapshot(
            indicator_frame, candles, signal,
            candle_open_ms=candle_open, config_version="v3", cooldown_candles=4,
            state_metadata={"armed_candle_open_ms": candle_open - 600_000, "cycle_id": 17},
        )
        self.assertEqual(snapshot["previous_rsi14"], 68.0)
        self.assertEqual(snapshot["rsi14"], 69.0)
        self.assertAlmostEqual(snapshot["ema_spread_pct"], 100.0 / 99.9)
        self.assertAlmostEqual(snapshot["price_vwap_distance_pct"], 1.9)
        self.assertAlmostEqual(snapshot["atr_pct_of_price"], 100.0 * 2.0 / 101.9)
        self.assertAlmostEqual(snapshot["price_vwap_distance_atr"], 0.95)
        self.assertEqual(snapshot["bars_since_armed"], 2)
        self.assertEqual(snapshot["state_cycle_id"], 17)
        self.assertEqual(snapshot["config_version"], "v3")
        self.assertEqual(snapshot["cooldown_candles"], 4)
        self.assertEqual(snapshot["market_trend_15m"], "15m bullish")
        self.assertIsNone(snapshot["index_trend"])
        self.assertIn("time_of_day_utc", snapshot)
        self.assertIn("day_of_week_utc", snapshot)
        self.assertIsNotNone(snapshot["relative_volume_20"])

    def test_shadow_diagnostics_are_direction_normalized_and_observational(self):
        candles, indicator_frame = self._frames()
        candle_open = int(candles.iloc[-1]["timestamp"])
        long_snapshot = build_indicator_snapshot(
            indicator_frame, candles, {"side": "LONG"},
            candle_open_ms=candle_open, config_version="v3", cooldown_candles=4,
        )
        short_snapshot = build_indicator_snapshot(
            indicator_frame, candles, {"side": "SHORT"},
            candle_open_ms=candle_open, config_version="v3", cooldown_candles=4,
        )
        long_diag = long_snapshot["shadow_diagnostics"]
        short_diag = short_snapshot["shadow_diagnostics"]
        self.assertEqual(long_diag["mode"], "OBSERVATION_ONLY")
        self.assertAlmostEqual(long_diag["extension_in_signal_direction_atr"], 0.95)
        self.assertAlmostEqual(short_diag["extension_in_signal_direction_atr"], -0.95)
        self.assertAlmostEqual(
            long_diag["rsi_change_in_signal_direction_3bars"], 3.0
        )
        self.assertAlmostEqual(
            short_diag["rsi_change_in_signal_direction_3bars"], -3.0
        )
        self.assertAlmostEqual(
            long_diag["di_spread_in_signal_direction"],
            -short_diag["di_spread_in_signal_direction"],
        )
        self.assertIsNotNone(long_diag["ema9_slope_in_signal_direction_atr_3bars"])
        self.assertIsNotNone(long_diag["macd_histogram_change_in_signal_direction_3bars"])
        # The shadow payload is research metadata; the signal's side is unchanged.
        self.assertEqual(long_snapshot["shadow_diagnostics"]["direction"], "LONG")
        self.assertEqual(short_snapshot["shadow_diagnostics"]["direction"], "SHORT")

    def test_entry_quality_shadow_metrics_are_direction_normalized_and_non_blocking(self):
        candles, indicator_frame = self._frames()
        candle_open = int(candles.iloc[-1]["timestamp"])
        long_snapshot = build_indicator_snapshot(
            indicator_frame, candles, {"side": "LONG"},
            candle_open_ms=candle_open, config_version="v3", cooldown_candles=4,
        )
        short_snapshot = build_indicator_snapshot(
            indicator_frame, candles, {"side": "SHORT"},
            candle_open_ms=candle_open, config_version="v3", cooldown_candles=4,
        )
        long_quality = long_snapshot["entry_quality_shadow"]
        short_quality = short_snapshot["entry_quality_shadow"]
        self.assertEqual(long_quality["mode"], "OBSERVATION_ONLY")
        self.assertGreater(long_quality["directional_body_ratio"], 0)
        self.assertLess(short_quality["directional_body_ratio"], 0)
        self.assertAlmostEqual(
            long_quality["directional_close_location"],
            1.0 - short_quality["directional_close_location"],
        )
        self.assertEqual(long_quality["di_spread_aligned"], True)
        self.assertEqual(short_quality["di_spread_aligned"], False)
        # The new research measurements are metadata only; signal direction is untouched.
        self.assertEqual(long_snapshot["shadow_diagnostics"]["direction"], "LONG")
        self.assertEqual(short_snapshot["shadow_diagnostics"]["direction"], "SHORT")

    def test_zero_range_candle_keeps_entry_quality_ratios_null(self):
        candles, indicator_frame = self._frames()
        for frame in (candles, indicator_frame):
            frame.loc[frame.index[-1], ["open", "high", "low", "close"]] = 100.0
        snapshot = build_indicator_snapshot(
            indicator_frame, candles, {"side": "LONG"},
            candle_open_ms=int(candles.iloc[-1]["timestamp"]),
            config_version="v3", cooldown_candles=4,
        )
        quality = snapshot["entry_quality_shadow"]
        self.assertIsNone(quality["body_ratio"])
        self.assertIsNone(quality["directional_close_location"])
        self.assertIsNone(quality["opposing_wick_ratio"])

    def test_short_history_keeps_shadow_metrics_null_without_blocking_snapshot(self):
        candles, indicator_frame = self._frames()
        short_frame = indicator_frame.tail(2).copy()
        snapshot = build_indicator_snapshot(
            short_frame, candles.tail(2), {"side": "SHORT"},
            candle_open_ms=int(candles.iloc[-1]["timestamp"]),
            config_version="v3", cooldown_candles=4,
        )
        self.assertIsNone(snapshot["shadow_diagnostics"]["rsi_change_in_signal_direction_3bars"])
        self.assertIsNone(snapshot["shadow_diagnostics"]["ema9_slope_in_signal_direction_atr_3bars"])


if __name__ == "__main__":
    unittest.main()
