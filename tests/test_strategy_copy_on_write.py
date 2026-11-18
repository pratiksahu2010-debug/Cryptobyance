import warnings
import unittest

import numpy as np
import pandas as pd

from strategy import candles_to_frame, indicators


class CandleFrameConversionTests(unittest.TestCase):
    def test_conversion_is_copy_on_write_safe_and_does_not_emit_chained_assignment_warning(self):
        raw = [
            [1_800_000_000_000, "100", "102", "99", "101", "12"],
            [1_800_000_300_000, "bad", "103", "100", "102", "13"],
        ]
        with pd.option_context("mode.copy_on_write", True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                frame = candles_to_frame(raw)

        chained = [
            item for item in caught
            if "ChainedAssignmentError" in str(item.message)
            or "chained assignment" in str(item.message).lower()
        ]
        self.assertEqual(chained, [])
        self.assertEqual(len(frame), 2)
        self.assertTrue(pd.api.types.is_numeric_dtype(frame["timestamp"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(frame["open"]))
        self.assertTrue(pd.isna(frame.loc[1, "open"]))
        self.assertEqual(float(frame.loc[0, "close"]), 101.0)

    def test_indicator_calculation_is_copy_on_write_safe_and_preserves_input(self):
        count = 100
        close = 100 + np.arange(count, dtype=float) * 0.15 + np.sin(np.arange(count) / 4)
        frame = pd.DataFrame({
            "timestamp": 1_800_000_000_000 + np.arange(count) * 300_000,
            "open": close - 0.1,
            "high": close + 0.6,
            "low": close - 0.6,
            "close": close,
            "volume": np.full(count, 100.0),
        })
        original = frame.copy(deep=True)

        with pd.option_context("mode.copy_on_write", True):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = indicators(frame)

        chained = [
            item for item in caught
            if "ChainedAssignmentError" in str(item.message)
            or "chained assignment" in str(item.message).lower()
        ]
        self.assertEqual(chained, [])
        pd.testing.assert_frame_equal(frame, original)
        for column in ("ema9", "ema21", "rsi", "adx", "plus_di", "minus_di", "atr", "vwap"):
            self.assertIn(column, result.columns)
            self.assertEqual(len(result[column]), count)
        self.assertTrue(np.isfinite(result["ema9"].iloc[-1]))
        self.assertTrue(np.isfinite(result["vwap"].iloc[-1]))


if __name__ == "__main__":
    unittest.main()
