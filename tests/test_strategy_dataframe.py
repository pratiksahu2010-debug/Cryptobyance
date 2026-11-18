import warnings
import unittest

import pandas as pd

from strategy import candles_to_frame


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


if __name__ == "__main__":
    unittest.main()
