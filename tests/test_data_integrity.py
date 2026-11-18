import unittest

import pandas as pd

from strategy import has_contiguous_tail


class ContiguousIndicatorWindowTests(unittest.TestCase):
    def test_accepts_exactly_contiguous_five_minute_tail(self):
        frame = pd.DataFrame({"timestamp": [i * 300_000 for i in range(60)]})
        self.assertTrue(has_contiguous_tail(frame, "5m", 60))

    def test_rejects_missing_candle_inside_indicator_window(self):
        stamps = [i * 300_000 for i in range(60)]
        stamps[30] += 300_000
        frame = pd.DataFrame({"timestamp": stamps})
        self.assertFalse(has_contiguous_tail(frame, "5m", 60))

    def test_ignores_gaps_older_than_required_indicator_tail(self):
        stamps = [i * 300_000 for i in range(61)]
        stamps[0] += 300_000
        frame = pd.DataFrame({"timestamp": stamps})
        self.assertTrue(has_contiguous_tail(frame, "5m", 60))

    def test_requires_full_fifteen_minute_context_window(self):
        frame = pd.DataFrame({"timestamp": [i * 900_000 for i in range(34)]})
        self.assertFalse(has_contiguous_tail(frame, "15m", 35))
        frame = pd.DataFrame({"timestamp": [i * 900_000 for i in range(35)]})
        self.assertTrue(has_contiguous_tail(frame, "15m", 35))


if __name__ == "__main__":
    unittest.main()
