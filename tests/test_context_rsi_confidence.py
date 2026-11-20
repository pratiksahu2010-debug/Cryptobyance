"""Tests for optional higher-timeframe RSI confidence scoring."""
import unittest

from strategy import context_rsi_is_aligned


class ContextRsiConfidenceTests(unittest.TestCase):
    def test_long_is_supported_at_or_above_midline(self):
        self.assertTrue(context_rsi_is_aligned("LONG", 50.0))
        self.assertTrue(context_rsi_is_aligned("LONG", 61.0))

    def test_long_below_midline_is_not_supported_but_is_not_a_veto(self):
        self.assertFalse(context_rsi_is_aligned("LONG", 49.9))

    def test_short_is_supported_at_or_below_midline(self):
        self.assertTrue(context_rsi_is_aligned("SHORT", 50.0))
        self.assertTrue(context_rsi_is_aligned("SHORT", 38.0))

    def test_short_above_midline_is_not_supported_but_is_not_a_veto(self):
        self.assertFalse(context_rsi_is_aligned("SHORT", 50.1))

    def test_custom_midline_is_respected(self):
        self.assertTrue(context_rsi_is_aligned("LONG", 55.0, 55.0))
        self.assertFalse(context_rsi_is_aligned("SHORT", 56.0, 55.0))

    def test_invalid_side_and_rsi_are_rejected(self):
        with self.assertRaises(ValueError):
            context_rsi_is_aligned("BUY", 60.0)
        with self.assertRaises(ValueError):
            context_rsi_is_aligned("LONG", float("nan"))
        with self.assertRaises(ValueError):
            context_rsi_is_aligned("SHORT", 101.0)


if __name__ == "__main__":
    unittest.main()
