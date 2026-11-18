import unittest

from training_rules import parse_instruction


class TrainingRuleParserTests(unittest.TestCase):
    def test_extracts_direction_timing_indicators_and_rvol(self):
        parsed = parse_instruction(
            "For BTCUSDT LONG, prefer the first retest after MACD turns positive "
            "with relative volume above 1.5 between 09:00 and 12:00."
        )
        self.assertEqual(parsed["category"], "PREFER_ENTRY")
        self.assertEqual(parsed["direction"], "LONG")
        self.assertEqual(parsed["symbols"], ["BTC"])
        self.assertEqual(parsed["time_window"], {
            "start_local": "09:00", "end_local": "12:00", "timezone": "Asia/Kolkata"
        })
        self.assertEqual(parsed["min_relative_volume"], 1.5)
        self.assertIn("macd", parsed["indicators_mentioned"])
        self.assertIn("first retest", parsed["entry_terms"])

    def test_identifies_avoid_setup_and_short_direction(self):
        parsed = parse_instruction("Avoid SHORT entries when RSI is weak.")
        self.assertEqual(parsed["category"], "AVOID_SETUP")
        self.assertEqual(parsed["direction"], "SHORT")

    def test_rejects_empty_and_oversized_instructions(self):
        for value in ("", " "):
            with self.assertRaises(ValueError):
                parse_instruction(value)
        with self.assertRaises(ValueError):
            parse_instruction("x" * 1501)

    def test_unknown_conditions_are_preserved_for_human_review(self):
        parsed = parse_instruction("Only trade the special candle pattern I described.")
        self.assertEqual(parsed["category"], "GENERAL")
        self.assertTrue(parsed["requires_human_review"])


if __name__ == "__main__":
    unittest.main()
