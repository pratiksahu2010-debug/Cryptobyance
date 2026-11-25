import unittest

import pandas as pd

from strategy import evaluate_setup, evaluate_state_inputs


def make_candles(count: int, interval_ms: int, *, rising: bool) -> pd.DataFrame:
    closes = [
        100.0 + (index * 0.2 if rising else -index * 0.2)
        for index in range(count)
    ]
    return pd.DataFrame({
        "timestamp": [index * interval_ms for index in range(count)],
        "open": [value - 0.05 for value in closes],
        "high": [value + 0.30 for value in closes],
        "low": [value - 0.30 for value in closes],
        "close": closes,
        "volume": [100.0] * count,
    })


class FifteenMinuteContextMustNotVetoTests(unittest.TestCase):
    def setUp(self):
        # A fully aligned bullish 5m setup alongside an opposing bearish 15m trend.
        self.df5 = make_candles(80, 300_000, rising=True)
        self.opposing_df15 = make_candles(60, 900_000, rising=False)

    def test_opposing_15m_trend_does_not_veto_5m_long_setup(self):
        signal = evaluate_setup(self.df5, self.opposing_df15)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(signal["context"], "15m bearish")

    def test_15m_presence_or_absence_does_not_change_state_inputs(self):
        without_context = evaluate_state_inputs(self.df5, None, armed_sides={"LONG"})
        with_opposing_context = evaluate_state_inputs(
            self.df5, self.opposing_df15, armed_sides={"LONG"}
        )
        self.assertEqual(with_opposing_context, without_context)
        self.assertTrue(with_opposing_context["LONG"]["setup_valid"])
        self.assertFalse(with_opposing_context["LONG"]["invalidation"])

    def test_opposing_15m_trend_does_not_change_emitted_setup_side(self):
        without_context = evaluate_setup(self.df5, None)
        with_opposing_context = evaluate_setup(self.df5, self.opposing_df15)
        self.assertEqual(with_context_side(with_opposing_context), with_context_side(without_context))


def with_context_side(signal):
    return None if signal is None else signal["side"]


if __name__ == "__main__":
    unittest.main()
