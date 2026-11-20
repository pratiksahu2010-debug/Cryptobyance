import unittest

from outcomes import advance_outcome


def base_outcome(**overrides):
    row = {
        "direction": "LONG",
        "signal_candle_open_ms": 1_800_000,
        "entry": 100.0,
        "stop": 98.0,
        "target": 104.0,
        "target1": 102.0,
        "target2": 104.0,
        "atr_at_entry": 4.0,
        "risk_per_unit": 2.0,
        "outcome_horizon_bars": 12,
        "cost_pct": 0.10,
        "best_favorable_r": 0.0,
        "worst_adverse_r": 0.0,
        "best_favorable_pct": 0.0,
        "worst_adverse_pct": 0.0,
        "best_favorable_atr": None,
        "worst_adverse_atr": None,
        "bars_observed": 0,
        "last_observed_candle_open_ms": 1_800_000,
        "data_gap": False,
        "best_favorable_candle_open_ms": None,
        "worst_adverse_candle_open_ms": None,
    }
    row.update(overrides)
    return row


class OutcomeLabelerTests(unittest.TestCase):
    def test_tp1_resolution_records_percent_atr_and_net_r(self):
        progress = advance_outcome(base_outcome(), [{
            "timestamp": 2_100_000, "high": 103.0, "low": 99.0, "close": 102.5,
        }])
        self.assertEqual(progress["status"], "TARGET")
        self.assertEqual(progress["first_hit"], "TP1")
        self.assertEqual(progress["result"], "WIN")
        self.assertAlmostEqual(progress["realized_r"], 0.95)
        self.assertAlmostEqual(progress["best_favorable_r"], 1.5)
        self.assertAlmostEqual(progress["worst_adverse_r"], 0.5)
        self.assertAlmostEqual(progress["best_favorable_pct"], 3.0)
        self.assertAlmostEqual(progress["worst_adverse_pct"], 1.0)
        self.assertAlmostEqual(progress["best_favorable_atr"], 0.75)
        self.assertAlmostEqual(progress["worst_adverse_atr"], 0.25)
        self.assertEqual(progress["bars_to_resolution"], 1)

    def test_same_bar_stop_and_target_is_conservative_loss(self):
        progress = advance_outcome(base_outcome(), [{
            "timestamp": 2_100_000, "high": 105.0, "low": 97.0, "close": 101.0,
        }])
        self.assertEqual(progress["status"], "STOP_LOSS")
        self.assertEqual(progress["first_hit"], "SL")
        self.assertEqual(progress["result"], "LOSS")
        self.assertAlmostEqual(progress["realized_r"], -1.05)

    def test_timeout_after_configured_number_of_forward_bars(self):
        progress = advance_outcome(
            base_outcome(outcome_horizon_bars=2),
            [
                {"timestamp": 2_100_000, "high": 101.0, "low": 99.0, "close": 100.5},
                {"timestamp": 2_400_000, "high": 101.0, "low": 99.0, "close": 101.0},
            ],
        )
        self.assertEqual(progress["status"], "TIMEOUT")
        self.assertEqual(progress["first_hit"], "TIMEOUT")
        self.assertEqual(progress["result"], "TIMEOUT")
        self.assertEqual(progress["bars_to_resolution"], 2)
        self.assertAlmostEqual(progress["realized_r"], 0.45)

    def test_short_side_uses_inverse_excursion_and_target_rules(self):
        row = base_outcome(
            direction="SHORT", stop=102.0, target=96.0,
            target1=98.0, target2=96.0,
        )
        progress = advance_outcome(row, [{
            "timestamp": 2_100_000, "high": 101.0, "low": 97.0, "close": 97.5,
        }])
        self.assertEqual(progress["first_hit"], "TP1")
        self.assertEqual(progress["result"], "WIN")
        self.assertAlmostEqual(progress["best_favorable_r"], 1.5)
        self.assertAlmostEqual(progress["worst_adverse_r"], 0.5)

    def test_missing_interval_is_ambiguous_and_not_a_timeout(self):
        progress = advance_outcome(base_outcome(), [{
            "timestamp": 2_400_000, "high": 105.0, "low": 97.0, "close": 100.0,
        }])
        self.assertEqual(progress["status"], "AMBIGUOUS")
        self.assertEqual(progress["first_hit"], "AMBIGUOUS")
        self.assertIsNone(progress["result"])
        self.assertTrue(progress["data_gap"])

    def test_duplicate_or_older_candles_do_not_change_outcome(self):
        self.assertIsNone(advance_outcome(base_outcome(), [{
            "timestamp": 1_800_000, "high": 105.0, "low": 97.0, "close": 100.0,
        }]))


if __name__ == "__main__":
    unittest.main()
