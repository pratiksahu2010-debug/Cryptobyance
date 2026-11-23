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
        "status": "OPEN",
        "first_hit": None,
        "result": None,
        "exit_candle_open_ms": None,
        "exit_price": None,
        "realized_r": None,
        "tp1_hit_candle_open_ms": None,
        "tp2_hit_candle_open_ms": None,
        "tp2_tracking_status": "NOT_STARTED",
        "tp2_realized_r": None,
        "tp2_bars_observed": 0,
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

    def test_tp1_then_later_tp2_is_tracked_without_overwriting_tp1_result(self):
        first = advance_outcome(base_outcome(), [{
            "timestamp": 2_100_000, "high": 103.0, "low": 99.0, "close": 102.5,
        }])
        self.assertEqual(first["status"], "TARGET")
        self.assertEqual(first["first_hit"], "TP1")
        self.assertEqual(first["tp2_tracking_status"], "OPEN")

        after_tp1 = base_outcome(
            status=first["status"], first_hit=first["first_hit"], result=first["result"],
            bars_observed=first["bars_observed"],
            last_observed_candle_open_ms=first["last_observed_candle_open_ms"],
            exit_candle_open_ms=first["exit_candle_open_ms"],
            exit_price=first["exit_price"], realized_r=first["realized_r"],
            tp1_hit_candle_open_ms=first["tp1_hit_candle_open_ms"],
            tp2_tracking_status=first["tp2_tracking_status"],
            best_favorable_r=first["best_favorable_r"],
            worst_adverse_r=first["worst_adverse_r"],
            best_favorable_pct=first["best_favorable_pct"],
            worst_adverse_pct=first["worst_adverse_pct"],
            best_favorable_atr=first["best_favorable_atr"],
            worst_adverse_atr=first["worst_adverse_atr"],
            best_favorable_candle_open_ms=first["best_favorable_candle_open_ms"],
            worst_adverse_candle_open_ms=first["worst_adverse_candle_open_ms"],
        )
        second = advance_outcome(after_tp1, [{
            "timestamp": 2_400_000, "high": 105.0, "low": 99.0, "close": 104.5,
        }])
        self.assertEqual(second["status"], "TARGET")
        self.assertEqual(second["first_hit"], "TP1")
        self.assertAlmostEqual(second["realized_r"], 0.95)
        self.assertEqual(second["tp2_tracking_status"], "HIT")
        self.assertEqual(second["tp2_hit_candle_open_ms"], 2_400_000)
        self.assertAlmostEqual(second["tp2_realized_r"], 1.95)

    def test_tp1_then_stop_before_tp2_closes_continuation_only(self):
        after_tp1 = base_outcome(
            status="TARGET", first_hit="TP1", result="WIN",
            bars_observed=1, last_observed_candle_open_ms=2_100_000,
            exit_candle_open_ms=2_100_000, exit_price=102.0, realized_r=0.95,
            tp1_hit_candle_open_ms=2_100_000, tp2_tracking_status="OPEN",
        )
        progress = advance_outcome(after_tp1, [{
            "timestamp": 2_400_000, "high": 101.0, "low": 97.0, "close": 98.0,
        }])
        self.assertEqual(progress["status"], "TARGET")
        self.assertEqual(progress["first_hit"], "TP1")
        self.assertAlmostEqual(progress["realized_r"], 0.95)
        self.assertEqual(progress["tp2_tracking_status"], "STOPPED")
        self.assertIsNone(progress["tp2_hit_candle_open_ms"])

    def test_tp1_on_final_horizon_bar_does_not_extend_tp2_window(self):
        row = base_outcome(outcome_horizon_bars=1)
        progress = advance_outcome(row, [{
            "timestamp": 2_100_000, "high": 103.0, "low": 99.0, "close": 102.5,
        }])
        self.assertEqual(progress["first_hit"], "TP1")
        self.assertEqual(progress["tp2_tracking_status"], "TIMEOUT")
        self.assertIsNone(progress["tp2_hit_candle_open_ms"])

    def test_tp1_and_tp2_in_same_candle_record_both_milestones(self):
        progress = advance_outcome(base_outcome(), [{
            "timestamp": 2_100_000, "high": 105.0, "low": 99.0, "close": 104.0,
        }])
        self.assertEqual(progress["first_hit"], "TP1")
        self.assertEqual(progress["tp2_tracking_status"], "HIT")
        self.assertEqual(progress["tp1_hit_candle_open_ms"], 2_100_000)
        self.assertEqual(progress["tp2_hit_candle_open_ms"], 2_100_000)

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
