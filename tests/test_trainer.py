import unittest

from trainer import build_training_proposal, prepare_samples


CURRENT = {
    "adx_min": 20,
    "rsi_long_min": 52,
    "rsi_long_max": 70,
    "rsi_short_min": 30,
    "rsi_short_max": 42,
}


def sample(index, *, adx=25.0, realized_r=1.0, manual_label=None):
    direction = "LONG" if index % 2 == 0 else "SHORT"
    features = {
        "adx14": adx,
        "rsi14": 60.0 if direction == "LONG" else 35.0,
        "ema_spread_pct": 0.2,
        "price_vwap_distance_pct": 0.1,
        "relative_volume_20": 1.6,
        "cooldown_candles": 4,
    }
    return {
        "signal_id": index + 1,
        "candle_open_ms": 1_800_000 + index * 300_000,
        "direction": direction,
        "features": features,
        "auto_result": "WIN" if realized_r > 0 else "LOSS",
        "realized_r": realized_r,
        "manual_label": manual_label,
    }


class OfflineTrainerTests(unittest.TestCase):
    def test_refuses_to_propose_below_minimum_sample_count(self):
        result = build_training_proposal(
            [sample(i) for i in range(99)], CURRENT,
            min_samples=100, min_bucket_samples=30,
        )
        self.assertEqual(result["status"], "INSUFFICIENT_DATA")
        self.assertIn("Need at least 100", result["reason"])

    def test_manual_label_overrides_auto_result_and_conflict_is_counted(self):
        rows = [sample(i) for i in range(100)]
        rows[0]["auto_result"] = "WIN"
        rows[0]["manual_label"] = "FAKE_SIGNAL"
        prepared, conflicts = prepare_samples(rows)
        self.assertEqual(conflicts, 1)
        self.assertEqual(prepared[0]["effective_result"], "LOSS")

    def test_proposal_requires_stable_out_of_sample_improvement(self):
        rows = []
        for i in range(300):
            good = i % 2 == 0
            rows.append(sample(i, adx=25.0 if good else 20.0,
                               realized_r=1.0 if good else -0.5))
        result = build_training_proposal(
            rows, CURRENT, base_config_version="v2",
            min_samples=100, min_bucket_samples=30,
        )
        self.assertEqual(result["status"], "PROPOSED")
        self.assertEqual(result["proposed_config_version"], "v3")
        self.assertIn("adx_min", result["changes"])
        self.assertGreaterEqual(result["proposed_config"]["adx_min"], CURRENT["adx_min"])
        self.assertEqual(len(result["expected_effect"]["walk_forward_folds"]), 2)
        self.assertGreater(result["expected_effect"]["baseline_resolved_signals_per_day"], 0)
        self.assertGreaterEqual(result["expected_effect"]["candidate_resolved_signals_per_day"], 0)
        self.assertIn("not a forecast", result["expected_effect"]["signal_rate_basis"])
        self.assertTrue(all(
            fold["expectancy_improvement_r"] >= 0.02
            for fold in result["expected_effect"]["walk_forward_folds"]
        ))

    def test_no_edge_does_not_produce_a_proposal(self):
        rows = [sample(i, adx=25.0, realized_r=0.25) for i in range(300)]
        result = build_training_proposal(rows, CURRENT)
        self.assertEqual(result["status"], "NO_OOS_IMPROVEMENT")
        self.assertIn("out-of-sample", result["reason"])

    def test_missing_observational_features_do_not_crash_bucket_analysis(self):
        rows = [sample(i) for i in range(100)]
        for row in rows:
            row["features"].pop("close", None)
            row["features"].pop("vwap", None)
            row["features"].pop("price_vwap_distance_pct", None)
        result = build_training_proposal(rows, CURRENT, min_bucket_samples=30)
        self.assertIn("vwap_distance_pct", result["summary"]["bucket_analysis"])
        self.assertIn("cooldown_candles", result["summary"]["bucket_analysis"])
        self.assertIn("rsi_long_band", result["summary"]["bucket_analysis"])
        self.assertIn("rsi_short_band", result["summary"]["bucket_analysis"])
        self.assertEqual(
            result["summary"]["bucket_analysis"]["vwap_distance_pct"]["available_samples"], 0
        )

    def test_analysis_covers_all_configurable_and_observational_parameters(self):
        rows = [sample(i) for i in range(120)]
        result = build_training_proposal(
            rows, CURRENT, min_samples=100, min_bucket_samples=30
        )
        buckets = result["summary"]["bucket_analysis"]
        for parameter in (
            "adx14", "rsi14", "rsi_long_band", "rsi_short_band",
            "cooldown_candles", "ema_spread_pct", "vwap_distance_pct",
            "relative_volume_20", "hour_utc",
        ):
            self.assertIn(parameter, buckets)
        self.assertGreater(buckets["cooldown_candles"]["available_samples"], 0)

    def test_bucket_statistics_are_suppressed_below_minimum(self):
        rows = [sample(i) for i in range(100)]
        result = build_training_proposal(rows, CURRENT, min_bucket_samples=30)
        buckets = result["summary"]["bucket_analysis"]["adx14"]["buckets"]
        self.assertTrue(buckets)
        self.assertTrue(all(
            bucket["metrics"] is None or bucket["sample_count"] >= 30
            for bucket in buckets
        ))


if __name__ == "__main__":
    unittest.main()
