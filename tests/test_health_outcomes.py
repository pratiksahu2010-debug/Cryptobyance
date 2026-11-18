import unittest
from unittest.mock import patch

import main


class HealthOutcomeDiagnosticsTests(unittest.TestCase):
    async def test_performance_endpoint_returns_aggregate_snapshot_only(self):
        summary = {
            "totals": {"delivered_signals": 5, "tracked_outcomes": 4},
            "by_symbol_direction": [],
            "outbox": [],
        }
        with patch.dict(main.state, {
            "performance_summary": summary,
            "last_scan": "2026-11-18T03:10:00+00:00",
            "performance_report_errors": 0,
        }):
            payload = await main.performance()

        self.assertTrue(payload["available"])
        self.assertEqual(payload["summary"]["totals"]["delivered_signals"], 5)
        self.assertNotIn("payload", payload)

    def test_health_exposes_outcome_tracker_counters(self):
        with patch.dict(main.state, {
            "outcome_rows_updated": 17,
            "outcome_tracking_errors": 2,
        }):
            payload = main.build_health_payload()

        self.assertEqual(payload["outcome_rows_updated"], 17)
        self.assertEqual(payload["outcome_tracking_errors"], 2)


if __name__ == "__main__":
    unittest.main()
