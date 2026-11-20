import unittest
from unittest.mock import patch

import main


class HealthOutcomeDiagnosticsTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_readiness_returns_service_unavailable_until_scanner_is_healthy(self):
        with patch("main.build_health_payload", return_value={"ok": False, "scanner_running": False}):
            response = await main.readiness()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.body, b'{"ok":false,"scanner_running":false}')

    async def test_readiness_returns_success_when_scanner_is_healthy(self):
        with patch("main.build_health_payload", return_value={"ok": True, "scanner_running": True}):
            response = await main.readiness()
        self.assertEqual(response.status_code, 200)

    def test_health_requires_database_telegram_and_a_fresh_complete_scan(self):
        import time

        ready_state = {
            "running": True, "market_metadata_ready": True,
            "database_connected": True, "telegram_configured": True,
            "active_symbols": 2, "last_scan_epoch": time.time(),
            "last_cycle_symbols_ok": 2, "last_cycle_symbols_failed": 0,
        }
        with patch.dict(main.state, ready_state):
            self.assertTrue(main.build_health_payload()["ok"])

        with patch.dict(main.state, {**ready_state, "database_connected": False}):
            self.assertFalse(main.build_health_payload()["ok"])
        with patch.dict(main.state, {**ready_state, "telegram_configured": False}):
            self.assertFalse(main.build_health_payload()["ok"])
        with patch.dict(main.state, {**ready_state, "last_cycle_symbols_failed": 1}):
            self.assertFalse(main.build_health_payload()["ok"])

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
