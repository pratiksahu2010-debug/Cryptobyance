import unittest
from unittest.mock import patch

import main


class HealthOutcomeDiagnosticsTests(unittest.TestCase):
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
