import unittest

from state_machine import signal_transition_path


class SignalLifecycleTests(unittest.TestCase):
    def test_first_signal_records_complete_lifecycle(self):
        self.assertEqual(
            signal_transition_path("IDLE", 1_800_000, None),
            [("IDLE", "ARMED"), ("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN")],
        )

    def test_armed_setup_transitions_to_triggered_then_cooldown(self):
        self.assertEqual(
            signal_transition_path("ARMED", 1_800_000, None),
            [("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN")],
        )

    def test_active_cooldown_rejects_signal(self):
        self.assertIsNone(signal_transition_path("COOLDOWN", 1_800_000, 2_100_000))

    def test_expired_cooldown_returns_to_idle_before_rearming(self):
        self.assertEqual(
            signal_transition_path("COOLDOWN", 2_400_000, 2_100_000),
            [("COOLDOWN", "IDLE"), ("IDLE", "ARMED"),
             ("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN")],
        )

    def test_cooldown_expires_at_existing_configured_bar_boundary(self):
        self.assertEqual(
            signal_transition_path("COOLDOWN", 2_700_000, 2_700_000),
            [("COOLDOWN", "IDLE"), ("IDLE", "ARMED"),
             ("ARMED", "TRIGGERED"), ("TRIGGERED", "COOLDOWN")],
        )

    def test_invalid_state_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown strategy state"):
            signal_transition_path("BROKEN", 1_800_000, None)


if __name__ == "__main__":
    unittest.main()
