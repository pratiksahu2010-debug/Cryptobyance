import unittest

from state_machine import advance_state


class SignalLifecycleTests(unittest.TestCase):
    def test_idle_arms_but_does_not_trigger_on_same_candle(self):
        decision = advance_state("IDLE", setup_valid=True, invalidation=False, trigger=True)
        self.assertEqual((decision.state, decision.action), ("ARMED", "ARMED"))

    def test_idle_without_setup_stays_idle(self):
        self.assertEqual(
            advance_state("IDLE", setup_valid=False, invalidation=False, trigger=True).action,
            "NONE",
        )

    def test_armed_triggers_only_on_later_valid_candle(self):
        decision = advance_state("ARMED", setup_valid=True, invalidation=False, trigger=True)
        self.assertEqual((decision.state, decision.action), ("ARMED", "TRIGGER"))

    def test_armed_invalidates_to_idle(self):
        decision = advance_state("ARMED", setup_valid=False, invalidation=True, trigger=True)
        self.assertEqual((decision.state, decision.action), ("IDLE", "INVALIDATED"))
        self.assertTrue(decision.invalidated)

    def test_armed_without_trigger_remains_armed(self):
        self.assertEqual(
            advance_state("ARMED", setup_valid=True, invalidation=False, trigger=False).state,
            "ARMED",
        )

    def test_armed_setup_is_invalidated_after_a_missed_candle(self):
        decision = advance_state(
            "ARMED", setup_valid=True, invalidation=False, trigger=True,
            missed_candles=True,
        )
        self.assertEqual((decision.state, decision.action), ("IDLE", "INVALIDATED"))
        self.assertTrue(decision.invalidated)

    def test_cooldown_does_not_assume_missed_candles_were_invalidation(self):
        decision = advance_state(
            "COOLDOWN", setup_valid=True, invalidation=False, trigger=False,
            cooldown_remaining=0, invalidated_during_cooldown=False,
            missed_candles=True,
        )
        self.assertEqual(decision.state, "COOLDOWN")
        self.assertFalse(decision.invalidated)

    def test_cooldown_waits_for_bars_and_invalidation(self):
        decision = advance_state(
            "COOLDOWN", setup_valid=True, invalidation=False, trigger=True,
            cooldown_remaining=2, invalidated_during_cooldown=False,
        )
        self.assertEqual(decision.state, "COOLDOWN")

    def test_cooldown_records_invalidation_then_waits_for_expiry(self):
        decision = advance_state(
            "COOLDOWN", setup_valid=False, invalidation=True, trigger=False,
            cooldown_remaining=2, invalidated_during_cooldown=False,
        )
        self.assertEqual(decision.state, "COOLDOWN")
        self.assertTrue(decision.invalidated)

    def test_cooldown_rearms_only_after_expiry_and_invalidation(self):
        decision = advance_state(
            "COOLDOWN", setup_valid=True, invalidation=False, trigger=True,
            cooldown_remaining=0, invalidated_during_cooldown=True,
        )
        self.assertEqual((decision.state, decision.action), ("IDLE", "REARMED"))

    def test_cooldown_does_not_rearm_without_invalidation(self):
        decision = advance_state(
            "COOLDOWN", setup_valid=True, invalidation=False, trigger=True,
            cooldown_remaining=0, invalidated_during_cooldown=False,
        )
        self.assertEqual(decision.state, "COOLDOWN")

    def test_invalid_state_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unknown strategy state"):
            advance_state("BROKEN", setup_valid=False, invalidation=False, trigger=False)


if __name__ == "__main__":
    unittest.main()
