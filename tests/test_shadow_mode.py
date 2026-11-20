import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import main
import strategy


class FakeShadowStore:
    def __init__(self, trigger_side="LONG"):
        self.trigger_side = trigger_side
        self.advanced = []
        self.recorded = []

    async def load_symbol_states(self, strategy_key, symbol):
        self.strategy_key = strategy_key
        return {}

    async def advance_symbol_state(self, **kwargs):
        self.advanced.append(kwargs)
        return "TRIGGER" if kwargs["direction"] == self.trigger_side else "IDLE"

    async def record_shadow_signal(self, proposal_id, version, signal):
        self.recorded.append((proposal_id, version, signal))
        return True


class FakeShadowJournal:
    def __init__(self):
        self.expired = 0

    async def expire_shadow_if_needed(self):
        self.expired += 1
        return True


class ShadowEvaluationTests(unittest.IsolatedAsyncioTestCase):
    async def test_shadow_signal_uses_proposal_config_and_never_queues_live_alert(self):
        old_shadow = main.shadow_runtime_config
        old_adx = strategy.ADX_MIN
        expiry = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        main.shadow_runtime_config = {
            "proposal_id": 77, "version": "v77",
            "shadow_until": expiry, "config": {"adx_min": 27.0},
        }
        store = FakeShadowStore()
        journal = FakeShadowJournal()
        seen = {}

        def fake_inputs(df5, df15, armed_sides=None):
            seen["adx"] = strategy.ADX_MIN
            return {
                "LONG": {"setup_valid": True, "invalidation": False, "trigger": True},
                "SHORT": {"setup_valid": False, "invalidation": False, "trigger": False},
            }

        try:
            with patch("main.evaluate_state_inputs", side_effect=fake_inputs), \
                 patch("main.evaluate_setup", return_value={
                     "side": "LONG", "entry": 100.0, "stop": 98.0,
                     "target": 104.0, "target1": 102.0, "target2": 104.0,
                 }):
                await main.process_shadow_symbol(
                    store, journal, "BTC/USDT", object(), object(), 1_800_000
                )
            self.assertEqual(seen["adx"], 27.0)
            self.assertEqual(strategy.ADX_MIN, old_adx)
            self.assertEqual(store.strategy_key, "shadow:v77")
            self.assertEqual(len(store.advanced), 2)
            self.assertEqual(len(store.recorded), 1)
            proposal_id, version, signal = store.recorded[0]
            self.assertEqual(proposal_id, 77)
            self.assertEqual(version, "v77")
            self.assertTrue(signal["shadow"])
            self.assertEqual(signal["config_version"], "v77")
            self.assertFalse(hasattr(store, "enqueue_signal"))
        finally:
            main.shadow_runtime_config = old_shadow

    async def test_expired_shadow_is_stopped_without_evaluating_signals(self):
        old_shadow = main.shadow_runtime_config
        main.shadow_runtime_config = {
            "proposal_id": 78, "version": "v78",
            "shadow_until": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
            "config": {"adx_min": 25.0},
        }
        store = FakeShadowStore()
        journal = FakeShadowJournal()
        try:
            with patch("main.evaluate_state_inputs") as evaluator:
                await main.process_shadow_symbol(
                    store, journal, "BTC/USDT", object(), object(), 1_800_000
                )
            evaluator.assert_not_called()
            self.assertEqual(journal.expired, 1)
            self.assertIsNone(main.shadow_runtime_config)
            self.assertEqual(store.advanced, [])
        finally:
            main.shadow_runtime_config = old_shadow


if __name__ == "__main__":
    unittest.main()
