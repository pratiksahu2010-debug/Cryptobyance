import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from candidate_store import CandidateStore
from journal import SignalJournal


class SQLitePersistenceIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "local-bot.db")
        self.store = await CandidateStore.connect("sqlite:///" + self.db_path)
        self.journal = SignalJournal(self.store)
        await self.journal.initialize()
        await self.journal.ensure_initial_config(
            "v2", {"adx_min": 20.0, "rsi_long_min": 52.0}
        )

    async def asyncTearDown(self):
        await self.store.close()
        self.temp_dir.cleanup()

    async def test_local_sqlite_persists_state_and_blocks_duplicate_candle(self):
        armed = await self.store.advance_symbol_state(
            strategy_key="local-test", symbol="BTC/USDT", direction="LONG",
            candle_open_ms=1_800_000, setup_valid=True, invalidation=False,
            trigger=False, cooldown_candles=4,
        )
        duplicate = await self.store.advance_symbol_state(
            strategy_key="local-test", symbol="BTC/USDT", direction="LONG",
            candle_open_ms=1_800_000, setup_valid=True, invalidation=False,
            trigger=True, cooldown_candles=4,
        )
        states = await self.store.load_symbol_states("local-test", "BTC/USDT")
        self.assertEqual(armed, "ARMED")
        self.assertEqual(duplicate, "DUPLICATE")
        self.assertEqual(states["LONG"], "ARMED")

    async def test_signal_outbox_delivery_outcomes_and_analytics_work_on_sqlite(self):
        await self.store.advance_symbol_state(
            strategy_key="coindcx_per_symbol_v2", symbol="BTC/USDT", direction="LONG",
            candle_open_ms=1_800_000, setup_valid=True, invalidation=False,
            trigger=False, cooldown_candles=4,
        )
        signal = {
            "symbol": "BTC/USDT", "side": "LONG", "candle": 2_100_000,
            "entry": 100.0, "stop": 98.0, "target": 104.0,
            "target1": 102.0, "target2": 104.0, "atr": 1.5,
            "risk_distance": 2.0,
        }
        self.assertTrue(await self.store.enqueue_signal(
            signal, None, 4, strategy_key="coindcx_per_symbol_v2"
        ))
        claimed = await self.store.claim_outbox()
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["payload"]["symbol"], "BTC/USDT")
        await self.store.finish_outbox(claimed["id"])

        summary = await self.store.performance_summary()
        self.assertEqual(summary["totals"]["delivered_signals"], 1)
        self.assertEqual(summary["totals"]["tracked_outcomes"], 1)
        self.assertEqual(summary["totals"]["open"], 1)

        feedback_id = await self.journal.add_feedback(
            1, "GOOD_ENTRY", "SQLite integration feedback"
        )
        self.assertGreater(feedback_id, 0)
        stats = await self.journal.stats(30)
        self.assertEqual(stats["totals"]["delivered"], 1)
        self.assertEqual(stats["totals"]["manual_labels"], 1)

        daily = await self.journal.daily_training_review()
        self.assertEqual(daily["signals"]["delivered"], 1)
        learning = await self.journal.learning_summary()
        self.assertEqual(learning["delivered_signal_totals"]["delivered_signals"], 1)

    async def test_weekly_digest_is_persisted_idempotently_and_retried(self):
        window_end = datetime(2026, 11, 16, tzinfo=timezone.utc)
        window_start = window_end - timedelta(days=7)
        summary = await self.journal.build_weekly_digest(window_start, window_end)
        self.assertEqual(summary["resolved_sample"], 0)
        self.assertFalse(summary["live_strategy_changed"])
        digest_id = await self.journal.save_weekly_digest(summary, window_start, window_end)
        duplicate_id = await self.journal.save_weekly_digest(summary, window_start, window_end)
        self.assertEqual(digest_id, duplicate_id)
        pending = await self.journal.pending_weekly_digests()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["id"], digest_id)
        await self.journal.mark_weekly_digest_attempt(digest_id)
        self.assertEqual((await self.journal.pending_weekly_digests())[0]["attempts"], 1)
        await self.journal.mark_weekly_digest_sent(digest_id)
        self.assertEqual(await self.journal.pending_weekly_digests(), [])

    async def test_sqlite_config_shadow_history_lifecycle(self):
        active = await self.journal.active_config()
        self.assertEqual(active["version"], "v2")
        history = await self.journal.config_history()
        self.assertEqual(history["versions"][0]["status"], "ACTIVE")
        self.assertIn("shadow_until", history["versions"][0])


if __name__ == "__main__":
    unittest.main()
