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

        note_id = await self.journal.add_feedback(
            1, "NEEDS_REVIEW", "Telegram reply note", "telegram"
        )
        self.assertGreater(note_id, 0)
        pending_before_grade = await self.journal.pending_entries()
        self.assertEqual([row["id"] for row in pending_before_grade], [1])
        exported = await self.journal.entries(10)
        self.assertEqual(exported[0]["feedback"][0]["comment"], "Telegram reply note")

        feedback_id = await self.journal.add_feedback(
            1, "GOOD_ENTRY", "SQLite integration feedback", "telegram"
        )
        self.assertGreater(feedback_id, 0)
        self.assertEqual(await self.journal.pending_entries(), [])
        stats = await self.journal.stats(30)
        self.assertEqual(stats["totals"]["delivered"], 1)
        self.assertEqual(stats["totals"]["manual_labels"], 1)

        daily = await self.journal.daily_training_review()
        self.assertEqual(daily["signals"]["delivered"], 1)
        learning = await self.journal.learning_summary()
        self.assertEqual(learning["delivered_signal_totals"]["delivered_signals"], 1)

    async def test_retention_prunes_telemetry_but_preserves_signal_audit(self):
        old = "2000-01-01 00:00:00"
        async with self.store.pool.acquire() as conn:
            async with conn.transaction():
                signal = await conn.fetchrow(
                    """INSERT INTO signal_history(
                         idempotency_key,symbol,direction,candle_open_ms,payload,status,
                         created_at,delivered_at)
                       VALUES('retention-test','BTC/USDT','LONG',1800000,'{}','SENT',$1,$1)
                       RETURNING id""", old)
                signal_id = int(signal["id"])
                await conn.execute(
                    """INSERT INTO signal_outcomes(
                         signal_id,symbol,direction,signal_candle_open_ms,entry,stop,target,
                         risk_per_unit,last_observed_candle_open_ms,created_at,updated_at)
                       VALUES($1,'BTC/USDT','LONG',1800000,100,98,104,2,1800000,$2,$2)""",
                    signal_id, old)
                await conn.execute(
                    """INSERT INTO journal_feedback(signal_id,category,comment,source,created_at)
                       VALUES($1,'GOOD_ENTRY','keep this feedback','telegram',$2)""",
                    signal_id, old)
                await conn.execute(
                    """INSERT INTO telegram_outbox(
                         signal_id,idempotency_key,payload,status,created_at,sent_at)
                       VALUES($1,'retention-outbox','{}','SENT',$2,$2)""",
                    signal_id, old)
                await conn.execute(
                    """INSERT INTO strategy_state_events(
                         strategy_key,symbol,direction,from_state,to_state,candle_open_ms,created_at)
                       VALUES('retention','BTC/USDT','LONG','IDLE','ARMED',1800000,$1)""", old)
                await conn.execute(
                    """INSERT INTO shadow_signal_history(
                         proposal_version,proposal_id,symbol,direction,candle_open_ms,payload,created_at)
                       VALUES('v-retention',99,'BTC/USDT','LONG',1800000,'{}',$1)""", old)
                await conn.execute(
                    """INSERT INTO training_daily_reviews(window_start,window_end,summary,created_at)
                       VALUES($1,$2,'{}',$1)""", old, "2000-01-02 00:00:00")
                await conn.execute(
                    """INSERT INTO training_weekly_digests(
                         window_start,window_end,summary,status,created_at,sent_at)
                       VALUES($1,$2,'{}','SENT',$1,$1)""", old, "2000-01-08 00:00:00")
                await conn.execute(
                    """INSERT INTO scanner_runtime(instance_key,status,updated_at)
                       VALUES('old-instance','{}',$1)""", old)

        result = await self.store.cleanup(now_ms=1_800_000, retention_days=365)
        self.assertEqual(result["deleted"], 6)
        self.assertEqual(result["retention_deleted"]["telegram_outbox"], 1)
        async with self.store.pool.acquire() as conn:
            for table in (
                "strategy_state_events", "shadow_signal_history", "training_daily_reviews",
                "training_weekly_digests", "telegram_outbox", "scanner_runtime",
            ):
                row = await conn.fetchrow(f"SELECT COUNT(*) AS n FROM {table}")
                self.assertEqual(row["n"], 0, table)
            for table in ("signal_history", "signal_outcomes", "journal_feedback", "config_history"):
                row = await conn.fetchrow(f"SELECT COUNT(*) AS n FROM {table}")
                self.assertEqual(row["n"], 1, table)

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
