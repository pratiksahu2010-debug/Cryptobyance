import json
import unittest
from unittest.mock import AsyncMock

from journal import FEEDBACK_CATEGORIES, JOURNAL_DDL, SignalJournal


class JournalSchemaTests(unittest.TestCase):
    def test_journal_tables_are_additive_and_auditable(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_feedback", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_candidates", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS training_proposals", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS config_history", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS config_history_events", JOURNAL_DDL)
        self.assertIn("uq_config_history_one_shadow", JOURNAL_DDL)
        self.assertIn("'SHADOW'", JOURNAL_DDL)
        self.assertIn("SHADOW_START", JOURNAL_DDL)
        self.assertIn("UNIQUE(symbol,direction,candle_open_ms)", JOURNAL_DDL)
        self.assertIn("ON DELETE RESTRICT", JOURNAL_DDL)

    def test_feedback_categories_include_fake_and_skip(self):
        self.assertIn("FAKE_SIGNAL", FEEDBACK_CATEGORIES)
        self.assertIn("SKIP", FEEDBACK_CATEGORIES)

    def test_feedback_categories_cover_signal_and_missed_setup_review(self):
        self.assertIn("GOOD_ENTRY", FEEDBACK_CATEGORIES)
        self.assertIn("BAD_SIGNAL", FEEDBACK_CATEGORIES)
        self.assertIn("LATE_ENTRY", FEEDBACK_CATEGORIES)
        self.assertIn("MISSED_OPPORTUNITY", FEEDBACK_CATEGORIES)
        self.assertIn("STOP_LOSS_TOO_TIGHT", FEEDBACK_CATEGORIES)
        self.assertIn("TARGET_TOO_CLOSE", FEEDBACK_CATEGORIES)
        self.assertIn("WRONG_DIRECTION", FEEDBACK_CATEGORIES)
        self.assertIn("NEEDS_REVIEW", FEEDBACK_CATEGORIES)



class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, fetchrows=None, fetchrows_result=None):
        self.fetchrow = AsyncMock(side_effect=fetchrows or [])
        self.fetch = AsyncMock(return_value=fetchrows_result or [])
        self.execute = AsyncMock()

    def transaction(self):
        return FakeTransaction()


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


class FakeStore:
    def __init__(self, conn):
        self.pool = FakePool(conn)


class ConfigApprovalTests(unittest.IsolatedAsyncioTestCase):
    async def test_approve_proposal_merges_delta_and_records_version(self):
        current = {
            "adx_min": 20.0, "rsi_long_min": 52.0, "rsi_long_max": 70.0,
            "rsi_short_min": 30.0, "rsi_short_max": 42.0, "concurrency": 8,
        }
        conn = FakeConnection(
            fetchrows=[
                {"id": 7, "base_config_version": "v2", "proposed_config_version": "v3",
                 "proposed_config": json.dumps({"adx_min": 22.0}), "status": "PROPOSED"},
                {"config_version": "v2", "config": json.dumps(current)},
            ],
            fetchrows_result=[{"config_version": "v2"}],
        )
        journal = SignalJournal(FakeStore(conn))
        result = await journal.approve_proposal(7, "12345")
        self.assertEqual(result["version"], "v3")
        self.assertEqual(result["previous_version"], "v2")
        self.assertEqual(result["config"]["adx_min"], 22.0)
        self.assertEqual(result["config"]["concurrency"], 8)
        self.assertEqual(conn.execute.await_count, 4)
        self.assertIn("status='INACTIVE'", conn.execute.await_args_list[0].args[0])

    async def test_stale_proposal_cannot_override_newer_active_config(self):
        conn = FakeConnection(fetchrows=[
            {"id": 8, "base_config_version": "v2", "proposed_config_version": "v3",
             "proposed_config": '{"adx_min":22}', "status": "PROPOSED"},
            {"config_version": "v3", "config": '{"adx_min":24}'},
        ])
        journal = SignalJournal(FakeStore(conn))
        with self.assertRaisesRegex(ValueError, "stale"):
            await journal.approve_proposal(8, "12345")
        conn.execute.assert_not_awaited()

    async def test_rollback_restores_previous_version_and_audits_actor(self):
        previous_config = {"adx_min": 20.0, "rsi_long_min": 52.0}
        conn = FakeConnection(fetchrows=[
            {"config_version": "v3", "config": '{"adx_min":22}', "previous_version": "v2"},
            {"config_version": "v2", "config": json.dumps(previous_config)},
        ])
        journal = SignalJournal(FakeStore(conn))
        result = await journal.rollback_config("12345")
        self.assertEqual(result["version"], "v2")
        self.assertEqual(result["previous_version"], "v3")
        self.assertEqual(result["config"]["adx_min"], 20.0)
        self.assertEqual(conn.execute.await_count, 4)
        self.assertIn("UPDATE config_history SET status='ACTIVE'", conn.execute.await_args_list[1].args[0])
        self.assertIn("'ACTIVE'", conn.execute.await_args_list[1].args[0])



class ShadowModeTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_shadow_keeps_live_config_active_and_versions_proposal(self):
        from datetime import datetime, timezone

        proposal = {
            "id": 9, "base_config_version": "v2", "proposed_config_version": "v3",
            "proposed_config": '{"adx_min":22}', "status": "PROPOSED",
        }
        active = {"config_version": "v2", "config": '{"adx_min":20,"concurrency":8}'}
        conn = FakeConnection(fetchrows=[
            proposal, active, None, None,
            {"shadow_until": datetime(2026, 11, 23, tzinfo=timezone.utc)},
        ], fetchrows_result=[{"config_version": "v2"}])
        journal = SignalJournal(FakeStore(conn))
        result = await journal.start_shadow_proposal(9, "12345", days=3)
        self.assertEqual(result["version"], "v3")
        self.assertEqual(result["config"]["adx_min"], 22)
        self.assertEqual(result["config"]["concurrency"], 8)
        self.assertEqual(result["base_version"], "v2")
        self.assertEqual(conn.execute.await_count, 3)
        self.assertIn("'SHADOW'", conn.execute.await_args_list[0].args[0])
        self.assertIn("UPDATE training_proposals SET status='SHADOW'", conn.execute.await_args_list[1].args[0])
        self.assertIn("SHADOW_START", conn.execute.await_args_list[2].args[0])

    async def test_start_shadow_rejects_stale_proposal_without_writes(self):
        conn = FakeConnection(fetchrows=[
            {"id": 10, "base_config_version": "v1", "proposed_config_version": "v2",
             "proposed_config": '{"adx_min":22}', "status": "PROPOSED"},
            {"config_version": "v3", "config": '{"adx_min":24}'},
        ])
        journal = SignalJournal(FakeStore(conn))
        with self.assertRaisesRegex(ValueError, "stale"):
            await journal.start_shadow_proposal(10, "12345", days=3)
        conn.execute.assert_not_awaited()

    async def test_start_shadow_rejects_second_running_shadow(self):
        conn = FakeConnection(fetchrows=[
            {"id": 11, "base_config_version": "v2", "proposed_config_version": "v4",
             "proposed_config": '{"adx_min":22}', "status": "PROPOSED"},
            {"config_version": "v2", "config": '{"adx_min":20}'},
            None,
            {"config_version": "v3"},
        ])
        journal = SignalJournal(FakeStore(conn))
        with self.assertRaisesRegex(ValueError, "already running"):
            await journal.start_shadow_proposal(11, "12345", days=3)
        conn.execute.assert_not_awaited()

    async def test_active_shadow_returns_proposal_id_and_deadline(self):
        from datetime import datetime, timezone

        conn = FakeConnection(fetchrows=[{
            "config_version": "v4", "config": '{"adx_min":22}',
            "shadow_until": datetime(2026, 11, 23, tzinfo=timezone.utc),
            "proposal_id": 12,
        }])
        journal = SignalJournal(FakeStore(conn))
        active = await journal.active_shadow_config()
        self.assertEqual(active["version"], "v4")
        self.assertEqual(active["proposal_id"], 12)
        self.assertEqual(active["config"]["adx_min"], 22)
        self.assertIn("p.status='SHADOW'", conn.fetchrow.await_args.args[0])

    async def test_stop_shadow_returns_proposal_to_pending_approval(self):
        conn = FakeConnection(fetchrows=[{
            "config_version": "v4", "previous_version": "v3",
        }])
        journal = SignalJournal(FakeStore(conn))
        result = await journal.stop_shadow("12345")
        self.assertEqual(result, {"version": "v4", "status": "STOPPED"})
        self.assertEqual(conn.execute.await_count, 3)
        self.assertIn("status='INACTIVE'", conn.execute.await_args_list[0].args[0])
        self.assertIn("status='PROPOSED'", conn.execute.await_args_list[1].args[0])
        self.assertIn("SHADOW_STOP", conn.execute.await_args_list[2].args[0])

    async def test_expire_shadow_returns_proposal_to_pending_approval(self):
        conn = FakeConnection(fetchrows=[{
            "config_version": "v4", "previous_version": "v3",
        }])
        journal = SignalJournal(FakeStore(conn))
        self.assertTrue(await journal.expire_shadow_if_needed())
        self.assertEqual(conn.execute.await_count, 3)
        self.assertIn("SHADOW_EXPIRE", conn.execute.await_args_list[2].args[0])

if __name__ == "__main__":
    unittest.main()
