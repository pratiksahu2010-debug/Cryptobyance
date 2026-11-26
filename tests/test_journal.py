import json
import unittest
from unittest.mock import AsyncMock

from journal import FEEDBACK_CATEGORIES, JOURNAL_DDL, SignalJournal, rejected_observation


class JournalSchemaTests(unittest.TestCase):
    def test_opposing_15m_context_is_diagnostic_not_a_rejection_reason(self):
        import numpy as np
        import pandas as pd
        from unittest.mock import patch

        five = pd.DataFrame({
            "timestamp": np.arange(60) * 300_000,
            "open": [101.0] * 60, "high": [102.0] * 60,
            "low": [100.0] * 60, "close": [101.0] * 60,
            "volume": [100.0] * 60,
            "ema9": [101.0] * 60, "ema21": [100.0] * 60,
            "rsi": [55.0] * 60, "adx": [25.0] * 60,
            "plus_di": [30.0] * 60, "minus_di": [10.0] * 60,
            "atr": [2.0] * 60, "vwap": [100.0] * 60,
        })
        fifteen = pd.DataFrame({
            "timestamp": np.arange(35) * 900_000,
            "open": [99.0] * 35, "high": [101.0] * 35,
            "low": [98.0] * 35, "close": [99.0] * 35,
            "volume": [100.0] * 35,
            "ema9": [99.0] * 35, "ema21": [100.0] * 35,
        })
        with patch("journal.indicators", side_effect=[five, fifteen]):
            observation = rejected_observation(
                five, fifteen, side='LONG',
                state_input={'setup_valid': True, 'trigger': False},
            )
        self.assertEqual(observation["snapshot"]["context_15m"], "BEARISH")
        self.assertNotIn("15m_trend_veto", observation["rejection_reasons"])
        self.assertNotIn("15m_context_unavailable", observation["rejection_reasons"])
        self.assertEqual(observation["rejection_reasons"], ["rsi_cross_not_confirmed"])
        self.assertEqual(observation["snapshot"]["version"], 3)
        self.assertEqual(observation["risk_model"], "swing_structure")
        self.assertAlmostEqual(observation["risk_per_unit"], 1.0)
        self.assertAlmostEqual(observation["target"], 102.0)
        self.assertAlmostEqual(observation["target2"], 103.0)
        self.assertGreater(observation["snapshot"]["price_vwap_distance_atr"], 0)
        self.assertNotIn("entry_extended_from_vwap", observation["rejection_reasons"])
        self.assertNotIn("ema_alignment_too_old", observation["rejection_reasons"])

    def test_rejected_observation_uses_production_swing_risk_and_targets(self):
        import numpy as np
        import pandas as pd
        from unittest.mock import patch

        lows = [100.0] * 60
        highs = [102.0] * 60
        lows[-11:-1] = [99.0] * 10
        five = pd.DataFrame({
            "timestamp": np.arange(60) * 300_000,
            "open": [101.0] * 60, "high": highs, "low": lows,
            "close": [101.0] * 60, "volume": [100.0] * 60,
            "ema9": [101.0] * 60, "ema21": [100.0] * 60,
            "rsi": [55.0] * 60, "adx": [25.0] * 60,
            "plus_di": [30.0] * 60, "minus_di": [10.0] * 60,
            "atr": [2.0] * 60, "vwap": [100.0] * 60,
        })
        with patch("journal.indicators", return_value=five):
            observation = rejected_observation(
                five, None, side="LONG",
                state_input={"setup_valid": True, "trigger": False},
            )
        self.assertEqual(observation["risk_model"], "swing_structure")
        self.assertAlmostEqual(observation["stop"], 99.0)
        self.assertAlmostEqual(observation["risk_per_unit"], 2.0)
        self.assertAlmostEqual(observation["target"], 103.0)
        self.assertAlmostEqual(observation["target2"], 105.0)

    def test_rejected_observation_uses_armed_side_and_exact_live_setup_gates(self):
        import numpy as np
        import pandas as pd
        from unittest.mock import patch

        five = pd.DataFrame({
            "timestamp": np.arange(60) * 300_000,
            "open": [101.0] * 60, "high": [102.0] * 60,
            "low": [100.0] * 60, "close": [101.0] * 60,
            "volume": [100.0] * 60,
            "ema9": [101.0] * 60, "ema21": [100.0] * 60,
            "rsi": [55.0] * 60, "adx": [25.0] * 60,
            "plus_di": [30.0] * 60, "minus_di": [10.0] * 60,
            "atr": [2.0] * 60, "vwap": [100.0] * 60,
        })
        with patch("journal.indicators", return_value=five):
            observation = rejected_observation(
                five, None, side="SHORT",
                state_input={"setup_valid": False, "trigger": False},
            )
        self.assertEqual(observation["direction"], "SHORT")
        self.assertEqual(
            observation["rejection_reasons"],
            ["directional_index_mismatch", "ema_direction_mismatch", "vwap_side_mismatch"],
        )

    def test_journal_tables_are_additive_and_auditable(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_feedback", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS journal_candidates", JOURNAL_DDL)
        self.assertIn("target2 DOUBLE PRECISION", JOURNAL_DDL)
        self.assertIn("risk_model TEXT", JOURNAL_DDL)
        self.assertIn("tp2_tracking_status", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS training_proposals", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS config_history", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS config_history_events", JOURNAL_DDL)
        self.assertIn("CREATE TABLE IF NOT EXISTS training_weekly_digests", JOURNAL_DDL)
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


class RejectedOutcomeTrackingTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejected_candidate_tracks_tp2_after_tp1(self):
        conn = FakeConnection(fetchrows_result=[{
            "id": 41, "direction": "LONG", "candle_open_ms": 1_800_000,
            "entry": 101.0, "stop": 99.0, "target": 103.0, "target2": 105.0,
            "risk_per_unit": 2.0, "cost_pct": 0.10,
            "best_favorable_r": 1.0, "worst_adverse_r": 0.25,
            "bars_observed": 1, "last_observed_candle_open_ms": 2_100_000,
            "data_gap": False, "status": "TARGET",
            "exit_candle_open_ms": 2_100_000, "realized_r": 0.9495,
            "tp1_hit_candle_open_ms": 2_100_000, "tp2_hit_candle_open_ms": None,
            "tp2_tracking_status": "OPEN", "tp2_realized_r": None,
            "tp2_bars_observed": 0,
        }])
        journal = SignalJournal(FakeStore(conn))
        updated = await journal.update_rejected_outcomes("BTC/USDT", [{
            "timestamp": 2_400_000, "high": 106.0, "low": 100.5, "close": 105.0,
        }])
        self.assertEqual(updated, 1)
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "TARGET")
        self.assertAlmostEqual(args[9], 0.9495)
        self.assertEqual(args[11], 2_400_000)
        self.assertEqual(args[12], "HIT")
        self.assertAlmostEqual(args[13], 1.9495)

    async def test_rejected_candidate_stop_after_tp1_only_closes_continuation(self):
        conn = FakeConnection(fetchrows_result=[{
            "id": 42, "direction": "LONG", "candle_open_ms": 1_800_000,
            "entry": 101.0, "stop": 99.0, "target": 103.0, "target2": 105.0,
            "risk_per_unit": 2.0, "cost_pct": 0.10,
            "best_favorable_r": 1.0, "worst_adverse_r": 0.25,
            "bars_observed": 1, "last_observed_candle_open_ms": 2_100_000,
            "data_gap": False, "status": "TARGET",
            "exit_candle_open_ms": 2_100_000, "realized_r": 0.9495,
            "tp1_hit_candle_open_ms": 2_100_000, "tp2_hit_candle_open_ms": None,
            "tp2_tracking_status": "OPEN", "tp2_realized_r": None,
            "tp2_bars_observed": 0,
        }])
        journal = SignalJournal(FakeStore(conn))
        await journal.update_rejected_outcomes("BTC/USDT", [{
            "timestamp": 2_400_000, "high": 102.0, "low": 98.0, "close": 100.0,
        }])
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "TARGET")
        self.assertAlmostEqual(args[9], 0.9495)
        self.assertEqual(args[12], "STOPPED")

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
                None,
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
            None,
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
            None,
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


class EntryTimingPolicyFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_latest_hold_policy_disables_an_older_active_adjustment(self):
        conn = FakeConnection(fetchrows=[{
            "training_day": "2026-11-25",
            "effective_from": "2026-11-26",
            "direction": "LONG",
            "status": "HOLD",
            "delay_bars": 0,
            "median_entry_adjustment_pct": 0.0,
            "sample_count": 4,
        }])
        journal = SignalJournal(FakeStore(conn))
        policy = await journal.get_entry_timing_policy("LONG")
        self.assertIsNone(policy)
        self.assertIn("status", conn.fetchrow.await_args.args[0])

    async def test_active_policy_is_returned_when_it_is_the_latest_daily_decision(self):
        conn = FakeConnection(fetchrows=[{
            "training_day": "2026-11-25",
            "effective_from": "2026-11-26",
            "direction": "SHORT",
            "status": "ACTIVE",
            "delay_bars": 2,
            "median_entry_adjustment_pct": -0.18,
            "sample_count": 24,
        }])
        journal = SignalJournal(FakeStore(conn))
        policy = await journal.get_entry_timing_policy("SHORT")
        self.assertEqual(policy["delay_bars"], 2)
        self.assertEqual(policy["status"], "ACTIVE")

