import json
import unittest
from unittest.mock import AsyncMock, patch

from candidate_store import CandidateStore, DDL
from state_machine import STATE_DDL


class _Context:
    def __init__(self, value):
        self.value = value
    async def __aenter__(self):
        return self.value
    async def __aexit__(self, *args):
        return False


class FakeConnection:
    def __init__(self, fetchrows=()):
        self.fetchrows = list(fetchrows)
        self.execute = AsyncMock(return_value="UPDATE 1")
        self.fetchrow_calls = []
        self.fetchrow = AsyncMock(side_effect=self._fetchrow)
        self.fetch = AsyncMock(return_value=[])
        self.transaction = lambda: _Context(None)
    async def _fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        return self.fetchrows.pop(0) if self.fetchrows else None


class FakePool:
    def __init__(self, conn):
        self.conn = conn
    def acquire(self):
        return _Context(self.conn)


class CandidateStoreArchitectureTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_repairs_legacy_outcome_rows(self):
        conn = FakeConnection()
        pool = FakePool(conn)
        with patch("candidate_store.asyncpg.create_pool", new=AsyncMock(return_value=pool)):
            await CandidateStore.connect("postgresql://test.invalid/db")
        statements = "\\n".join(call.args[0] for call in conn.execute.await_args_list)
        self.assertIn("DELETE FROM signal_outcomes", statements)
        self.assertIn("s.status <> 'SENT'", statements)
        self.assertIn("INSERT INTO signal_outcomes", statements)
        self.assertIn("s.status='SENT'", statements)
        self.assertIn("ON CONFLICT(signal_id) DO NOTHING", statements)

    async def test_signal_is_atomically_recorded_and_queued(self):
        conn = FakeConnection(fetchrows=[None, None, None, {"id": 71}])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "BTC/USDT", "side": "LONG", "candle": 1_800_000,
                  "entry": 100.0, "stop": 98.0, "target": 103.0}
        queued = await store.enqueue_signal(signal, candidate_id=9, cooldown_candles=3)
        self.assertTrue(queued)
        history_insert = next(args for query, args in conn.fetchrow_calls
                              if "INSERT INTO signal_history" in query)
        saved_payload = json.loads(history_insert[4])
        self.assertEqual(
            saved_payload["performance_attribution"]["strategy_key"], "default"
        )
        self.assertEqual(len(conn.fetchrow_calls), 4)
        sql = "\n".join(call[0] for call in conn.fetchrow_calls)
        self.assertIn("signal_cooldowns", sql)
        self.assertIn("signal_history", sql)
        executed = "\n".join(call.args[0] for call in conn.execute.await_args_list)
        self.assertIn("telegram_outbox", executed)
        self.assertIn("TRIGGERED", executed)
        self.assertIn("signal_cooldowns", executed)
        self.assertIn("strategy_state_events", executed)
        self.assertIn("strategy_signal_states", executed)
        self.assertTrue(any("strategy_signal_states" in query for query, _ in conn.fetchrow_calls))

    async def test_duplicate_signal_is_not_queued_twice(self):
        conn = FakeConnection(fetchrows=[None, {"id": 72}])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "ETH/USDT", "side": "SHORT", "candle": 2_100_000,
                  "entry": 100.0, "stop": 102.0, "target": 97.0}
        self.assertFalse(await store.enqueue_signal(signal, None, 3))
        self.assertEqual(conn.execute.await_count, 2)  # symbol and strategy-direction advisory locks

    async def test_same_direction_cooldown_is_durable(self):
        conn = FakeConnection(fetchrows=[{
            "last_direction": "LONG", "last_candle_open_ms": 3_000_000
        }])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "SOL/USDT", "side": "LONG", "candle": 3_300_000,
                  "entry": 100.0, "stop": 98.0, "target": 103.0}
        self.assertFalse(await store.enqueue_signal(signal, candidate_id=12, cooldown_candles=3))
        self.assertEqual(conn.execute.await_count, 3)  # two advisory locks plus candidate invalidation
        self.assertIn("INVALID", conn.execute.await_args_list[-1].args[0])

    async def test_opposite_direction_is_not_blocked_by_same_direction_cooldown(self):
        conn = FakeConnection(fetchrows=[
            {"last_direction": "LONG", "last_candle_open_ms": 3_000_000},
            None,
            None,
            {"id": 81},
        ])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "SOL/USDT", "side": "SHORT", "candle": 3_300_000,
                  "entry": 100.0, "stop": 102.0, "target": 97.0}
        self.assertTrue(await store.enqueue_signal(signal, None, cooldown_candles=3))

    async def test_shadow_signal_is_persisted_without_telegram_outbox(self):
        conn = FakeConnection(fetchrows=[{"id": 501}])
        store = CandidateStore(FakePool(conn))
        signal = {
            "symbol": "BTC/USDT", "side": "LONG", "candle": 1_800_000,
            "entry": 100.0, "stop": 98.0, "target": 104.0,
        }
        self.assertTrue(await store.record_shadow_signal(9, "v3", signal))
        query, args = conn.fetchrow_calls[0]
        self.assertIn("INSERT INTO shadow_signal_history", query)
        self.assertIn("ON CONFLICT", query)
        self.assertEqual(args[:5], ("v3", 9, "BTC/USDT", "LONG", 1_800_000))
        self.assertEqual(conn.execute.await_count, 0)

    async def test_shadow_signal_counts_are_scoped_to_proposal_version(self):
        conn = FakeConnection()
        conn.fetchrow = AsyncMock(return_value={
            "total": 7, "longs": 4, "shorts": 3, "symbols": 5,
        })
        store = CandidateStore(FakePool(conn))
        counts = await store.shadow_signal_counts("v3")
        self.assertEqual(counts, {"total": 7, "longs": 4, "shorts": 3, "symbols": 5})
        self.assertIn("WHERE proposal_version=$1", conn.fetchrow.await_args.args[0])

    async def test_load_symbol_state_metadata_returns_armed_cycle_details(self):
        conn = FakeConnection()
        conn.fetch.return_value = [{
            "direction": "LONG", "state": "ARMED",
            "armed_candle_open_ms": 1_800_000, "cycle_id": 12,
            "last_processed_candle_open_ms": 1_800_000, "setup_invalidated": False,
        }]
        store = CandidateStore(FakePool(conn))
        details = await store.load_symbol_state_metadata("strategy-v2", "BTC/USDT")
        self.assertEqual(details["LONG"]["state"], "ARMED")
        self.assertEqual(details["LONG"]["armed_candle_open_ms"], 1_800_000)
        self.assertEqual(details["LONG"]["cycle_id"], 12)
        self.assertFalse(details["LONG"]["setup_invalidated"])

    async def test_restart_gap_invalidates_armed_state_before_trigger(self):
        conn = FakeConnection(fetchrows=[{
            "state": "ARMED",
            "last_processed_candle_open_ms": 1_800_000,
            "setup_invalidated": False,
            "cooldown_until_candle_open_ms": None,
        }])
        store = CandidateStore(FakePool(conn))
        action = await store.advance_symbol_state(
            strategy_key="coindcx_per_symbol_v2",
            symbol="BTC/USDT", direction="LONG",
            candle_open_ms=2_400_000, setup_valid=True,
            invalidation=False, trigger=True, cooldown_candles=4,
        )
        self.assertEqual(action, "INVALIDATED")
        update = next(call.args[0] for call in conn.execute.await_args_list
                      if "UPDATE strategy_signal_states" in call.args[0])
        self.assertIn("state=$4", update)
        self.assertIn("strategy_state_events", "\\n".join(
            call.args[0] for call in conn.execute.await_args_list
        ))

    async def test_performance_summary_returns_aggregate_and_symbol_breakdown(self):
        conn = FakeConnection(fetchrows=[{
            "delivered_signals": 12, "delivery_failed": 1, "queued": 2,
            "tracked_outcomes": 10, "open": 4, "targets": 3,
            "stop_losses": 2, "ambiguous": 1, "data_gaps": 1,
            "sent_without_outcome": 2, "avg_realized_r": 0.5,
            "avg_mfe_r": 1.2, "avg_mae_r": 0.7,
        }])
        conn.fetch = AsyncMock(side_effect=[
            [{"strategy_key": "ema_vwap_rsi_adx_15m_v1", "delivered_signals": 10,
              "targets": 3, "stop_losses": 2, "open": 4, "ambiguous": 1,
              "avg_realized_r": 0.5}],
            [{"symbol": "BTC/USDT", "direction": "LONG", "tracked": 4,
              "targets": 2, "stop_losses": 1, "open": 1, "ambiguous": 0,
              "avg_realized_r": 0.75, "avg_mfe_r": 1.4, "avg_mae_r": 0.5}],
            [{"status": "STOP_LOSS", "count": 2, "avg_mfe_r": 0.4,
              "median_mfe_r": 0.3, "avg_mae_r": 1.0,
              "stop_mfe_050r_before_exit": 1}],
            [{"direction": "LONG", "status": "STOP_LOSS", "count": 2}],
            [{"status": "SENT", "count": 12}, {"status": "DEAD", "count": 1}],
        ])
        store = CandidateStore(FakePool(conn))
        report = await store.performance_summary()
        self.assertEqual(report["totals"]["delivered_signals"], 12)
        self.assertEqual(report["totals"]["sent_without_outcome"], 2)
        self.assertEqual(report["by_strategy"][0]["strategy_key"], "ema_vwap_rsi_adx_15m_v1")
        self.assertEqual(report["by_strategy"][0]["delivered_signals"], 10)
        self.assertEqual(report["by_symbol_direction"][0]["symbol"], "BTC/USDT")
        self.assertEqual(report["risk_by_outcome"][0]["stop_mfe_050r_before_exit"], 1)
        self.assertEqual(report["by_direction"][0]["direction"], "LONG")
        self.assertEqual(report["outbox"][1]["status"], "DEAD")
        self.assertIn("AVG(o.realized_r)", conn.fetchrow.await_args.args[0])

    async def test_outbox_claim_uses_skip_locked_and_lease(self):
        conn = FakeConnection(fetchrows=[{
            "id": 5, "signal_id": 55, "payload": '{"symbol":"BTC/USDT","side":"LONG"}', "attempts": 1
        }])
        store = CandidateStore(FakePool(conn))
        item = await store.claim_outbox()
        self.assertEqual(item["id"], 5)
        self.assertEqual(item["attempts"], 2)
        self.assertIn("FOR UPDATE SKIP LOCKED", conn.fetchrow_calls[0][0])
        self.assertIn("lease_until", conn.execute.await_args.args[0])

    async def test_adx_qualified_candidate_is_ready_on_same_candle_only(self):
        conn = FakeConnection()
        store = CandidateStore(FakePool(conn))
        state, candidate_id = await store.observe(
            "BTC/USDT", "LONG", 1_800_000, 20.0, 20.0
        )
        self.assertEqual((state, candidate_id), ("READY", None))
        self.assertEqual(conn.execute.await_count, 1)
        self.assertIn("INVALID", conn.execute.await_args.args[0])

    async def test_adx_below_threshold_is_rejected_without_pending_candidate(self):
        conn = FakeConnection()
        store = CandidateStore(FakePool(conn))
        state, candidate_id = await store.observe(
            "ETH/USDT", "SHORT", 2_100_000, 19.9, 20.0
        )
        self.assertEqual((state, candidate_id), ("REJECT", None))
        self.assertEqual(conn.execute.await_count, 1)
        self.assertNotIn("INSERT", conn.execute.await_args.args[0].upper())

    async def test_outbox_exhaustion_moves_alert_to_dead_letter(self):
        conn = FakeConnection(fetchrows=[{"signal_id": 91}])
        store = CandidateStore(FakePool(conn))
        await store.retry_outbox(8, attempts=12, error_type="TelegramUnavailable")
        self.assertEqual(conn.execute.await_count, 2)
        self.assertIn("status='DEAD'", conn.execute.await_args_list[0].args[0])
        self.assertIn("DELIVERY_FAILED", conn.execute.await_args_list[1].args[0])

    async def test_cleanup_expires_candidates_but_never_deletes_rows(self):
        conn = FakeConnection()
        conn.execute.return_value = "UPDATE 2"
        store = CandidateStore(FakePool(conn))
        result = await store.cleanup(now_ms=9_000_000)
        self.assertEqual(result, {"expired": 2, "deleted": 0})
        self.assertEqual(conn.execute.await_count, 1)
        sql = conn.execute.await_args.args[0]
        self.assertIn("SET status='EXPIRED'", sql)
        self.assertNotIn("DELETE", sql.upper())

    async def test_queued_signal_does_not_create_outcome_before_delivery(self):
        conn = FakeConnection(fetchrows=[None, None, None, {"id": 101}])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "BTC/USDT", "side": "LONG", "candle": 1_800_000,
                  "entry": 100.0, "stop": 98.0, "target": 103.0,
                  "diagnostics": {"version": 1, "adx14": 24.0}}
        self.assertTrue(await store.enqueue_signal(signal, None, 3))
        sql = "\\n".join(call.args[0] for call in conn.execute.await_args_list)
        self.assertNotIn("INSERT INTO signal_outcomes", sql)

    async def test_outcome_row_is_created_atomically_after_confirmed_delivery(self):
        conn = FakeConnection(fetchrows=[{"signal_id": 101}])
        store = CandidateStore(FakePool(conn))
        await store.finish_outbox(12)
        sql = "\\n".join(call.args[0] for call in conn.execute.await_args_list)
        self.assertIn("UPDATE signal_history SET status='SENT'", sql)
        self.assertIn("INSERT INTO signal_outcomes", sql)
        self.assertIn("FROM signal_history WHERE id=$1", sql)
        self.assertIn("ON CONFLICT(signal_id) DO NOTHING", sql)

    async def test_outcome_tracker_records_target_and_mfe_mae_in_r(self):
        conn = FakeConnection()
        conn.fetch.return_value = [{
            "signal_id": 7, "direction": "LONG", "signal_candle_open_ms": 1_800_000,
            "entry": 100.0, "stop": 98.0, "target": 104.0,
            "target1": 102.0, "target2": 104.0, "atr_at_entry": 4.0,
            "risk_per_unit": 2.0, "outcome_horizon_bars": 12, "cost_pct": 0.10,
            "best_favorable_r": 0.0, "worst_adverse_r": 0.0,
            "best_favorable_pct": 0.0, "worst_adverse_pct": 0.0,
            "best_favorable_atr": None, "worst_adverse_atr": None,
            "bars_observed": 0, "last_observed_candle_open_ms": 1_800_000,
            "data_gap": False, "best_favorable_candle_open_ms": None,
            "worst_adverse_candle_open_ms": None,
        }]
        store = CandidateStore(FakePool(conn))
        updated = await store.update_open_outcomes("BTC/USDT", [
            {"timestamp": 2_100_000, "high": 104.0, "low": 99.0, "close": 103.0},
        ])
        self.assertEqual(updated, 1)
        query = conn.fetch.await_args.args[0]
        self.assertIn("s.status='SENT'", query)
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "TARGET")
        self.assertEqual(args[3], "TP1")
        self.assertEqual(args[4], "WIN")
        self.assertAlmostEqual(args[5], 2.0)
        self.assertAlmostEqual(args[6], 0.5)
        self.assertAlmostEqual(args[7], 4.0)
        self.assertAlmostEqual(args[8], 1.0)
        self.assertAlmostEqual(args[9], 1.0)
        self.assertAlmostEqual(args[10], 0.25)
        self.assertEqual(args[11], 1)
        self.assertEqual(args[12], 1)
        self.assertEqual(args[14], 2_100_000)
        self.assertEqual(args[15], 102.0)
        self.assertAlmostEqual(args[16], 0.95)
        self.assertFalse(args[17])
    async def test_same_bar_stop_and_target_is_conservative_loss(self):
        conn = FakeConnection()
        conn.fetch.return_value = [{
            "signal_id": 8, "direction": "LONG", "signal_candle_open_ms": 1_800_000,
            "entry": 100.0, "stop": 98.0, "target": 104.0,
            "target1": 102.0, "target2": 104.0, "atr_at_entry": 4.0,
            "risk_per_unit": 2.0, "best_favorable_r": 0.0,
            "worst_adverse_r": 0.0, "bars_observed": 0,
            "last_observed_candle_open_ms": 1_800_000, "data_gap": False,
            "best_favorable_candle_open_ms": None, "worst_adverse_candle_open_ms": None,
        }]
        store = CandidateStore(FakePool(conn))
        await store.update_open_outcomes("BTC/USDT", [
            {"timestamp": 2_100_000, "high": 105.0, "low": 97.0, "close": 100.0},
        ])
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "STOP_LOSS")
        self.assertEqual(args[3], "SL")
        self.assertEqual(args[4], "LOSS")
        self.assertAlmostEqual(args[16], -1.05)
        self.assertFalse(args[17])
    async def test_outcome_tracker_flags_missing_candle_intervals(self):
        conn = FakeConnection()
        conn.fetch.return_value = [{
            "signal_id": 9, "direction": "SHORT", "signal_candle_open_ms": 1_800_000,
            "entry": 100.0, "stop": 102.0, "target": 97.0, "risk_per_unit": 2.0,
            "best_favorable_r": 0.0, "worst_adverse_r": 0.0, "bars_observed": 0,
            "last_observed_candle_open_ms": 1_800_000, "data_gap": False,
            "best_favorable_candle_open_ms": None, "worst_adverse_candle_open_ms": None,
        }]
        store = CandidateStore(FakePool(conn))
        await store.update_open_outcomes("ETH/USDT", [
            {"timestamp": 2_400_000, "high": 101.0, "low": 99.0, "close": 100.0},
        ])
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "AMBIGUOUS")
        self.assertEqual(args[3], "AMBIGUOUS")
        self.assertIsNone(args[4])
        self.assertTrue(args[17])
        self.assertEqual(args[13], 2_400_000)
        self.assertIsNone(args[15])
    async def test_schema_has_durable_signal_outbox_cooldown_and_runtime(self):
        for table in ("signal_history", "signal_outcomes", "telegram_outbox", "signal_cooldowns", "scanner_runtime", "schema_migrations"):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", DDL)
        self.assertIn("UNIQUE(symbol,direction,candle_open_ms)", DDL)
        self.assertIn("best_favorable_candle_open_ms", DDL)
        self.assertIn("worst_adverse_candle_open_ms", DDL)
        self.assertIn("target1 DOUBLE PRECISION", DDL)
        self.assertIn("target2 DOUBLE PRECISION", DDL)
        self.assertIn("outcome_horizon_bars", DDL)
        self.assertIn("first_hit", DDL)
        self.assertIn("best_favorable_pct", DDL)
        self.assertIn("best_favorable_atr", DDL)
        self.assertIn("'TIMEOUT'", DDL)
        self.assertIn("ON DELETE RESTRICT", DDL)
        self.assertIn("strategy_signal_states", STATE_DDL)
        self.assertIn("strategy_state_events", STATE_DDL)


if __name__ == "__main__":
    unittest.main()
