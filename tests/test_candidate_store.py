import unittest
from unittest.mock import AsyncMock, patch

from candidate_store import CandidateStore, DDL


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
        conn = FakeConnection(fetchrows=[None, {"id": 71}])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "BTC/USDT", "side": "LONG", "candle": 1_800_000,
                  "entry": 100.0, "stop": 98.0, "target": 103.0}
        queued = await store.enqueue_signal(signal, candidate_id=9, cooldown_candles=3)
        self.assertTrue(queued)
        self.assertEqual(len(conn.fetchrow_calls), 2)
        sql = "\n".join(call[0] for call in conn.fetchrow_calls)
        self.assertIn("signal_cooldowns", sql)
        self.assertIn("signal_history", sql)
        executed = "\n".join(call.args[0] for call in conn.execute.await_args_list)
        self.assertIn("telegram_outbox", executed)
        self.assertIn("TRIGGERED", executed)
        self.assertIn("signal_cooldowns", executed)

    async def test_duplicate_signal_is_not_queued_twice(self):
        conn = FakeConnection(fetchrows=[None, None])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "ETH/USDT", "side": "SHORT", "candle": 2_100_000,
                  "entry": 100.0, "stop": 102.0, "target": 97.0}
        self.assertFalse(await store.enqueue_signal(signal, None, 3))
        self.assertEqual(conn.execute.await_count, 1)  # per-symbol advisory transaction lock only

    async def test_same_direction_cooldown_is_durable(self):
        conn = FakeConnection(fetchrows=[{
            "last_direction": "LONG", "last_candle_open_ms": 3_000_000
        }])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "SOL/USDT", "side": "LONG", "candle": 3_300_000,
                  "entry": 100.0, "stop": 98.0, "target": 103.0}
        self.assertFalse(await store.enqueue_signal(signal, candidate_id=12, cooldown_candles=3))
        self.assertEqual(conn.execute.await_count, 2)  # advisory lock plus candidate invalidation
        self.assertIn("INVALID", conn.execute.await_args_list[-1].args[0])

    async def test_opposite_direction_is_not_blocked_by_same_direction_cooldown(self):
        conn = FakeConnection(fetchrows=[
            {"last_direction": "LONG", "last_candle_open_ms": 3_000_000},
            {"id": 81},
        ])
        store = CandidateStore(FakePool(conn))
        signal = {"symbol": "SOL/USDT", "side": "SHORT", "candle": 3_300_000,
                  "entry": 100.0, "stop": 102.0, "target": 97.0}
        self.assertTrue(await store.enqueue_signal(signal, None, cooldown_candles=3))

    async def test_performance_summary_returns_aggregate_and_symbol_breakdown(self):
        conn = FakeConnection(fetchrows=[{
            "delivered_signals": 12, "delivery_failed": 1, "queued": 2,
            "tracked_outcomes": 10, "open": 4, "targets": 3,
            "stop_losses": 2, "ambiguous": 1, "data_gaps": 1,
            "sent_without_outcome": 2, "avg_realized_r": 0.5,
            "avg_mfe_r": 1.2, "avg_mae_r": 0.7,
        }])
        conn.fetch = AsyncMock(side_effect=[
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
        conn = FakeConnection(fetchrows=[None, {"id": 101}])
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
            "entry": 100.0, "stop": 98.0, "target": 103.0, "risk_per_unit": 2.0,
            "best_favorable_r": 0.0, "worst_adverse_r": 0.0, "bars_observed": 0,
            "last_observed_candle_open_ms": 1_800_000, "data_gap": False,
            "best_favorable_candle_open_ms": None, "worst_adverse_candle_open_ms": None,
        }]
        store = CandidateStore(FakePool(conn))
        updated = await store.update_open_outcomes("BTC/USDT", [
            {"timestamp": 2_100_000, "high": 104.0, "low": 99.0},
        ])
        self.assertEqual(updated, 1)
        query = conn.fetch.await_args.args[0]
        self.assertIn("s.status='SENT'", query)
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "TARGET")
        self.assertAlmostEqual(args[3], 2.0)
        self.assertAlmostEqual(args[4], 0.5)
        self.assertEqual(args[5], 1)
        self.assertEqual(args[8], 103.0)
        self.assertAlmostEqual(args[9], 1.5)
        self.assertFalse(args[10])
        self.assertEqual(args[11], 2_100_000)
        self.assertEqual(args[12], 2_100_000)

    async def test_same_bar_stop_and_target_is_ambiguous_not_a_win(self):
        conn = FakeConnection()
        conn.fetch.return_value = [{
            "signal_id": 8, "direction": "LONG", "signal_candle_open_ms": 1_800_000,
            "entry": 100.0, "stop": 98.0, "target": 103.0, "risk_per_unit": 2.0,
            "best_favorable_r": 0.0, "worst_adverse_r": 0.0, "bars_observed": 0,
            "last_observed_candle_open_ms": 1_800_000, "data_gap": False,
            "best_favorable_candle_open_ms": None, "worst_adverse_candle_open_ms": None,
        }]
        store = CandidateStore(FakePool(conn))
        await store.update_open_outcomes("BTC/USDT", [
            {"timestamp": 2_100_000, "high": 104.0, "low": 97.0},
        ])
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "AMBIGUOUS")
        self.assertIsNone(args[9])
        self.assertIsNone(args[8])

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
            {"timestamp": 2_400_000, "high": 101.0, "low": 99.0},
        ])
        args = conn.execute.await_args.args
        self.assertEqual(args[2], "AMBIGUOUS")
        self.assertTrue(args[10])
        self.assertEqual(args[7], 2_400_000)
        self.assertIsNone(args[8])

    async def test_schema_has_durable_signal_outbox_cooldown_and_runtime(self):
        for table in ("signal_history", "signal_outcomes", "telegram_outbox", "signal_cooldowns", "scanner_runtime", "schema_migrations"):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", DDL)
        self.assertIn("UNIQUE(symbol,direction,candle_open_ms)", DDL)
        self.assertIn("best_favorable_candle_open_ms", DDL)
        self.assertIn("worst_adverse_candle_open_ms", DDL)
        self.assertIn("ON DELETE RESTRICT", DDL)


if __name__ == "__main__":
    unittest.main()
