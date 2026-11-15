import unittest
from unittest.mock import AsyncMock

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

    async def test_outbox_claim_uses_skip_locked_and_lease(self):
        conn = FakeConnection(fetchrows=[{
            "id": 5, "payload": '{"symbol":"BTC/USDT","side":"LONG"}', "attempts": 1
        }])
        store = CandidateStore(FakePool(conn))
        item = await store.claim_outbox()
        self.assertEqual(item["id"], 5)
        self.assertEqual(item["attempts"], 2)
        self.assertIn("FOR UPDATE SKIP LOCKED", conn.fetchrow_calls[0][0])
        self.assertIn("lease_until", conn.execute.await_args.args[0])

    async def test_pending_candidate_gets_exactly_three_followup_candles(self):
        first = 1_800_000
        row = {"id": 17, "direction": "LONG",
               "expires_after_candle_open_ms": first + 3 * 300_000,
               "last_checked_candle_open_ms": first + 2 * 300_000}
        conn = FakeConnection(fetchrows=[row])
        store = CandidateStore(FakePool(conn))
        state, candidate_id = await store.observe(
            "BTC/USDT", "LONG", first + 3 * 300_000, 25.0, 25.0
        )
        self.assertEqual((state, candidate_id), ("TRIGGER", 17))
        self.assertEqual(conn.execute.await_count, 2)  # expiry check and last-checked update

    async def test_new_pending_candidate_expires_after_three_followup_bars(self):
        first = 2_100_000
        conn = FakeConnection(fetchrows=[None, {"id": 29}])
        store = CandidateStore(FakePool(conn))
        state, candidate_id = await store.observe(
            "ETH/USDT", "SHORT", first, 18.0, 25.0
        )
        self.assertEqual((state, candidate_id), ("PENDING", 29))
        insert_args = conn.fetchrow_calls[1][1]
        self.assertEqual(insert_args[3], first + 3 * 300_000)

    async def test_outbox_exhaustion_moves_alert_to_dead_letter(self):
        conn = FakeConnection(fetchrows=[{"signal_id": 91}])
        store = CandidateStore(FakePool(conn))
        await store.retry_outbox(8, attempts=12, error_type="TelegramUnavailable")
        self.assertEqual(conn.execute.await_count, 2)
        self.assertIn("status='DEAD'", conn.execute.await_args_list[0].args[0])
        self.assertIn("DELIVERY_FAILED", conn.execute.await_args_list[1].args[0])

    async def test_schema_has_durable_signal_outbox_cooldown_and_runtime(self):
        for table in ("signal_history", "telegram_outbox", "signal_cooldowns", "scanner_runtime", "schema_migrations"):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", DDL)
        self.assertIn("UNIQUE(symbol,direction,candle_open_ms)", DDL)
        self.assertIn("ON DELETE RESTRICT", DDL)


if __name__ == "__main__":
    unittest.main()
