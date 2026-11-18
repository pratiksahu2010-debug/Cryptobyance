"""Regression tests for REST-confirmed candle cache integrity."""

import asyncio
import unittest
from unittest.mock import AsyncMock

from coindcx_feed import CoinDCXFuturesFeed


def _row(timestamp: int, close: float) -> list[float | int]:
    return [timestamp, close, close + 1, close - 1, close, 10.0]


class TestRestConfirmedCandles(unittest.TestCase):
    def make_feed(self) -> CoinDCXFuturesFeed:
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        self.addCleanup(lambda: asyncio.run(feed.close()))
        return feed

    def test_websocket_cannot_overwrite_rest_confirmed_candle(self):
        feed = self.make_feed()
        pair = "B-BTC_USDT"
        timestamp = 1_800_000_000_000
        provisional = _row(timestamp, 100.0)
        confirmed = _row(timestamp, 101.0)
        late_websocket = _row(timestamp, 99.0)

        feed._merge_rows(pair, [provisional], source="websocket")
        feed._merge_rows(pair, [confirmed], source="rest")
        feed._rest_confirmed_closed_opens[pair].add(timestamp)
        feed._merge_rows(pair, [late_websocket], source="websocket")

        self.assertEqual(feed.rows(pair), [confirmed])

    def test_websocket_can_update_unconfirmed_newer_candle(self):
        feed = self.make_feed()
        pair = "B-BTC_USDT"
        confirmed_timestamp = 1_800_000_000_000
        newer_timestamp = confirmed_timestamp + 300_000
        confirmed = _row(confirmed_timestamp, 101.0)
        provisional_newer = _row(newer_timestamp, 102.0)
        updated_newer = _row(newer_timestamp, 103.0)

        feed._merge_rows(pair, [confirmed], source="rest")
        feed._rest_confirmed_closed_opens[pair].add(confirmed_timestamp)
        feed._merge_rows(pair, [provisional_newer], source="websocket")
        feed._merge_rows(pair, [updated_newer], source="websocket")

        self.assertEqual(feed.rows(pair), [confirmed, updated_newer])

    def test_rest_can_replace_provisional_websocket_candle(self):
        feed = self.make_feed()
        pair = "B-BTC_USDT"
        timestamp = 1_800_000_000_000
        provisional = _row(timestamp, 100.0)
        confirmed = _row(timestamp, 101.0)

        feed._merge_rows(pair, [provisional], source="websocket")
        feed._merge_rows(pair, [confirmed], source="rest")

        self.assertEqual(feed.rows(pair), [confirmed])


    def test_initial_websocket_connection_does_not_duplicate_bootstrap(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        feed.sio.emit = AsyncMock()

        async def run():
            await feed._on_connect()
            self.assertIsNone(feed._reconnect_backfill_task)
            await feed.close()

        asyncio.run(run())

    def test_reconnect_backfills_full_history_for_each_active_pair(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT", "B-ETH_USDT"])
        feed.sio.emit = AsyncMock()
        feed.fetch_history = AsyncMock(return_value=[_row(1_800_000_000_000, 100.0)])

        async def run():
            await feed._on_connect()
            await feed._on_disconnect()
            await feed._on_connect()
            task = feed._reconnect_backfill_task
            self.assertIsNotNone(task)
            await task
            self.assertEqual(feed.fetch_history.await_count, 2)
            self.assertEqual(
                {call.args[0] for call in feed.fetch_history.await_args_list},
                {"B-BTC_USDT", "B-ETH_USDT"},
            )
            self.assertTrue(all(call.kwargs["limit"] == 320 for call in feed.fetch_history.await_args_list))
            await feed.close()

        asyncio.run(run())

    def test_reconnect_backfill_failure_for_one_pair_does_not_abort_others(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT", "B-ETH_USDT"])
        feed.sio.emit = AsyncMock()
        calls = []

        async def fetch(pair, limit=320):
            calls.append(pair)
            if pair == "B-BTC_USDT":
                raise RuntimeError("temporary REST failure")
            return [_row(1_800_000_000_000, 100.0)]

        feed.fetch_history = AsyncMock(side_effect=fetch)

        async def run():
            await feed._on_connect()
            await feed._on_disconnect()
            await feed._on_connect()
            await feed._reconnect_backfill_task
            self.assertCountEqual(calls, ["B-BTC_USDT", "B-ETH_USDT"])
            await feed.close()

        asyncio.run(run())


    def test_latest_candle_refresh_waits_for_reconnect_backfill(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        feed.fetch_history = AsyncMock(return_value=[_row(1_800_000_000_000, 100.0)])

        async def run():
            backfill_started = asyncio.Event()
            release_backfill = asyncio.Event()

            async def backfill():
                backfill_started.set()
                await release_backfill.wait()

            feed._reconnect_backfill_task = asyncio.create_task(backfill())
            refresh = asyncio.create_task(
                feed.refresh_latest_closed_candle("B-BTC_USDT", now_ms=1_800_000_300_000)
            )
            await backfill_started.wait()
            await asyncio.sleep(0)
            self.assertFalse(refresh.done())
            feed.fetch_history.assert_not_awaited()

            release_backfill.set()
            result = await refresh
            self.assertEqual(result, 1_800_000_000_000)
            feed.fetch_history.assert_awaited_once_with("B-BTC_USDT", limit=6)
            await feed.close()

        asyncio.run(run())


    def test_failed_reconnect_backfill_blocks_scan_until_full_repair_succeeds(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        expected_open = 1_800_000_000_000
        calls = []

        async def fetch(pair, limit=320):
            calls.append(limit)
            if len(calls) == 1:
                raise RuntimeError("temporary reconnect backfill failure")
            return [_row(expected_open, 100.0)]

        feed.fetch_history = AsyncMock(side_effect=fetch)

        async def run():
            # Reproduce a failed reconnect backfill; the feed records the pair
            # as blocked, then the next scan cycle attempts a full repair.
            await feed._backfill_after_reconnect()
            self.assertIn("B-BTC_USDT", feed._reconnect_backfill_failed_pairs)
            now_ms = expected_open + 300_000
            confirmed = await feed.refresh_latest_closed_candle("B-BTC_USDT", now_ms=now_ms)
            self.assertEqual(confirmed, expected_open)
            self.assertEqual(calls, [320, 320, 6])
            self.assertNotIn("B-BTC_USDT", feed._reconnect_backfill_failed_pairs)
            await feed.close()

        asyncio.run(run())

    def test_failed_full_repair_keeps_pair_blocked_and_skips_short_refresh(self):
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        feed.fetch_history = AsyncMock(side_effect=RuntimeError("REST unavailable"))

        async def run():
            feed._reconnect_backfill_failed_pairs.add("B-BTC_USDT")
            with self.assertRaisesRegex(RuntimeError, "reconnect history remains incomplete"):
                await feed.refresh_latest_closed_candle(
                    "B-BTC_USDT", now_ms=1_800_000_300_000
                )
            feed.fetch_history.assert_awaited_once_with("B-BTC_USDT", limit=320)
            self.assertIn("B-BTC_USDT", feed._reconnect_backfill_failed_pairs)
            await feed.close()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
