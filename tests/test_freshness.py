import asyncio
import unittest
from unittest.mock import patch

from main import scan_symbol
from strategy import validate_ohlcv


INTERVAL = 300_000


def candle_rows(count=80, start=1_800_000_000_000):
    start = (start // INTERVAL) * INTERVAL
    return [
        [start + i * INTERVAL, 100.0, 102.0, 99.0, 101.0, 10.0]
        for i in range(count)
    ]


class CandleFreshnessTests(unittest.TestCase):
    def test_live_freshness_limit_rejects_a_two_minute_old_close(self):
        rows = candle_rows()
        latest_close = rows[-1][0] + INTERVAL
        now_ms = latest_close + 120_000

        # Generic validator retains its existing interval-based default.
        self.assertFalse(validate_ohlcv(rows, "5m", now_ms).empty)
        with self.assertRaisesRegex(ValueError, "stale 5m candles"):
            validate_ohlcv(rows, "5m", now_ms, max_age_ms=90_000)

    def test_negative_freshness_limit_is_rejected(self):
        rows = candle_rows()
        with self.assertRaisesRegex(ValueError, "max_age_ms"):
            validate_ohlcv(rows, "5m", rows[-1][0] + INTERVAL, max_age_ms=-1)


class ScannerFreshnessTests(unittest.TestCase):
    def test_stale_candle_is_refreshed_once_then_rejected_if_rest_is_still_stale(self):
        rows = candle_rows()
        now_ms = rows[-1][0] + INTERVAL + 120_000

        class FakeFeed:
            def __init__(self):
                self.refresh_limits = []

            def rows(self, pair):
                return rows

            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)

        feed = FakeFeed()
        with patch("main.evaluate_setup") as evaluate:
            asyncio.run(scan_symbol(
                feed, object(), asyncio.Semaphore(1), "FRESHNESS_TEST/USDT",
                "B-FRESHNESS_TEST_USDT", now_ms,
            ))

        self.assertEqual(feed.refresh_limits, [6])
        evaluate.assert_not_called()

    def test_stale_cache_refreshes_and_uses_new_completed_rest_candle(self):
        rows = candle_rows()
        stale_now = rows[-1][0] + INTERVAL + 120_000
        fresh_rows = rows + [[rows[-1][0] + INTERVAL, 101.0, 103.0, 100.0, 102.0, 12.0]]
        now_ms = fresh_rows[-1][0] + INTERVAL + 30_000

        class FakeFeed:
            def __init__(self):
                self.cache = rows
                self.refresh_limits = []

            def rows(self, pair):
                return self.cache

            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)
                self.cache = fresh_rows

        feed = FakeFeed()
        with patch("main.evaluate_setup", return_value=None) as evaluate:
            asyncio.run(scan_symbol(
                feed, object(), asyncio.Semaphore(1), "FRESHNESS_FRESH/USDT",
                "B-FRESHNESS_FRESH_USDT", now_ms,
            ))

        self.assertEqual(feed.refresh_limits, [6])
        evaluate.assert_called_once()


if __name__ == "__main__":
    unittest.main()
