import unittest

import pandas as pd

from strategy import has_contiguous_tail, validate_ohlcv
from coindcx_feed import CoinDCXFuturesFeed, deduplicate_rest_candles


class ContiguousIndicatorWindowTests(unittest.TestCase):
    def test_accepts_exactly_contiguous_five_minute_tail(self):
        frame = pd.DataFrame({"timestamp": [i * 300_000 for i in range(60)]})
        self.assertTrue(has_contiguous_tail(frame, "5m", 60))

    def test_rejects_missing_candle_inside_indicator_window(self):
        stamps = [i * 300_000 for i in range(60)]
        stamps[30] += 300_000
        frame = pd.DataFrame({"timestamp": stamps})
        self.assertFalse(has_contiguous_tail(frame, "5m", 60))

    def test_ignores_gaps_older_than_required_indicator_tail(self):
        stamps = [i * 300_000 for i in range(61)]
        stamps[0] += 300_000
        frame = pd.DataFrame({"timestamp": stamps})
        self.assertTrue(has_contiguous_tail(frame, "5m", 60))

    def test_requires_full_fifteen_minute_context_window(self):
        frame = pd.DataFrame({"timestamp": [i * 900_000 for i in range(34)]})
        self.assertFalse(has_contiguous_tail(frame, "15m", 35))
        frame = pd.DataFrame({"timestamp": [i * 900_000 for i in range(35)]})
        self.assertTrue(has_contiguous_tail(frame, "15m", 35))


    def test_conflicting_duplicate_timestamp_is_rejected(self):
        start = 1_800_000_000_000
        rows = [
            [start + i * 300_000, 100.0, 102.0, 99.0, 101.0, 10.0]
            for i in range(5)
        ]
        rows.append([rows[-1][0], 100.0, 104.0, 99.0, 103.0, 12.0])
        with self.assertRaisesRegex(ValueError, "conflicting duplicate timestamps"):
            validate_ohlcv(rows, "5m", now_ms=rows[-1][0] + 300_000 + 5_000)

    def test_identical_duplicate_timestamp_is_collapsed_safely(self):
        start = 1_800_000_000_000
        row = [start, 100.0, 102.0, 99.0, 101.0, 10.0]
        result = deduplicate_rest_candles([row, row.copy()])
        self.assertEqual(result, [row])

    def test_conflicting_rest_duplicate_timestamp_is_rejected(self):
        row = [1_800_000_000_000, 100.0, 102.0, 99.0, 101.0, 10.0]
        conflicting = [1_800_000_000_000, 100.0, 104.0, 99.0, 103.0, 12.0]
        with self.assertRaisesRegex(ValueError, "conflicting CoinDCX REST candles"):
            deduplicate_rest_candles([row, conflicting])


if __name__ == "__main__":
    unittest.main()


class RestConfirmedCandleTests(unittest.TestCase):
    def test_confirms_newly_closed_candle_once_per_interval(self):
        import asyncio
        from unittest.mock import AsyncMock

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        now_ms = 1_800_000_000_000
        expected_open = (now_ms // 300_000) * 300_000 - 300_000
        fetch = AsyncMock(return_value=[
            [expected_open, 100.0, 102.0, 99.0, 101.0, 10.0],
        ])
        feed.fetch_history = fetch
        try:
            first = asyncio.run(feed.refresh_latest_closed_candle("B-BTC_USDT", now_ms))
            second = asyncio.run(feed.refresh_latest_closed_candle("B-BTC_USDT", now_ms + 1_000))
            self.assertEqual(first, expected_open)
            self.assertEqual(second, expected_open)
            fetch.assert_awaited_once_with("B-BTC_USDT", limit=6)
        finally:
            asyncio.run(feed.close())

    def test_does_not_mark_interval_confirmed_when_rest_omits_latest_closed_bar(self):
        import asyncio
        from unittest.mock import AsyncMock

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        now_ms = 1_800_000_000_000
        expected_open = (now_ms // 300_000) * 300_000 - 300_000
        feed.fetch_history = AsyncMock(return_value=[
            [expected_open - 300_000, 100.0, 102.0, 99.0, 101.0, 10.0],
        ])
        try:
            with self.assertRaisesRegex(ValueError, "did not confirm latest completed"):
                asyncio.run(feed.refresh_latest_closed_candle("B-BTC_USDT", now_ms))
            self.assertNotIn(expected_open, feed._rest_confirmed_closed_opens.get("B-BTC_USDT", set()))
        finally:
            asyncio.run(feed.close())
