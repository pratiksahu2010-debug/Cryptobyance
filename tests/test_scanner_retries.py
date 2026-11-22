import asyncio
import unittest

import httpx

import main


class ScannerRetryTests(unittest.TestCase):
    def test_transient_transport_error_is_retryable(self):
        request = httpx.Request("GET", "https://api.coindcx.com")
        self.assertTrue(main._is_retryable_scan_error(httpx.ConnectError("temporary", request=request)))

    def test_rate_limit_and_server_errors_are_retryable(self):
        request = httpx.Request("GET", "https://api.coindcx.com")
        for status in (408, 425, 429, 500, 502, 503):
            response = httpx.Response(status, request=request)
            self.assertTrue(main._is_retryable_scan_error(httpx.HTTPStatusError("temporary", request=request, response=response)))

    def test_client_error_and_invalid_data_are_not_retryable(self):
        request = httpx.Request("GET", "https://api.coindcx.com")
        response = httpx.Response(400, request=request)
        self.assertFalse(main._is_retryable_scan_error(httpx.HTTPStatusError("bad request", request=request, response=response)))
        self.assertFalse(main._is_retryable_scan_error(ValueError("invalid OHLCV")))
        self.assertFalse(main._is_retryable_scan_error(TypeError("bad type")))

    def test_retry_targets_include_only_explicit_retryable_symbols(self):
        targets = [("BTC/USDT", "B-BTC_USDT"), ("ETH/USDT", "B-ETH_USDT"), ("SOL/USDT", "B-SOL_USDT")]
        results = [None, "RETRYABLE", "PERMANENT_FAILURE"]
        self.assertEqual(main.retryable_scan_targets(targets, results), [targets[1]])

    def test_scan_symbol_returns_retryable_for_transient_feed_failure(self):
        class BrokenFeed:
            async def refresh_latest_closed_candle(self, pair, now_ms):
                raise httpx.ConnectError("temporary network failure")

            def rows(self, pair):
                return []

        result = asyncio.run(main.scan_symbol(
            BrokenFeed(), object(), asyncio.Semaphore(1),
            "RETRY_TEST/USDT", "B-RETRY_TEST_USDT", 1_900_000_000_000,
        ))
        self.assertEqual(result, "RETRYABLE")

    def test_scan_symbol_does_not_retry_invalid_market_data(self):
        class BrokenFeed:
            async def refresh_latest_closed_candle(self, pair, now_ms):
                raise ValueError("invalid candle timestamp")

            def rows(self, pair):
                return []

        result = asyncio.run(main.scan_symbol(
            BrokenFeed(), object(), asyncio.Semaphore(1),
            "INVALID_TEST/USDT", "B-INVALID_TEST_USDT", 1_900_000_000_000,
        ))
        self.assertEqual(result, "PERMANENT_FAILURE")


if __name__ == "__main__":
    unittest.main()
