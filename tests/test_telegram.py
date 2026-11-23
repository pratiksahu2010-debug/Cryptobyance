import logging
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from telegram import TelegramNotifier


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0
        self.requests = []

    async def post(self, url, json, timeout):
        self.calls += 1
        self.requests.append({"url": url, "json": json, "timeout": timeout})
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class TelegramNotifierTests(unittest.IsolatedAsyncioTestCase):
    def _signal(self):
        return {
            "side": "LONG", "symbol": "BTC/USDT", "entry": 100.0,
            "stop": 98.0, "target": 103.0, "rr": 1.5,
            "rsi": 60.0, "adx": 25.0, "context": "15m bullish",
            "ema9": 101.0, "ema21": 100.0, "vwap": 100.0,
            "candle": 1_800_000_000_000,
        }

    def test_tradingview_url_opens_symbol_market_directory_without_wrong_exchange_assumption(self):
        self.assertEqual(
            TelegramNotifier.tradingview_url("BTC/USDT"),
            "https://www.tradingview.com/symbols/BTCUSDT/markets/",
        )
        self.assertEqual(
            TelegramNotifier.tradingview_url("1000BONK/USDT"),
            "https://www.tradingview.com/symbols/1000BONKUSDT/markets/",
        )
        self.assertNotIn("BINANCE", TelegramNotifier.tradingview_url("BTC/USDT"))

    def test_tradingview_url_rejects_invalid_or_non_usdt_symbols(self):
        self.assertEqual(TelegramNotifier.tradingview_url(""), "")
        self.assertEqual(TelegramNotifier.tradingview_url("BTC/USD"), "")
        self.assertEqual(TelegramNotifier.tradingview_url("BTC/USDT<script>"), "")

    def test_formatted_signal_contains_clickable_tradingview_chart_link(self):
        message = TelegramNotifier.format_message(self._signal())
        self.assertIn(
            '<a href="https://www.tradingview.com/symbols/BTCUSDT/markets/">',
            message,
        )
        self.assertIn("OPEN TRADINGVIEW MARKET LIST · SELECT EXCHANGE", message)
        self.assertIn("CoinDCX USDT Perpetual", message)
        self.assertIn("EMA 9/21:", message)
        self.assertIn("15m context:", message)
        self.assertIn("informational only; not used to approve or block 5m signals", message)
        self.assertIn("15m bullish", message)
        self.assertNotIn("mandatory direction confirmation", message)
        self.assertNotIn("15m RSI", message)
        self.assertNotIn("\\n", message)
        self.assertIn("</code></b>\n<b>VWAP:", message)
        self.assertIn("</code></b>\n<b>Stop loss (swing):", message)
        self.assertIn("5m candle close:", message)
        self.assertNotIn("Signal generated:", message)
        self.assertEqual(message.count("UTC"), 1)

    async def test_feedback_keyboard_contains_good_bad_late_fake_skip(self):
        client = FakeClient([httpx.Response(200, json={"ok": True})])
        notifier = TelegramNotifier(client, "123:secret", "456")
        self.assertTrue(await notifier.send(self._signal(), signal_id=77))
        keyboard = client.requests[0]["json"]["reply_markup"]["inline_keyboard"]
        labels = [button["callback_data"].split("|")[2] for row in keyboard for button in row]
        self.assertEqual(labels, ["GOOD_ENTRY", "BAD_SIGNAL", "LATE_ENTRY", "FAKE_SIGNAL", "SKIP"])

    async def test_rate_limit_returns_retry_after_to_durable_outbox(self):
        client = FakeClient([
            httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 120}}),
        ])
        notifier = TelegramNotifier(client, "123:secret", "456")
        result = await notifier.send(self._signal())
        self.assertFalse(result)
        self.assertTrue(result.retryable)
        self.assertEqual(result.error_type, "HTTP_429")
        self.assertEqual(result.retry_after, 120.0)
        self.assertEqual(client.calls, 1)

    async def test_does_not_retry_permanent_telegram_rejection(self):
        client = FakeClient([httpx.Response(400, json={"ok": False, "description": "bad request"})])
        notifier = TelegramNotifier(client, "123:secret", "456")
        result = await notifier.send(self._signal())
        self.assertFalse(result)
        self.assertFalse(result.retryable)
        self.assertEqual(result.error_type, "HTTP_400")
        self.assertEqual(result.http_status, 400)
        self.assertEqual(client.calls, 1)

    async def test_server_error_is_classified_as_retryable_after_bounded_attempts(self):
        client = FakeClient([
            httpx.Response(503, json={"ok": False}),
            httpx.Response(503, json={"ok": False}),
            httpx.Response(503, json={"ok": False}),
        ])
        notifier = TelegramNotifier(client, "123:secret", "456")
        with patch("telegram.asyncio.sleep", new_callable=AsyncMock):
            result = await notifier.send(self._signal())
        self.assertFalse(result)
        self.assertTrue(result.retryable)
        self.assertEqual(result.error_type, "HTTP_503")
        self.assertEqual(result.http_status, 503)
        self.assertEqual(client.calls, 3)

    async def test_network_error_does_not_log_bot_token(self):
        token = "123:secret"
        client = FakeClient([
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
        ])
        notifier = TelegramNotifier(client, "123:secret", "456")
        with self.assertLogs("cryptobyance.telegram", level=logging.ERROR) as captured:
            with patch("telegram.asyncio.sleep", new_callable=AsyncMock):
                self.assertFalse(await notifier.send(self._signal()))
        self.assertNotIn(token, "\n".join(captured.output))
        self.assertEqual(client.calls, 3)

    async def test_missing_credentials_are_reported_without_network_request(self):
        client = FakeClient([])
        notifier = TelegramNotifier(client, "", "")
        with self.assertLogs("cryptobyance.telegram", level=logging.ERROR):
            self.assertFalse(await notifier.send(self._signal()))
        self.assertFalse(notifier.configured)
        self.assertEqual(client.calls, 0)


if __name__ == "__main__":
    unittest.main()
