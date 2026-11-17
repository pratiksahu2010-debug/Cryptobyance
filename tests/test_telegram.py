import logging
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from telegram import TelegramNotifier


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    async def post(self, url, json, timeout):
        self.calls += 1
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
            "candle": 1_800_000_000_000,
        }

    def test_tradingview_url_uses_binance_usdt_perpetual_and_five_minute_interval(self):
        self.assertEqual(
            TelegramNotifier.tradingview_url("BTC/USDT"),
            "https://www.tradingview.com/chart/?symbol=BINANCE%3ABTCUSDT.P&interval=5",
        )
        self.assertEqual(
            TelegramNotifier.tradingview_url("1000BONK/USDT"),
            "https://www.tradingview.com/chart/?symbol=BINANCE%3A1000BONKUSDT.P&interval=5",
        )

    def test_tradingview_url_rejects_invalid_or_non_usdt_symbols(self):
        self.assertEqual(TelegramNotifier.tradingview_url(""), "")
        self.assertEqual(TelegramNotifier.tradingview_url("BTC/USD"), "")
        self.assertEqual(TelegramNotifier.tradingview_url("BTC/USDT<script>"), "")

    def test_formatted_signal_contains_clickable_tradingview_chart_link(self):
        message = TelegramNotifier.format_message(self._signal())
        self.assertIn(
            '<a href="https://www.tradingview.com/chart/?symbol=BINANCE%3ABTCUSDT.P&amp;interval=5">',
            message,
        )
        self.assertIn("OPEN BINANCE PERPETUAL CHART · 5m", message)
        self.assertIn("CoinDCX USDT Perpetual", message)
        self.assertIn("EMA 9/21: fresh crossover confirmed", message)
        self.assertIn("15m context:", message)
        self.assertIn("informational only; never a veto", message)
        self.assertIn("15m bullish", message)
        self.assertIn("5m candle close:", message)
        self.assertNotIn("Signal generated:", message)
        self.assertEqual(message.count("UTC"), 1)

    async def test_retries_rate_limit_then_succeeds(self):
        client = FakeClient([
            httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 2}}),
            httpx.Response(200, json={"ok": True}),
        ])
        notifier = TelegramNotifier(client, "123:secret", "456")
        with patch("telegram.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertTrue(await notifier.send(self._signal()))
        self.assertEqual(client.calls, 2)
        sleep.assert_awaited_once_with(2.0)

    async def test_does_not_retry_permanent_telegram_rejection(self):
        client = FakeClient([httpx.Response(400, json={"ok": False, "description": "bad request"})])
        notifier = TelegramNotifier(client, "123:secret", "456")
        self.assertFalse(await notifier.send(self._signal()))
        self.assertEqual(client.calls, 1)

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
