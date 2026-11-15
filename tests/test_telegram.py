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

    @patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "123:secret", "TELEGRAM_CHAT_ID": "456"})
    async def test_retries_rate_limit_then_succeeds(self):
        client = FakeClient([
            httpx.Response(429, json={"ok": False, "parameters": {"retry_after": 2}}),
            httpx.Response(200, json={"ok": True}),
        ])
        notifier = TelegramNotifier(client)
        with patch("telegram.asyncio.sleep", new_callable=AsyncMock) as sleep:
            self.assertTrue(await notifier.send(self._signal()))
        self.assertEqual(client.calls, 2)
        sleep.assert_awaited_once_with(2.0)

    @patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "123:secret", "TELEGRAM_CHAT_ID": "456"})
    async def test_does_not_retry_permanent_telegram_rejection(self):
        client = FakeClient([httpx.Response(400, json={"ok": False, "description": "bad request"})])
        notifier = TelegramNotifier(client)
        self.assertFalse(await notifier.send(self._signal()))
        self.assertEqual(client.calls, 1)

    @patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "123:secret", "TELEGRAM_CHAT_ID": "456"})
    async def test_network_error_does_not_log_bot_token(self):
        token = "123:secret"
        client = FakeClient([
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
            httpx.ConnectError(f"failed https://api.telegram.org/bot{token}/sendMessage"),
        ])
        notifier = TelegramNotifier(client)
        with self.assertLogs("cryptobyance.telegram", level=logging.ERROR) as captured:
            with patch("telegram.asyncio.sleep", new_callable=AsyncMock):
                self.assertFalse(await notifier.send(self._signal()))
        self.assertNotIn(token, captured.output.__str__())
        self.assertEqual(client.calls, 3)

    @patch.dict("os.environ", {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""})
    async def test_missing_credentials_are_reported_without_network_request(self):
        client = FakeClient([])
        notifier = TelegramNotifier(client)
        with self.assertLogs("cryptobyance.telegram", level=logging.ERROR):
            self.assertFalse(await notifier.send(self._signal()))
        self.assertFalse(notifier.configured)
        self.assertEqual(client.calls, 0)


if __name__ == "__main__":
    unittest.main()
