import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import main
from main import telegram_outbox_loop
from telegram import DeliveryResult


class OutboxConfigurationTests(unittest.IsolatedAsyncioTestCase):
    async def test_permanent_telegram_rejection_dead_letters_without_retry(self):
        class Store:
            def __init__(self):
                self.claims = 0
                self.fail_outbox = AsyncMock()
                self.retry_outbox = AsyncMock()
                self.finish_outbox = AsyncMock()

            async def claim_outbox(self):
                self.claims += 1
                if self.claims == 1:
                    return {"id": 4, "signal_id": 9, "payload": {}, "attempts": 1}
                raise asyncio.CancelledError

        class Notifier:
            configured = True

            async def send(self, payload, signal_id=None):
                return DeliveryResult(False, False, "HTTP_400", 400)

        store = Store()
        with patch("main.state", {"telegram_delivery_failures": 0}):
            with self.assertRaises(asyncio.CancelledError):
                await telegram_outbox_loop(store, Notifier())
        store.fail_outbox.assert_awaited_once_with(4, "HTTP_400")
        store.retry_outbox.assert_not_awaited()

    async def test_transient_telegram_failure_uses_durable_retry(self):
        class Store:
            def __init__(self):
                self.claims = 0
                self.fail_outbox = AsyncMock()
                self.retry_outbox = AsyncMock()
                self.finish_outbox = AsyncMock()

            async def claim_outbox(self):
                self.claims += 1
                if self.claims == 1:
                    return {"id": 5, "signal_id": 10, "payload": {}, "attempts": 2}
                raise asyncio.CancelledError

        class Notifier:
            configured = True

            async def send(self, payload, signal_id=None):
                return DeliveryResult(False, True, "HTTP_429", 429, 30.0)

        store = Store()
        with patch("main.state", {"telegram_delivery_failures": 0}):
            with self.assertRaises(asyncio.CancelledError):
                await telegram_outbox_loop(store, Notifier())
        store.retry_outbox.assert_awaited_once_with(
            5, 2, "HTTP_429", retry_after=30.0
        )
        store.fail_outbox.assert_not_awaited()

    async def test_missing_telegram_configuration_does_not_claim_or_burn_attempts(self):
        class Store:
            claim_called = False

            async def claim_outbox(self):
                self.claim_called = True
                return None

        class Notifier:
            configured = False

        store = Store()
        with patch("main.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await telegram_outbox_loop(store, Notifier())

        self.assertFalse(store.claim_called)


if __name__ == "__main__":
    unittest.main()
