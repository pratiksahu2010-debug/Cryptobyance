import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from main import telegram_outbox_loop


class OutboxConfigurationTests(unittest.IsolatedAsyncioTestCase):
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
