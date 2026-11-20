import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, patch

import main


class FakeRequest:
    def __init__(self, update):
        self.headers = {"x-telegram-bot-api-secret-token": "test-secret"}
        self.update = update

    async def json(self):
        return self.update


class TelegramReplyNoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_reply_to_bot_signal_saves_note_without_grading_signal(self):
        journal = type("JournalStub", (), {})()
        journal.add_feedback = AsyncMock(return_value=88)
        update = {
            "message": {
                "chat": {"id": 123},
                "text": "The entry was a little late; momentum was already fading.",
                "reply_to_message": {
                    "from": {"is_bot": True},
                    "text": "🟢 LONG SIGNAL\nBTC/USDT\nSignal ID: 321",
                },
            }
        }
        old_settings = main.settings
        try:
            with patch.dict("os.environ", {"TELEGRAM_WEBHOOK_SECRET": "test-secret"}), \
                 patch.object(main, "settings", replace(
                     old_settings, telegram_bot_token="token", telegram_chat_id="123"
                 )), \
                 patch.object(main, "get_journal", return_value=journal), \
                 patch.object(main, "_telegram_send_message", new_callable=AsyncMock) as send:
                result = await main.telegram_webhook(FakeRequest(update))
            self.assertTrue(result["note_saved"])
            self.assertEqual(result["signal_id"], 321)
            journal.add_feedback.assert_awaited_once_with(
                321, "NEEDS_REVIEW",
                "The entry was a little late; momentum was already fading.",
                "telegram",
            )
            send.assert_awaited_once()
        finally:
            main.settings = old_settings

    async def test_non_bot_reply_does_not_attach_note_to_signal(self):
        journal = type("JournalStub", (), {})()
        journal.add_feedback = AsyncMock(return_value=88)
        update = {
            "message": {
                "chat": {"id": 123}, "text": "note",
                "reply_to_message": {
                    "from": {"is_bot": False}, "text": "Signal ID: 321",
                },
            }
        }
        old_settings = main.settings
        try:
            with patch.dict("os.environ", {"TELEGRAM_WEBHOOK_SECRET": "test-secret"}), \
                 patch.object(main, "settings", replace(
                     old_settings, telegram_bot_token="token", telegram_chat_id="123"
                 )), \
                 patch.object(main, "get_journal", return_value=journal):
                result = await main.telegram_webhook(FakeRequest(update))
            self.assertTrue(result["ignored"])
            journal.add_feedback.assert_not_awaited()
        finally:
            main.settings = old_settings


if __name__ == "__main__":
    unittest.main()
