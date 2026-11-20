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


    async def test_inline_button_callback_persists_feedback_for_configured_chat(self):
        journal = type("JournalStub", (), {})()
        journal.add_telegram_feedback = AsyncMock(return_value=501)
        update = {
            "callback_query": {
                "id": "callback-1",
                "data": "jfb|321|GOOD_ENTRY",
                "message": {"chat": {"id": 123}},
            }
        }
        old_settings = main.settings
        try:
            client_cm = AsyncMock()
            client = client_cm.__aenter__.return_value
            client.post = AsyncMock()
            with patch.dict("os.environ", {"TELEGRAM_WEBHOOK_SECRET": "test-secret"}), \
                 patch.object(main, "settings", replace(
                     old_settings, telegram_bot_token="token", telegram_chat_id="123"
                 )), \
                 patch.object(main, "get_journal", return_value=journal), \
                 patch("main.httpx.AsyncClient", return_value=client_cm):
                result = await main.telegram_webhook(FakeRequest(update))
            self.assertEqual(result["feedback_id"], 501)
            journal.add_telegram_feedback.assert_awaited_once_with(321, "GOOD_ENTRY")
            client.post.assert_awaited_once()
            self.assertIn("answerCallbackQuery", client.post.await_args.args[0])
        finally:
            main.settings = old_settings

    async def test_inline_button_callback_from_other_chat_is_ignored(self):
        journal = type("JournalStub", (), {})()
        journal.add_telegram_feedback = AsyncMock(return_value=501)
        update = {
            "callback_query": {
                "id": "callback-2",
                "data": "jfb|321|GOOD_ENTRY",
                "message": {"chat": {"id": 999}},
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
            journal.add_telegram_feedback.assert_not_awaited()
        finally:
            main.settings = old_settings

    async def test_render_startup_registers_callback_query_updates(self):
        old_settings = main.settings
        response = __import__("httpx").Response(200, json={"ok": True, "result": True})
        client_cm = AsyncMock()
        client = client_cm.__aenter__.return_value
        client.post = AsyncMock(return_value=response)
        try:
            with patch.dict("os.environ", {
                "TELEGRAM_WEBHOOK_SECRET": "test-secret",
                "RENDER_EXTERNAL_URL": "https://cryptobyance.onrender.com",
            }), patch.object(main, "settings", replace(
                old_settings, telegram_bot_token="token", telegram_chat_id="123"
            )), patch("main.httpx.AsyncClient", return_value=client_cm):
                self.assertTrue(await main.register_telegram_webhook())
            args = client.post.await_args
            self.assertEqual(args.args[0], "https://api.telegram.org/bottoken/setWebhook")
            self.assertEqual(args.kwargs["json"]["url"],
                             "https://cryptobyance.onrender.com/telegram/webhook")
            self.assertEqual(args.kwargs["json"]["allowed_updates"],
                             ["message", "callback_query"])
            self.assertFalse(args.kwargs["json"]["drop_pending_updates"])
            self.assertTrue(main.state["telegram_webhook_registered"])
        finally:
            main.settings = old_settings


if __name__ == "__main__":
    unittest.main()
