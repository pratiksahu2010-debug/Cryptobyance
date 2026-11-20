import unittest
from unittest.mock import patch

from config import Settings


class SettingsTests(unittest.TestCase):
    def test_defaults_preserve_single_engine_timeframes_and_threshold(self):
        with patch.dict("os.environ", {}, clear=True):
            settings = Settings.from_env()
        self.assertEqual(settings.timeframe, "5m")
        self.assertEqual(settings.context_timeframe, "15m")
        self.assertEqual(settings.adx_min, 20)
        self.assertEqual(settings.signal_cooldown_candles, 4)
        self.assertEqual(settings.ema_fast, 9)
        self.assertEqual(settings.ema_slow, 21)
        self.assertEqual(settings.outcome_horizon_bars, 12)
        self.assertEqual(settings.outcome_cost_pct, 0.10)

    def test_rejects_unsupported_timeframes(self):
        with patch.dict("os.environ", {"TIMEFRAME": "1m"}, clear=True):
            with self.assertRaisesRegex(ValueError, "requires TIMEFRAME=5m"):
                Settings.from_env()

    def test_rejects_invalid_concurrency(self):
        with patch.dict("os.environ", {"CONCURRENCY": "100"}, clear=True):
            with self.assertRaisesRegex(ValueError, "CONCURRENCY"):
                Settings.from_env()

    def test_rejects_non_finite_adx_threshold(self):
        with patch.dict("os.environ", {"ADX_MIN": "nan"}, clear=True):
            with self.assertRaisesRegex(ValueError, "ADX_MIN"):
                Settings.from_env()

    def test_rejects_invalid_outcome_horizon_and_cost(self):
        with patch.dict("os.environ", {"OUTCOME_HORIZON_BARS": "0"}, clear=True):
            with self.assertRaisesRegex(ValueError, "OUTCOME_HORIZON_BARS"):
                Settings.from_env()
        with patch.dict("os.environ", {"OUTCOME_COST_PCT": "-0.1"}, clear=True):
            with self.assertRaisesRegex(ValueError, "OUTCOME_COST_PCT"):
                Settings.from_env()

    def test_secrets_are_not_in_settings_repr(self):
        with patch.dict("os.environ", {
            "TELEGRAM_BOT_TOKEN": "private-token",
            "TELEGRAM_CHAT_ID": "private-chat",
        }, clear=True):
            settings = Settings.from_env()
        self.assertNotIn("private-token", repr(settings))
        self.assertNotIn("private-chat", repr(settings))


if __name__ == "__main__":
    unittest.main()
