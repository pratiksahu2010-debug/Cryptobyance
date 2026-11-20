import unittest

import candidate_store
import config
import journal
import main
import strategy


class RuntimeConfigReloadTests(unittest.TestCase):
    def test_invalid_approved_config_is_rejected_before_hot_reload(self):
        old_settings = main.settings
        old_version = main.CONFIG_VERSION
        payload = main._runtime_config_payload(old_settings)
        payload["rsi_long_min"] = payload["rsi_long_max"]
        with self.assertRaisesRegex(ValueError, "invalid strategy or risk settings"):
            main.apply_runtime_config(payload, "v99")
        self.assertIs(main.settings, old_settings)
        self.assertEqual(main.CONFIG_VERSION, old_version)

    def test_approved_config_hot_reloads_strategy_and_scanner_globals(self):
        old_settings = main.settings
        old_version = main.CONFIG_VERSION
        old_payload = main._runtime_config_payload(old_settings)
        next_payload = dict(old_payload)
        next_payload.update({
            "adx_min": old_settings.adx_min + 2,
            "rsi_long_min": old_settings.rsi_long_min + 2,
            "signal_cooldown_candles": old_settings.signal_cooldown_candles + 1,
        })
        try:
            main.apply_runtime_config(next_payload, "v99")
            self.assertEqual(main.settings.adx_min, old_settings.adx_min + 2)
            self.assertEqual(strategy.settings.adx_min, old_settings.adx_min + 2)
            self.assertEqual(strategy.ADX_MIN, old_settings.adx_min + 2)
            self.assertEqual(journal.ADX_MIN, old_settings.adx_min + 2)
            self.assertEqual(candidate_store.settings.adx_min, old_settings.adx_min + 2)
            self.assertEqual(main.COOLDOWN_CANDLES, old_settings.signal_cooldown_candles + 1)
            self.assertEqual(main.CONFIG_VERSION, "v99")
            self.assertEqual(config.CONFIG_VERSION, "v99")
        finally:
            main.apply_runtime_config(old_payload, old_version)


if __name__ == "__main__":
    unittest.main()
