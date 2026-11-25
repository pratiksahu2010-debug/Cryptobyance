import unittest

from config import SYMBOLS
from coindcx_feed import symbol_to_pair


class CoinDCXUniverseTests(unittest.TestCase):
    def test_migrated_symbols_use_current_coin_dcx_contract_names(self):
        expected = {
            "1000FLOKI/USDT": "B-1000FLOKI_USDT",
            "SONIC/USDT": "B-SONIC_USDT",
            "SKY/USDT": "B-SKY_USDT",
            "1000PEPE/USDT": "B-1000PEPE_USDT",
            "1000SHIB/USDT": "B-1000SHIB_USDT",
        }
        configured = set(SYMBOLS)
        for symbol, pair in expected.items():
            with self.subTest(symbol=symbol):
                self.assertIn(symbol, configured)
                self.assertEqual(symbol_to_pair(symbol), pair)

    def test_configured_universe_remains_unique_and_sized_at_60(self):
        self.assertEqual(len(SYMBOLS), 59)
        self.assertEqual(len(set(SYMBOLS)), 60)


if __name__ == "__main__":
    unittest.main()
