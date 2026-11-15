import unittest
import numpy as np
import pandas as pd

from coindcx_feed import (
    aggregate_5m_to_15m, candle_to_row, parse_active_instruments, symbol_to_pair,
)
from main import SYMBOLS, scan_symbol, last_seen_candle, state
from strategy import candles_to_frame, indicators, evaluate_signal, validate_ohlcv


class CoinDCXFeedTests(unittest.TestCase):
    def test_configured_symbol_maps_to_coin_dcx_pair(self):
        self.assertEqual(symbol_to_pair("BTC/USDT"), "B-BTC_USDT")
        self.assertEqual(symbol_to_pair("1000BONK/USDT"), "B-1000BONK_USDT")

    def test_active_instruments_response_normalization(self):
        self.assertEqual(
            parse_active_instruments(["B-BTC_USDT", "B-ETH_USDT"]),
            {"B-BTC_USDT", "B-ETH_USDT"},
        )
        self.assertEqual(
            parse_active_instruments({"data": [{"pair": "B-BTC_USDT"}]}),
            {"B-BTC_USDT"},
        )

    def test_feed_configures_only_confirmed_active_pairs(self):
        from coindcx_feed import CoinDCXFuturesFeed
        feed = CoinDCXFuturesFeed(["B-BTC_USDT", "B-ETH_USDT", "B-NOTLISTED_USDT"])
        try:
            feed.configure_active_pairs(["B-BTC_USDT", "B-ETH_USDT"])
            self.assertEqual(feed.pairs, ["B-BTC_USDT", "B-ETH_USDT"])
            self.assertEqual(set(feed.bars), {"B-BTC_USDT", "B-ETH_USDT"})
        finally:
            import asyncio
            asyncio.run(feed.close())

    def test_active_instruments_rejects_empty_or_invalid_payload(self):
        with self.assertRaises(ValueError):
            parse_active_instruments([])
        with self.assertRaises(ValueError):
            parse_active_instruments({"error": "bad response"})

    def test_rest_and_socket_candle_timestamp_units(self):
        rest = candle_to_row({"time": 1_800_000_000_000, "open": "1", "high": "2",
                              "low": ".5", "close": "1.5", "volume": "10"})
        socket = candle_to_row({"open_time": 1_800_000_000, "open": "1", "high": "2",
                                "low": ".5", "close": "1.5", "volume": "10"})
        self.assertEqual(rest, socket)

    def test_futures_socket_candle_event_is_parsed_and_counted(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed
        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        try:
            payload = {"channel": "B-BTC_USDT_5m-futures", "data": [{
                "pair": "B-BTC_USDT", "open_time": 1_800_000_000,
                "open": "100", "high": "102", "low": "99",
                "close": "101", "volume": "12",
            }]}
            asyncio.run(feed._on_candlestick(payload))
            self.assertEqual(feed.ws_events_received, 1)
            self.assertEqual(feed.last_ws_pair, "B-BTC_USDT")
            self.assertEqual(len(feed.rows("B-BTC_USDT")), 1)
        finally:
            asyncio.run(feed.close())

    def test_15m_aggregation_requires_three_complete_contiguous_bars(self):
        base = (1_800_000_000_000 // 300_000) * 300_000
        rows = [
            [base, 10, 12, 9, 11, 100],
            [base + 300_000, 11, 13, 10, 12, 200],
            [base + 600_000, 12, 14, 11, 13, 300],
            [base + 900_000, 13, 15, 12, 14, 400],
        ]
        result = aggregate_5m_to_15m(rows, base + 1_800_000)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], [base, 10.0, 14.0, 9.0, 13.0, 600.0])

    def test_15m_aggregation_drops_incomplete_group(self):
        base = (1_800_000_000_000 // 300_000) * 300_000
        rows = [[base + i * 300_000, 10, 12, 9, 11, 10] for i in (0, 1, 3)]
        self.assertEqual(aggregate_5m_to_15m(rows, base + 2_000_000), [])

    def test_universe_has_exactly_60_unique_symbols(self):
        self.assertEqual(len(SYMBOLS), 60)
        self.assertEqual(len(set(SYMBOLS)), 60)


class EngineTests(unittest.TestCase):
    def test_indicators_produce_expected_columns(self):
        n = 400
        close = np.linspace(100, 150, n) + np.sin(np.arange(n) / 5)
        df = pd.DataFrame({
            "timestamp": np.arange(n) * 300000 + 1700000000000,
            "open": close - .2, "high": close + .5, "low": close - .5,
            "close": close, "volume": np.full(n, 100.0),
        })
        result = indicators(df)
        for col in ("ema9", "ema21", "rsi", "adx", "atr", "vwap"):
            self.assertIn(col, result.columns)
        self.assertTrue(np.isfinite(result.iloc[-1]["adx"]))

    def test_candle_parser(self):
        raw = [[1700000000000, "1", "2", ".5", "1.5", "10"]]
        frame = candles_to_frame(raw)
        self.assertEqual(float(frame.iloc[0].close), 1.5)
        self.assertEqual(float(frame.iloc[0].volume), 10)

    def test_validator_sorts_deduplicates_and_excludes_forming_bar(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = []
        for i in range(5):
            price = 100 + i
            raw.append([base + i * interval, price, price + 2, price - 1, price + 1, 10])
        raw = list(reversed(raw))
        raw.append([base + interval, 101, 103, 100, 102, 25])
        now_ms = base + 4 * interval + 2000
        frame = validate_ohlcv(raw, "5m", now_ms)
        self.assertEqual(len(frame), 4)
        self.assertEqual(frame.timestamp.tolist(), sorted(frame.timestamp.tolist()))
        self.assertEqual(float(frame.iloc[1].volume), 25)
        self.assertEqual(int(frame.iloc[-1].timestamp), base + 3 * interval)

    def test_validator_rejects_stale_candles(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 102, 99, 101, 10] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "stale"):
            validate_ohlcv(raw, "5m", base + 8 * interval)

    def test_validator_rejects_recent_missing_interval(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 102, 99, 101, 10] for i in (0, 1, 3, 4)]
        with self.assertRaisesRegex(ValueError, "missing interval"):
            validate_ohlcv(raw, "5m", base + 5 * interval + 2000)

    def test_validator_rejects_inconsistent_ohlc(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 99, 98, 101, 10] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "inconsistent high"):
            validate_ohlcv(raw, "5m", base + 4 * interval + 2000)

    def _trend_frame(self, direction: int, interval_ms: int) -> pd.DataFrame:
        n = 400
        base = np.linspace(100, 200, n) if direction > 0 else np.linspace(200, 100, n)
        return pd.DataFrame({
            "timestamp": 1700000000000 + np.arange(n) * interval_ms,
            "open": base - direction * .1, "high": base + .5,
            "low": base - .5, "close": base, "volume": np.full(n, 100.0),
        })

    def test_failed_telegram_delivery_is_not_retried_for_same_candle(self):
        import asyncio

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = []
        for i in range(200):
            price = 100 + i * 0.1
            rows.append([base + i * interval, price, price + 0.5, price - 0.5, price, 100])
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def rows(self, pair):
                return rows

        class FakeNotifier:
            def __init__(self):
                self.calls = 0

            async def send(self, signal):
                self.calls += 1
                return False

        signal = {"side": "LONG", "entry": 120.0, "stop": 118.0, "target": 123.0,
                  "rr": 1.5, "rsi": 60.0, "adx": 25.0, "context": "15m bullish",
                  "candle": rows[-1][0]}

        last_seen_candle.pop("BTC/USDT", None)
        notifier = FakeNotifier()
        failures_before = state['telegram_delivery_failures']
        try:
            from unittest.mock import patch
            with patch("main.evaluate_signal", return_value=signal):
                async def run_twice():
                    semaphore = asyncio.Semaphore(1)
                    await scan_symbol(FakeFeed(), notifier, semaphore, "BTC/USDT", "B-BTC_USDT", now_ms)
                    await scan_symbol(FakeFeed(), notifier, semaphore, "BTC/USDT", "B-BTC_USDT", now_ms)
                asyncio.run(run_twice())
            self.assertEqual(notifier.calls, 1)
            self.assertEqual(state['telegram_delivery_failures'], failures_before + 1)
        finally:
            last_seen_candle.pop("BTC/USDT", None)

    def test_scanner_skips_signal_from_candle_closed_before_session_start(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = []
        for i in range(200):
            price = 100 + i * 0.1
            rows.append([base + i * interval, price, price + 0.5, price - 0.5, price, 100])
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def rows(self, pair):
                return rows

        class FakeNotifier:
            def __init__(self):
                self.calls = 0

            async def send(self, signal):
                self.calls += 1
                return True

        signal = {"side": "LONG", "entry": 120.0, "stop": 118.0, "target": 123.0,
                  "rr": 1.5, "rsi": 60.0, "adx": 25.0, "context": "15m bullish",
                  "candle": rows[-1][0]}
        last_seen_candle.pop("BTC/USDT", None)
        notifier = FakeNotifier()
        try:
            with patch("main.evaluate_signal", return_value=signal):
                asyncio.run(scan_symbol(
                    FakeFeed(), notifier, asyncio.Semaphore(1), "BTC/USDT",
                    "B-BTC_USDT", now_ms, session_started_ms=now_ms - 1_000,
                ))
            self.assertEqual(notifier.calls, 0)
        finally:
            last_seen_candle.pop("BTC/USDT", None)

    def test_strict_engine_emits_long_for_aligned_uptrend(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertGreaterEqual(signal["adx"], 17)

    def test_signal_stop_and_target_preserve_configured_risk_reward(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        risk = signal["entry"] - signal["stop"]
        reward = signal["target"] - signal["entry"]
        self.assertAlmostEqual(reward / risk, 1.5, places=8)

    def test_15m_opposing_context_blocks_long_signal(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNone(signal)

    def test_15m_opposing_context_blocks_short_signal(self):
        signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(1, 900000))
        self.assertIsNone(signal)

    def test_strict_engine_emits_short_for_aligned_downtrend(self):
        signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")
        self.assertGreaterEqual(signal["adx"], 17)


if __name__ == "__main__":
    unittest.main()
