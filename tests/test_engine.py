import unittest
import numpy as np
import pandas as pd

from coindcx_feed import (
    aggregate_5m_to_15m, candle_to_row, parse_active_instruments, symbol_to_pair,
)
from main import SYMBOLS, scan_symbol, last_seen_candle, state, build_health_payload, ping
from strategy import candles_to_frame, indicators, evaluate_signal, evaluate_setup, validate_ohlcv
from telegram import TelegramNotifier


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

    def test_nested_coindcx_socket_envelope_is_decoded_and_counted(self):
        import asyncio
        import json
        from coindcx_feed import CoinDCXFuturesFeed

        feed = CoinDCXFuturesFeed(["B-ARB_USDT"])
        try:
            inner = {
                "data": [{
                    "pair": "B-ARB_USDT", "open_time": 1_800_000_000,
                    "open": "1.0", "high": "1.2", "low": "0.9",
                    "close": "1.1", "volume": "42",
                }],
                "ecode": "B",
            }
            payload = {"event": "candlestick", "data": json.dumps(inner)}
            asyncio.run(feed._on_candlestick(payload))
            self.assertEqual(feed.ws_events_received, 1)
            self.assertEqual(feed.last_ws_pair, "B-ARB_USDT")
            self.assertEqual(len(feed.rows("B-ARB_USDT")), 1)
            self.assertEqual(feed.rows("B-ARB_USDT")[0][5], 42.0)
        finally:
            asyncio.run(feed.close())

    def test_websocket_ignores_malformed_nested_json(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        try:
            asyncio.run(feed._on_candlestick({
                "event": "candlestick", "data": "{not valid json",
            }))
            self.assertEqual(feed.ws_events_received, 0)
            self.assertEqual(feed.rows("B-BTC_USDT"), [])
        finally:
            asyncio.run(feed.close())

    def test_websocket_connect_subscribes_to_exact_futures_candle_channels(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed

        class FakeSocket:
            connected = False
            def __init__(self):
                self.emitted = []
            async def emit(self, event, payload):
                self.emitted.append((event, payload))

        feed = CoinDCXFuturesFeed(["B-BTC_USDT", "B-ETH_USDT"])
        fake = FakeSocket()
        feed.sio = fake
        try:
            async def connect_and_confirm_ready():
                await feed._on_connect()
                return await feed.wait_until_connected(timeout=0.01)
            self.assertTrue(asyncio.run(connect_and_confirm_ready()))
            self.assertTrue(feed.websocket_connected)
            self.assertEqual(fake.emitted, [
                ("join", {"channelName": "B-BTC_USDT_5m-futures"}),
                ("join", {"channelName": "B-ETH_USDT_5m-futures"}),
            ])
        finally:
            feed.websocket_connected = False
            asyncio.run(feed.close())

    def test_websocket_ignores_unconfigured_pairs(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        try:
            payload = {"channel": "B-ETH_USDT_5m-futures", "data": [{
                "pair": "B-ETH_USDT", "open_time": 1_800_000_000,
                "open": "100", "high": "102", "low": "99",
                "close": "101", "volume": "12",
            }]}
            asyncio.run(feed._on_candlestick(payload))
            self.assertEqual(feed.ws_events_received, 0)
            self.assertEqual(feed.rows("B-BTC_USDT"), [])
        finally:
            asyncio.run(feed.close())

    def test_socketio_v4_connection_uses_supported_connect_arguments(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed, SOCKET_URL

        class FakeSocket:
            connected = False
            def __init__(self):
                self.calls = []
            async def connect(self, url, transports=None):
                self.calls.append((url, transports))
            async def wait(self):
                raise asyncio.CancelledError()

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        fake = FakeSocket()
        feed.sio = fake
        try:
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(feed.run_websocket())
            self.assertEqual(fake.calls, [(SOCKET_URL, ["websocket"])])
        finally:
            asyncio.run(feed.close())

    def test_websocket_connect_error_is_counted_and_truncated(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        try:
            asyncio.run(feed._on_connect_error("handshake rejected"))
            self.assertEqual(feed.ws_connect_errors, 1)
            self.assertEqual(feed.last_ws_error, "handshake rejected")
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

    def test_ping_returns_empty_success_response(self):
        import asyncio
        response = asyncio.run(ping())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, b"")


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

    def test_validator_rejects_fractional_timestamps(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval + 0.5, 100, 102, 99, 101, 10] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "fractional timestamp"):
            validate_ohlcv(raw, "5m", base + 4 * interval + 2000)

    def test_validator_rejects_negative_volume(self):
        interval = 300000
        base = (1700000000000 // interval) * interval
        raw = [[base + i * interval, 100, 102, 99, 101, -1] for i in range(4)]
        with self.assertRaisesRegex(ValueError, "invalid price or volume"):
            validate_ohlcv(raw, "5m", base + 4 * interval + 2000)

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

    def test_signal_is_queued_once_for_durable_outbox(self):
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
            calls = 0
            async def send(self, signal):
                self.calls += 1
                return True

        class FakeCandidateStore:
            def __init__(self):
                self.queued = []
            async def observe(self, symbol, direction, candle_ms, adx, adx_min):
                return "READY", None
            async def enqueue_signal(self, signal, candidate_id, cooldown_candles):
                self.queued.append(signal.copy())
                return True

        signal = {"side": "LONG", "entry": 120.0, "stop": 118.0, "target": 123.0,
                  "rr": 1.5, "rsi": 60.0, "adx": 25.0, "context": "15m bullish",
                  "candle": rows[-1][0]}

        last_seen_candle.pop("BTC/USDT", None)
        notifier = FakeNotifier()
        store = FakeCandidateStore()
        queued_before = state.get("signals_queued", 0)
        try:
            from unittest.mock import patch
            with patch("main.evaluate_setup", return_value={**signal, "adx_satisfied": True}):
                async def run_twice():
                    semaphore = asyncio.Semaphore(1)
                    await scan_symbol(FakeFeed(), notifier, semaphore, "BTC/USDT", "B-BTC_USDT",
                                      now_ms, candidate_store=store)
                    await scan_symbol(FakeFeed(), notifier, semaphore, "BTC/USDT", "B-BTC_USDT",
                                      now_ms, candidate_store=store)
                asyncio.run(run_twice())
            self.assertEqual(len(store.queued), 1)
            self.assertEqual(notifier.calls, 0)  # delivery is exclusively handled by outbox worker
            self.assertEqual(state.get("signals_queued", 0), queued_before + 1)
            self.assertEqual(last_seen_candle["BTC/USDT"], int(rows[-1][0]))
        finally:
            last_seen_candle.pop("BTC/USDT", None)

    def test_recent_cache_does_not_trigger_unnecessary_rest_refresh(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = [[base + i * interval, 100 + i, 102 + i, 99 + i, 101 + i, 10]
                for i in range(200)]
        # A cache only 60 seconds past its latest candle close is still
        # inside the validator's accepted freshness window.
        now_ms = base + 200 * interval + 60_000

        class FakeFeed:
            def __init__(self):
                self.refresh_limits = []

            def rows(self, pair):
                return rows

            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)
                return rows[-limit:]

        class FakeNotifier:
            async def send(self, signal):
                return True

        feed = FakeFeed()
        with patch("main.evaluate_setup", return_value=None):
            asyncio.run(scan_symbol(
                feed, FakeNotifier(), asyncio.Semaphore(1), "BTC/USDT",
                "B-BTC_USDT", now_ms,
            ))
        self.assertEqual(feed.refresh_limits, [])

    def test_stale_cache_uses_incremental_rest_refresh_after_freshness_window(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = [[base + i * interval, 100 + i, 102 + i, 99 + i, 101 + i, 10]
                for i in range(200)]
        # Latest candle closed more than one full 5m interval ago.
        now_ms = base + 200 * interval + interval + 31_000

        class FakeFeed:
            def __init__(self):
                self.refresh_limits = []
            def rows(self, pair):
                return rows
            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)
                return rows[-limit:]

        class FakeNotifier:
            async def send(self, signal):
                return True

        feed = FakeFeed()
        with patch("main.evaluate_setup", return_value=None):
            asyncio.run(scan_symbol(
                feed, FakeNotifier(), asyncio.Semaphore(1), "BTC/USDT",
                "B-BTC_USDT", now_ms,
            ))
        self.assertEqual(feed.refresh_limits, [6])

    def test_cache_under_180_bars_retries_full_history_for_15m_context(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        full_rows = [
            [base + i * interval, 100 + i * 0.1, 100.5 + i * 0.1,
             99.5 + i * 0.1, 100 + i * 0.1, 100]
            for i in range(200)
        ]
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def __init__(self):
                self.cache = []
                self.refresh_limits = []

            def rows(self, pair):
                return self.cache

            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)
                self.cache = full_rows[-limit:]

        class FakeNotifier:
            async def send(self, signal):
                return True

        feed = FakeFeed()
        with patch("main.evaluate_setup", return_value=None):
            asyncio.run(scan_symbol(
                feed, FakeNotifier(), asyncio.Semaphore(1), "BTC/USDT",
                "B-BTC_USDT", now_ms,
            ))
        self.assertEqual(feed.refresh_limits, [320])
        self.assertEqual(len(feed.cache), 200)

    def test_gapped_cache_with_180_rows_retries_full_history_for_15m_context(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        full_rows = [
            [base + i * interval, 100 + i * 0.1, 100.5 + i * 0.1,
             99.5 + i * 0.1, 100 + i * 0.1, 100]
            for i in range(300)
        ]
        # Keep >180 cached bars and the latest three contiguous, but retain
        # only two bars per historical 15m bucket so context remains too short.
        sparse_rows = [row for i, row in enumerate(full_rows)
                       if i % 3 != 2 or i == 299]
        now_ms = base + 300 * interval + 2_000

        class FakeFeed:
            def __init__(self):
                self.cache = sparse_rows
                self.refresh_limits = []

            def rows(self, pair):
                return self.cache

            async def fetch_history(self, pair, limit):
                self.refresh_limits.append(limit)
                if limit == 320:
                    self.cache = full_rows

        class FakeNotifier:
            async def send(self, signal):
                return True

        feed = FakeFeed()
        with patch("main.evaluate_setup", return_value=None):
            asyncio.run(scan_symbol(
                feed, FakeNotifier(), asyncio.Semaphore(1), "BTC/USDT",
                "B-BTC_USDT", now_ms,
            ))
        self.assertEqual(feed.refresh_limits, [320])
        self.assertGreaterEqual(len(aggregate_5m_to_15m(feed.cache, now_ms)), 60)

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
            with patch("main.evaluate_setup", return_value=signal):
                asyncio.run(scan_symbol(
                    FakeFeed(), notifier, asyncio.Semaphore(1), "BTC/USDT",
                    "B-BTC_USDT", now_ms, session_started_ms=now_ms - 1_000,
                ))
            self.assertEqual(notifier.calls, 0)
        finally:
            last_seen_candle.pop("BTC/USDT", None)

    def test_health_is_false_until_every_active_symbol_is_accounted_for(self):
        import time
        previous = state.copy()
        try:
            state.update(
                running=True, market_metadata_ready=True, active_symbols=3,
                last_scan_epoch=time.time(), last_cycle_symbols_ok=2,
                last_cycle_symbols_failed=0,
            )
            self.assertFalse(build_health_payload()["ok"])
        finally:
            state.clear()
            state.update(previous)

    def test_health_is_false_when_cycle_completes_with_symbol_failures(self):
        import time
        previous = state.copy()
        try:
            state.update(
                running=True, market_metadata_ready=True, active_symbols=3,
                last_scan_epoch=time.time(), last_cycle_symbols_ok=2,
                last_cycle_symbols_failed=1,
            )
            self.assertFalse(build_health_payload()["ok"])
        finally:
            state.clear()
            state.update(previous)

    def test_health_is_true_after_fresh_successful_scan_cycle(self):
        import time
        previous = state.copy()
        try:
            state.update(
                running=True, market_metadata_ready=True, active_symbols=3,
                last_scan_epoch=time.time(), last_cycle_symbols_ok=3,
                last_cycle_symbols_failed=0,
            )
            self.assertTrue(build_health_payload()["ok"])
        finally:
            state.clear()
            state.update(previous)

    def test_strict_engine_emits_long_for_aligned_uptrend(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertGreaterEqual(signal["adx"], 25)

    def test_signal_includes_actual_five_minute_candle_close(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["candle_close"], signal["candle"] + 300_000)

    def test_telegram_message_uses_candle_close_not_open_timestamp(self):
        signal = {
            "side": "LONG", "symbol": "BTC/USDT", "entry": 100.0,
            "stop": 98.0, "target": 103.0, "rr": 1.5, "rsi": 60.0,
            "adx": 25.0, "context": "15m bullish",
            "candle": 1_800_000_000_000,
            "candle_close": 1_800_000_300_000,
            "signal_generated_at": "2027-01-15T08:05:02+00:00",
        }
        message = TelegramNotifier.format_message(signal)
        self.assertIn("5m candle close: 2027-01-15 08:05:00 UTC", message)
        self.assertIn("Signal generated: 2027-01-15T08:05:02+00:00", message)

    def test_signal_stop_and_target_preserve_configured_risk_reward(self):
        signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        risk = signal["entry"] - signal["stop"]
        reward = signal["target"] - signal["entry"]
        self.assertAlmostEqual(risk, signal["atr"] * 1.5, places=8)
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
        self.assertGreaterEqual(signal["adx"], 25)


if __name__ == "__main__":
    unittest.main()
