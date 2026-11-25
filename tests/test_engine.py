import unittest
import numpy as np
import pandas as pd

from coindcx_feed import (
    aggregate_5m_to_15m, candle_to_row, parse_active_instruments, symbol_to_pair,
)
from main import (
    SYMBOLS, scan_symbol, last_seen_candle, state, build_health_payload, ping,
    scanner_sleep_seconds, seconds_until_next_candle_scan,
)
from strategy import candles_to_frame, indicators, evaluate_signal, evaluate_setup, evaluate_state_inputs, validate_ohlcv
from telegram import TelegramNotifier




class ScannerSchedulingTests(unittest.TestCase):
    def test_stale_scan_waits_for_next_completed_candle_instead_of_repeating_old_bar(self):
        # 1000s is 200s after a 5m boundary; the next close is 203s away.
        self.assertEqual(seconds_until_next_candle_scan(1000), 203)
        self.assertEqual(scanner_sleep_seconds(1000, stale_data=True), 203)

    def test_stale_scan_near_next_close_waits_only_until_close_grace(self):
        # Five seconds before the next boundary, wait eight seconds (3s grace).
        self.assertEqual(scanner_sleep_seconds(1195, stale_data=True), 8)

    def test_fresh_scan_remains_aligned_to_next_completed_candle(self):
        self.assertEqual(scanner_sleep_seconds(1000, stale_data=False), 203)


class CoinDCXFeedTests(unittest.TestCase):
    def test_socket_transport_loggers_do_not_emit_every_market_data_frame(self):
        import logging
        self.assertGreaterEqual(logging.getLogger("socketio.client").level, logging.WARNING)
        self.assertGreaterEqual(logging.getLogger("engineio.client").level, logging.WARNING)

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
        self.assertEqual(
            parse_active_instruments({
                "data": {"instruments": [
                    {"instrument_name": "B-ETH_USDT"},
                    {"symbol": "B-BTC_USDC"},
                    "B-SOL_USDT",
                ]}
            }),
            {"B-ETH_USDT", "B-SOL_USDT"},
        )

    def test_active_instruments_retries_unfiltered_when_filtered_response_is_empty(self):
        import asyncio
        from coindcx_feed import CoinDCXFuturesFeed

        class FakeResponse:
            def __init__(self, payload):
                self.payload = payload
                self.status_code = 200
                self.headers = {"content-type": "application/json"}
                self.text = str(payload)

            def raise_for_status(self):
                return None

            def json(self):
                return self.payload

        class FakeClient:
            def __init__(self):
                self.calls = []

            async def get(self, url, params=None):
                self.calls.append((url, params))
                if params is not None:
                    return FakeResponse([])
                return FakeResponse(["B-BTC_USDT", "B-ETH_USDC", "B-SOL_USDT"])

            async def aclose(self):
                return None

        feed = CoinDCXFuturesFeed(["B-BTC_USDT"])
        fake_client = FakeClient()
        feed.client = fake_client
        try:
            result = asyncio.run(feed.active_instruments())
            self.assertEqual(result, {"B-BTC_USDT", "B-SOL_USDT"})
            self.assertEqual(len(fake_client.calls), 2)
            self.assertIsNotNone(fake_client.calls[0][1])
            self.assertIsNone(fake_client.calls[1][1])
        finally:
            asyncio.run(feed.close())

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

    def test_universe_has_exactly_59_unique_symbols(self):
        self.assertEqual(len(SYMBOLS), 59)
        self.assertEqual(len(set(SYMBOLS)), 59)

    def test_ping_returns_http_200_keepalive_response(self):
        import asyncio
        response = asyncio.run(ping())
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body, b"OK")


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
        # A byte-for-byte equivalent duplicate is safe to collapse.
        raw.append([base + interval, 101, 103, 100, 102, 10])
        now_ms = base + 4 * interval + 2000
        frame = validate_ohlcv(raw, "5m", now_ms)
        self.assertEqual(len(frame), 4)
        self.assertEqual(frame.timestamp.tolist(), sorted(frame.timestamp.tolist()))
        self.assertEqual(float(frame.iloc[1].volume), 10)
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
            async def invalidate(self, symbol):
                return None
            async def enqueue_signal(self, signal, candidate_id, cooldown_candles, strategy_key="default"):
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
        self.assertEqual(feed.refresh_limits, [])
        self.assertEqual(len(feed.cache), len(sparse_rows))

    def test_scanner_passes_optional_aligned_15m_context_without_changing_5m_scan(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = [[base + i * interval, 100 + i * 0.1, 100.5 + i * 0.1,
                 99.5 + i * 0.1, 100 + i * 0.1, 100] for i in range(200)]
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def rows(self, pair):
                return rows

        class FakeNotifier:
            async def send(self, signal):
                return True

        last_seen_candle.pop('BTC/USDT', None)
        captured = {}
        def fake_evaluate(df5, df15=None):
            captured['df5'] = df5
            captured['df15'] = df15
            return None

        with patch('main.evaluate_setup', side_effect=fake_evaluate):
            asyncio.run(scan_symbol(FakeFeed(), FakeNotifier(), asyncio.Semaphore(1),
                                    'BTC/USDT', 'B-BTC_USDT', now_ms))
        self.assertIsNotNone(captured['df15'])
        self.assertGreaterEqual(len(captured['df15']), 35)
        self.assertTrue((captured['df15'].timestamp.astype('int64') % 900_000 == 0).all())
        self.assertEqual(int(captured['df5'].iloc[-1].timestamp), rows[-1][0])
        last_seen_candle.pop('BTC/USDT', None)

    def test_scanner_keeps_5m_evaluation_when_15m_context_is_unavailable(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = [[base + i * interval, 100 + i * 0.1, 100.5 + i * 0.1,
                 99.5 + i * 0.1, 100 + i * 0.1, 100] for i in range(200)]
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def rows(self, pair):
                return rows

        class FakeNotifier:
            async def send(self, signal):
                return True

        last_seen_candle.pop('BTC/USDT', None)
        with patch('main.aggregate_5m_to_15m', return_value=[]):
            with patch('main.evaluate_setup', return_value=None) as evaluate:
                asyncio.run(scan_symbol(FakeFeed(), FakeNotifier(), asyncio.Semaphore(1),
                                        'BTC/USDT', 'B-BTC_USDT', now_ms))
        evaluate.assert_called_once()
        self.assertIsNone(evaluate.call_args.args[1])
        last_seen_candle.pop('BTC/USDT', None)

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

    def test_incomplete_state_inputs_leave_closed_candle_retryable(self):
        import asyncio
        from unittest.mock import patch

        interval = 300_000
        base = (1_800_000_000_000 // interval) * interval
        rows = [[base + i * interval, 100 + i * 0.1, 101 + i * 0.1,
                 99 + i * 0.1, 100.5 + i * 0.1, 100] for i in range(200)]
        now_ms = base + 200 * interval + 2_000

        class FakeFeed:
            def rows(self, pair):
                return rows

        class FakeNotifier:
            async def send(self, signal):
                raise AssertionError("scanner must not send directly")

        class FakeStore:
            def __init__(self):
                self.state_loads = 0

            async def update_open_outcomes(self, symbol, candles):
                return 0

            async def load_symbol_states(self, strategy_key, symbol):
                self.state_loads += 1
                return {}

        symbol = "BTC/USDT"
        last_seen_candle.pop(symbol, None)
        store = FakeStore()
        before = state.get("state_input_deferred", 0)
        try:
            with patch("main.evaluate_state_inputs", return_value=None):
                async def run_twice():
                    for _ in range(2):
                        await scan_symbol(
                            FakeFeed(), FakeNotifier(), asyncio.Semaphore(1),
                            symbol, "B-BTC_USDT", now_ms,
                            candidate_store=store,
                        )
                asyncio.run(run_twice())

            self.assertEqual(store.state_loads, 2)
            self.assertNotIn(symbol, last_seen_candle)
            self.assertEqual(state.get("state_input_deferred", 0), before + 2)
        finally:
            last_seen_candle.pop(symbol, None)

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
                running=True, market_metadata_ready=True,
                database_connected=True, telegram_configured=True,
                active_symbols=3, last_scan_epoch=time.time(),
                last_cycle_symbols_ok=3, last_cycle_symbols_failed=0,
            )
            self.assertTrue(build_health_payload()["ok"])
        finally:
            state.clear()
            state.update(previous)

    def _indicator_frame(self, side="LONG", adx=22.0, n=60):
        # Last three bars are directionally aligned and both EMAs slope in the
        # trade direction; the preceding bar is not aligned (fresh, not late).
        if side == "LONG":
            ema9 = np.full(n, 99.0)
            ema21 = np.full(n, 99.5)
            ema9[-3:] = [100.2, 100.6, 101.0]
            ema21[-3:] = [99.6, 99.8, 100.0]
        else:
            ema9 = np.full(n, 101.0)
            ema21 = np.full(n, 100.5)
            ema9[-3:] = [99.8, 99.4, 99.0]
            ema21[-3:] = [100.4, 100.2, 100.0]
        frame = pd.DataFrame({
            "timestamp": 1_800_000_000_000 + np.arange(n) * 300_000,
            "open": np.full(n, 100.0), "high": np.full(n, 103.0),
            "low": np.full(n, 97.0),
            "close": np.full(n, 102.0 if side == "LONG" else 98.0),
            "volume": np.full(n, 100.0),
            "ema9": ema9, "ema21": ema21,
            "vwap": np.full(n, 100.0), "rsi": np.full(n, 60.0 if side == "LONG" else 40.0),
            "adx": np.full(n, adx), "atr": np.full(n, 2.0),
            "plus_di": np.full(n, 30.0 if side == "LONG" else 10.0),
            "minus_di": np.full(n, 10.0 if side == "LONG" else 30.0),
        })
        return frame

    def _context_frame(self, bullish=True, n=60):
        frame = self._indicator_frame("LONG", n=n)
        frame["ema9"] = 101.0 if bullish else 99.0
        frame["ema21"] = 100.0
        return frame

    def test_15m_context_calculates_ema_only_and_never_calculates_rsi(self):
        from unittest.mock import patch

        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        fifteen["rsi"] = 40.0
        calls = []

        def fake_indicators(frame, include_rsi=True):
            calls.append(include_rsi)
            return five.copy() if len(calls) == 1 else fifteen.copy()

        with patch("strategy.indicators", side_effect=fake_indicators):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))

        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(calls, [True, False])
        self.assertNotIn("context_rsi14", signal)
        self.assertNotIn("context_rsi_aligned", signal)
        self.assertNotIn("context_rsi_score", signal)
        self.assertNotIn("context_rsi14", signal["diagnostics"])
        self.assertEqual(signal["diagnostics"]["version"], 5)
        self.assertAlmostEqual(signal["diagnostics"]["ema_spread_atr"], 0.5)
        self.assertAlmostEqual(signal["diagnostics"]["price_vwap_distance_atr"], 1.0)
        self.assertAlmostEqual(signal["diagnostics"]["context_ema_spread_pct"], 1.0)
        self.assertTrue(all(np.isfinite(value) for value in signal["diagnostics"].values()))


    def test_short_signal_diagnostics_keep_directional_signs(self):
        from unittest.mock import patch

        five, fifteen = self._indicator_frame("SHORT"), self._context_frame(False)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(-1, 900000))

        self.assertIsNotNone(signal)
        diagnostics = signal["diagnostics"]
        self.assertLess(diagnostics["ema_spread_atr"], 0)
        self.assertLess(diagnostics["price_vwap_distance_atr"], 0)
        self.assertIn("ema9_slope_atr_3bars", diagnostics)

    def test_ema_alignment_with_all_gates_and_bullish_15m_emits_long(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(signal["context"], "15m bullish")
        self.assertGreaterEqual(signal["adx"], 20)

    def test_two_bar_ema_alignment_age_is_accepted(self):
        from unittest.mock import patch
        five = self._indicator_frame("LONG")
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_setup(self._trend_frame(1, 300000), None)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["diagnostics"]["ema_alignment_age_bars"], 2)

    def test_established_ema_alignment_without_recent_crossover_is_accepted(self):
        from unittest.mock import patch
        five = self._indicator_frame("LONG")
        # EMA9 has been above EMA21 throughout the supplied history: no
        # crossover exists in the lookback, but directional alignment is valid.
        five["ema9"] = 101.0
        five["ema21"] = 100.0
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_setup(self._trend_frame(1, 300000), None)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertIsNone(signal["diagnostics"]["ema_alignment_age_bars"])

    def test_armed_setup_remains_valid_when_alignment_is_older_than_two_bars(self):
        from unittest.mock import patch
        five = self._indicator_frame("LONG")
        five["ema9"] = 101.0
        five["ema21"] = 100.0
        with patch("strategy.indicators", return_value=five):
            result = evaluate_state_inputs(self._trend_frame(1, 300000), None, {"LONG"})
        self.assertTrue(result["LONG"]["setup_valid"])
        self.assertFalse(result["LONG"]["invalidation"])
        self.assertFalse(result["LONG"]["trigger"])

    def test_short_established_alignment_without_crossover_is_accepted(self):
        from unittest.mock import patch
        five = self._indicator_frame("SHORT")
        five["ema9"] = 99.0
        five["ema21"] = 100.0
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_setup(self._trend_frame(-1, 300000), None)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")
        self.assertIsNone(signal["diagnostics"]["ema_alignment_age_bars"])

    def test_existing_ema_alignment_is_valid_without_a_new_crossover(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")

    def test_15m_bearish_trend_does_not_veto_valid_5m_long(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(False)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(signal["context"], "15m bearish")

    def test_ema_alignment_with_bearish_15m_emits_short(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("SHORT"), self._context_frame(False)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")
        self.assertEqual(signal["context"], "15m bearish")

    def test_15m_bullish_trend_does_not_veto_valid_5m_short(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("SHORT"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")
        self.assertEqual(signal["context"], "15m bullish")

    def test_missing_15m_data_does_not_block_valid_5m_signal(self):
        from unittest.mock import patch
        with patch("strategy.indicators", return_value=self._indicator_frame("LONG")):
            signal = evaluate_signal(self._trend_frame(1, 300000), None)
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(signal["context"], "15m unavailable")

    def test_insufficient_15m_history_does_not_block_valid_5m_signal(self):
        from unittest.mock import patch
        five = self._indicator_frame("LONG")
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000).iloc[:34])
        self.assertIsNotNone(signal)
        self.assertEqual(signal["context"], "15m unavailable")

    def test_neutral_15m_trend_does_not_block_valid_5m_signal(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        fifteen["ema9"] = 100.0
        fifteen["ema21"] = 100.0
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["context"], "15m neutral")

    def test_nonfinite_15m_ema_is_ignored_for_5m_entry(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        fifteen.loc[fifteen.index[-1], "ema9"] = np.nan
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["context"], "15m unavailable")

    def test_simple_strategy_accepts_ema_alignment_older_than_two_bars(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        five.loc[five.index[-4], "ema9"] = 100.1
        five.loc[five.index[-4], "ema21"] = 100.0
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "LONG")
        self.assertEqual(signal["diagnostics"]["ema_alignment_age_bars"], 3)

    def test_simple_strategy_does_not_require_ema_slope_gate(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        five.loc[five.index[-4], "ema9"] = five.iloc[-1].ema9
        five.loc[five.index[-4], "ema21"] = five.iloc[-1].ema9 + 0.1
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            self.assertEqual(evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))["side"], "LONG")

    def test_simple_strategy_does_not_add_vwap_extension_gate(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        five.loc[five.index[-1], "close"] = 104.1
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            self.assertEqual(evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))["side"], "LONG")

    def test_global_early_entry_gates_apply_to_short_side(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("SHORT"), self._context_frame(False)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(-1, 300000), self._trend_frame(-1, 900000))
        self.assertIsNotNone(signal)
        self.assertEqual(signal["side"], "SHORT")

    def test_adx_below_20_rejects_immediately(self):
        from unittest.mock import patch
        with patch("strategy.indicators", return_value=self._indicator_frame("LONG", adx=19.9)):
            self.assertIsNone(evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000)))

    def test_signal_includes_actual_five_minute_candle_close(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        self.assertEqual(signal["candle_close"], signal["candle"] + 300_000)

    def test_production_setup_uses_swing_stop_and_swing_r_targets(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_setup(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        risk = signal["entry"] - signal["stop"]
        self.assertEqual(signal["risk_model"], "swing_structure")
        self.assertAlmostEqual(signal["stop"], 97.0, places=8)
        self.assertAlmostEqual(signal["target1"], signal["entry"] + risk * 1.0, places=8)
        self.assertAlmostEqual(signal["target2"], signal["entry"] + risk * 2.0, places=8)
        self.assertAlmostEqual(signal["risk_distance"], risk, places=8)

    def test_executable_setup_rejects_long_when_directional_di_is_bearish(self):
        from unittest.mock import patch
        five = self._indicator_frame("LONG")
        five.loc[five.index[-1], "plus_di"] = 10.0
        five.loc[five.index[-1], "minus_di"] = 30.0
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_setup(self._trend_frame(1, 300000), None)
        self.assertIsNone(signal)

    def test_executable_setup_rejects_short_when_directional_di_is_bullish(self):
        from unittest.mock import patch
        five = self._indicator_frame("SHORT")
        five.loc[five.index[-1], "plus_di"] = 30.0
        five.loc[five.index[-1], "minus_di"] = 10.0
        with patch("strategy.indicators", return_value=five):
            signal = evaluate_setup(self._trend_frame(-1, 300000), None)
        self.assertIsNone(signal)

    def test_legacy_signal_keeps_atr_stop_and_consistent_supplemental_targets(self):
        from unittest.mock import patch
        five, fifteen = self._indicator_frame("LONG"), self._context_frame(True)
        with patch("strategy.indicators", side_effect=[five, fifteen]):
            signal = evaluate_signal(self._trend_frame(1, 300000), self._trend_frame(1, 900000))
        risk = signal["entry"] - signal["stop"]
        reward = signal["target"] - signal["entry"]
        self.assertEqual(signal["risk_model"], "legacy_atr")
        self.assertAlmostEqual(risk, signal["atr"] * 1.5, places=8)
        self.assertAlmostEqual(reward / risk, 1.5, places=8)
        self.assertAlmostEqual(signal["target1"], signal["entry"] + risk * 1.0, places=8)
        self.assertAlmostEqual(signal["target2"], signal["entry"] + risk * 2.0, places=8)
        self.assertAlmostEqual(signal["risk_distance"], risk, places=8)


if __name__ == "__main__":
    unittest.main()
