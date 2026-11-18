"""CoinDCX USDT-margined futures market-data adapter (public endpoints only)."""
from __future__ import annotations

import asyncio
import logging
import json
import time
from collections import defaultdict
from typing import Any

import httpx
import socketio

log = logging.getLogger("cryptobyance.coindcx")

# Socket.IO/Engine.IO emit one INFO record per market-data frame, including the
# full candle payload. Keep production logs focused on bot diagnostics.
logging.getLogger("socketio.client").setLevel(logging.WARNING)
logging.getLogger("engineio.client").setLevel(logging.WARNING)

ACTIVE_INSTRUMENTS_URL = "https://api.coindcx.com/exchange/v1/derivatives/futures/data/active_instruments"
CANDLES_URL = "https://public.coindcx.com/market_data/candlesticks"
SOCKET_URL = "wss://stream.coindcx.com"


def symbol_to_pair(symbol: str) -> str:
    """Map BTC/USDT to CoinDCX's B-BTC_USDT instrument name."""
    base, quote = symbol.split("/", 1)
    if quote != "USDT" or not base:
        raise ValueError(f"unsupported configured futures symbol: {symbol}")
    return f"B-{base}_{quote}"


def parse_active_instruments(payload: Any) -> set[str]:
    """Normalize CoinDCX's active-instruments response to USDT futures pairs."""
    # Accept the documented plain list and common API envelope wrappers.
    for _ in range(4):
        if isinstance(payload, dict):
            nested = next(
                (payload[key] for key in ("data", "instruments", "result")
                 if key in payload and isinstance(payload[key], (list, dict))),
                None,
            )
            if nested is None or nested is payload:
                break
            payload = nested
        else:
            break
    if not isinstance(payload, list):
        raise ValueError("CoinDCX active instruments response is not a list")

    pairs: set[str] = set()
    for item in payload:
        if isinstance(item, str):
            pair = item.strip()
        elif isinstance(item, dict):
            pair = str(
                item.get("pair") or item.get("instrument_name") or item.get("symbol") or ""
            ).strip()
        else:
            continue
        # The unfiltered fallback can contain other margin currencies. Only
        # accept the exact CoinDCX USDT perpetual naming convention.
        if pair.startswith("B-") and pair.endswith("_USDT"):
            pairs.add(pair)
    if not pairs:
        raise ValueError("CoinDCX returned no active USDT futures instruments")
    return pairs


def candle_to_row(item: dict[str, Any]) -> list[float | int]:
    """Convert REST or futures-socket candle payload into CCXT-like OHLCV rows."""
    stamp = item.get("time", item.get("open_time", item.get("t")))
    if stamp is None:
        raise ValueError("CoinDCX candle missing open timestamp")
    stamp = float(stamp)
    # REST 'time' and socket 'open_time' are milliseconds and seconds respectively.
    timestamp = int(stamp if stamp > 10_000_000_000 else stamp * 1000)
    return [
        timestamp,
        float(item["open"]), float(item["high"]), float(item["low"]),
        float(item["close"]), float(item.get("volume", item.get("v", 0)) or 0),
    ]


def deduplicate_rest_candles(rows: list[list]) -> list[list]:
    """Collapse identical REST duplicates; reject conflicting bars at one timestamp."""
    by_timestamp: dict[int, list] = {}
    for row in rows:
        timestamp = int(row[0])
        previous = by_timestamp.get(timestamp)
        if previous is not None and previous != row:
            raise ValueError(f"conflicting CoinDCX REST candles for timestamp {timestamp}")
        by_timestamp[timestamp] = row
    return [by_timestamp[timestamp] for timestamp in sorted(by_timestamp)]


def aggregate_5m_to_15m(rows: list[list], now_ms: int | None = None) -> list[list]:
    """Build aligned 15m bars from three contiguous, fully closed 5m candles."""
    now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    interval = 300_000
    groups: dict[int, list[list]] = defaultdict(list)
    for row in rows:
        ts = int(row[0])
        if ts % interval:
            continue
        if ts + interval > now_ms - 1_500:
            continue
        groups[(ts // 900_000) * 900_000].append(row)
    result = []
    for start, group in sorted(groups.items()):
        group.sort(key=lambda r: int(r[0]))
        expected = [start, start + interval, start + 2 * interval]
        if [int(r[0]) for r in group] != expected:
            continue
        if start + 900_000 > now_ms - 1_500:
            continue
        result.append([
            start, float(group[0][1]), max(float(r[2]) for r in group),
            min(float(r[3]) for r in group), float(group[-1][4]),
            sum(float(r[5]) for r in group),
        ])
    return result


class CoinDCXFuturesFeed:
    """REST history + public futures 5m candlestick Socket.IO stream."""

    def __init__(self, pairs: list[str], timeout: float = 15.0):
        self.pairs = list(dict.fromkeys(pairs))
        self.client = httpx.AsyncClient(timeout=timeout, headers={"User-Agent": "cryptobyance-signal-bot/2.0"})
        self.sio = socketio.AsyncClient(
            reconnection=True, reconnection_attempts=0, reconnection_delay=2,
            reconnection_delay_max=30, logger=False, engineio_logger=False,
        )
        self.bars: dict[str, dict[int, list]] = {pair: {} for pair in self.pairs}
        # 5m candles whose final OHLCV was confirmed by REST. Keep markers
        # bounded with the bar cache so late websocket frames cannot rewrite
        # a completed candle after the next interval has been confirmed.
        self._rest_confirmed_closed_opens: dict[str, set[int]] = {
            pair: set() for pair in self.pairs
        }
        self.websocket_connected = False
        self.last_ws_event: float | None = None
        self.ws_events_received = 0
        self.last_ws_pair: str | None = None
        self.ws_connect_errors = 0
        self.last_ws_error: str | None = None
        self.ws_disconnects = 0
        self.ws_task: asyncio.Task | None = None
        self._websocket_ready = asyncio.Event()
        self._closed = False
        self.sio.on("connect", self._on_connect)
        self.sio.on("disconnect", self._on_disconnect)
        self.sio.on("candlestick", self._on_candlestick)
        self.sio.on("connect_error", self._on_connect_error)

    def configure_active_pairs(self, active_pairs: list[str]) -> None:
        """Subscribe and cache only contracts confirmed active by CoinDCX."""
        confirmed = set(active_pairs)
        self.pairs = [pair for pair in self.pairs if pair in confirmed]
        self.bars = {pair: self.bars.get(pair, {}) for pair in self.pairs}
        log.info("CoinDCX active futures pairs configured=%d", len(self.pairs))

    async def active_instruments(self) -> set[str]:
        """Load active USDT futures, retrying the documented endpoint unfiltered.

        Some CoinDCX responses have intermittently returned an empty/unsupported
        filtered payload. The unfiltered fallback is safe because the parser
        explicitly keeps only B-..._USDT instruments.
        """
        attempts = (
            ("USDT-filtered", [("margin_currency_short_name[]", "USDT")]),
            ("unfiltered-fallback", None),
        )
        failures: list[str] = []
        for label, params in attempts:
            response = None
            try:
                response = await self.client.get(ACTIVE_INSTRUMENTS_URL, params=params)
                response.raise_for_status()
                pairs = parse_active_instruments(response.json())
                log.info("CoinDCX active instruments loaded source=%s count=%d", label, len(pairs))
                return pairs
            except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
                status = getattr(response, "status_code", None)
                content_type = getattr(response, "headers", {}).get("content-type", "unknown")
                preview = str(getattr(response, "text", ""))[:180].replace("\\n", " ")
                log.warning(
                    "CoinDCX active instruments source=%s failed status=%s content_type=%s "
                    "error=%s response_preview=%r",
                    label, status, content_type, type(exc).__name__, preview,
                )
                failures.append(f"{label}:{type(exc).__name__}")
        raise RuntimeError(
            "CoinDCX active instruments unavailable after filtered and unfiltered requests "
            f"({', '.join(failures)})"
        )

    async def fetch_history(self, pair: str, limit: int = 320) -> list[list]:
        now_s = int(time.time())
        params = {
            "pair": pair,
            "from": now_s - limit * 5 * 60,
            "to": now_s,
            "resolution": "5",
            "pcode": "f",
        }
        response = await self.client.get(CANDLES_URL, params=params)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            if payload.get("s") not in (None, "ok"):
                raise RuntimeError(f"CoinDCX candle API status: {payload.get('s')}")
            data = payload.get("data", [])
        else:
            data = payload
        if not isinstance(data, list) or not data:
            raise ValueError(f"empty CoinDCX 5m history for {pair}")
        rows = [candle_to_row(item) for item in data]
        rows = deduplicate_rest_candles(rows)
        self._merge_rows(pair, rows)
        return rows

    async def refresh_latest_closed_candle(self, pair: str, now_ms: int | None = None) -> int:
        """Confirm the just-closed 5m candle from REST once per pair and interval.

        WebSocket candle events are useful for live cache updates, but a final
        frame is not guaranteed to arrive exactly at the interval boundary.
        Never let a provisional last-bar value become an actionable completed
        candle without a REST confirmation.
        """
        now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
        interval = 300_000
        expected_open = (now_ms // interval) * interval - interval
        if expected_open < 0:
            raise ValueError("invalid expected completed candle timestamp")
        if expected_open in self._rest_confirmed_closed_opens.get(pair, set()):
            return expected_open

        rows = await self.fetch_history(pair, limit=6)
        confirmed = {int(row[0]) for row in rows}
        if expected_open not in confirmed:
            raise ValueError(
                f"REST history did not confirm latest completed 5m candle for {pair}: "
                f"expected_open={expected_open}"
            )
        self._rest_confirmed_closed_opens.setdefault(pair, set()).add(expected_open)
        return expected_open

    def _merge_rows(self, pair: str, rows: list[list], source: str = "rest") -> None:
        """Merge candle rows without allowing provisional WS data to replace REST finals.

        REST is authoritative and may replace a provisional websocket row.
        WebSocket rows may update unconfirmed timestamps only.
        """
        if source not in {"rest", "websocket"}:
            raise ValueError(f"unsupported candle source: {source}")
        cache = self.bars.setdefault(pair, {})
        confirmed = self._rest_confirmed_closed_opens.setdefault(pair, set())
        for row in rows:
            timestamp = int(row[0])
            if source == "websocket" and timestamp in confirmed:
                continue
            cache[timestamp] = row
        if len(cache) > 500:
            evicted = sorted(cache)[:-500]
            for ts in evicted:
                del cache[ts]
                confirmed.discard(ts)

    def rows(self, pair: str) -> list[list]:
        return [self.bars.get(pair, {})[ts] for ts in sorted(self.bars.get(pair, {}))]

    async def _on_connect(self):
        self.websocket_connected = True
        log.info("CoinDCX futures websocket connected; subscribing to %d 5m channels", len(self.pairs))
        for pair in self.pairs:
            await self.sio.emit("join", {"channelName": f"{pair}_5m-futures"})
        self._websocket_ready.set()
        log.info("CoinDCX futures websocket subscriptions sent=%d", len(self.pairs))

    async def _on_connect_error(self, response):
        """Record Socket.IO handshake failures using the v4-compatible event API."""
        self.ws_connect_errors += 1
        self.last_ws_error = str(response)[:300]
        log.warning("CoinDCX websocket connect error: %s", self.last_ws_error)

    async def _on_disconnect(self):
        self.websocket_connected = False
        self._websocket_ready.clear()
        self.ws_disconnects += 1
        log.warning("CoinDCX futures websocket disconnected")

    async def _on_candlestick(self, response):
        """Decode Socket.IO candle events, including CoinDCX's nested JSON envelope."""
        try:
            if isinstance(response, str):
                response = json.loads(response)
            if not isinstance(response, dict):
                return

            channel = str(response.get("channel") or response.get("channelName") or "")
            data = response.get("data", [])

            # CoinDCX futures Socket.IO sends data as a JSON-encoded envelope:
            # {"event":"candlestick","data":"{\"data\":[{...}],...}"}.
            if isinstance(data, str):
                data = json.loads(data)
            if isinstance(data, dict):
                channel = str(data.get("channel") or data.get("channelName") or channel)
                nested = data.get("data", data)
                if isinstance(nested, str):
                    nested = json.loads(nested)
                if isinstance(nested, dict) and isinstance(nested.get("data"), (list, dict)):
                    nested = nested["data"]
                data = nested
            if isinstance(data, dict):
                data = [data]
            if not isinstance(data, list):
                return

            channel_pair = channel.removesuffix("_5m-futures")
            for item in data:
                if not isinstance(item, dict):
                    continue
                pair = str(item.get("pair") or channel_pair)
                if pair not in self.bars:
                    continue
                row = candle_to_row(item)
                self._merge_rows(pair, [row], source="websocket")
                self.last_ws_event = time.time()
                self.ws_events_received += 1
                self.last_ws_pair = pair
                if self.ws_events_received == 1:
                    log.info("CoinDCX first futures candle event received pair=%s timestamp=%s", pair, row[0])
        except (TypeError, ValueError, json.JSONDecodeError, KeyError) as exc:
            log.warning("Ignoring malformed CoinDCX futures candlestick event: %s", str(exc)[:200])
        except Exception:
            log.exception("Could not process CoinDCX futures candlestick event")

    async def run_websocket(self) -> None:
        delay = 2
        while not self._closed:
            try:
                if not self.sio.connected:
                    await self.sio.connect(SOCKET_URL, transports=["websocket"])
                await self.sio.wait()
                delay = 2
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.websocket_connected = False
                log.warning("CoinDCX websocket connection failed: %s; retry in %ss", exc, delay)
                await asyncio.sleep(delay)
                delay = min(30, delay * 2)

    def start_websocket(self) -> None:
        if self.ws_task is None or self.ws_task.done():
            self.ws_task = asyncio.create_task(self.run_websocket(), name="coindcx-futures-websocket")

    async def wait_until_connected(self, timeout: float = 20.0) -> bool:
        """Wait for connection and all candle-channel subscriptions, but never indefinitely."""
        try:
            await asyncio.wait_for(self._websocket_ready.wait(), timeout=max(0.0, timeout))
            return True
        except asyncio.TimeoutError:
            return False

    async def close(self) -> None:
        self._closed = True
        if self.ws_task:
            self.ws_task.cancel()
            try:
                await self.ws_task
            except asyncio.CancelledError:
                pass
        if self.sio.connected:
            await self.sio.disconnect()
        await self.client.aclose()
