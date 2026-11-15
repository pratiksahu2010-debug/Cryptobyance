"""Single-engine CoinDCX USDT futures crypto signal bot. Signals only; never places orders."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Response

load_dotenv()

from coindcx_feed import CoinDCXFuturesFeed, aggregate_5m_to_15m, symbol_to_pair
from strategy import evaluate_signal, timeframe_milliseconds, validate_ohlcv
from telegram import TelegramNotifier

from config import settings

LOG_LEVEL = settings.log_level
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("cryptobyance")

# Fixed, deterministic universe: liquid USDT-margined perpetual contracts.
SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT",
    "TRX/USDT", "LTC/USDT", "BCH/USDT", "NEAR/USDT", "APT/USDT",
    "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT", "INJ/USDT",
    "ATOM/USDT", "ETC/USDT", "FIL/USDT", "ICP/USDT", "AAVE/USDT",
    "UNI/USDT", "TIA/USDT", "JUP/USDT", "WIF/USDT", "PEPE/USDT",
    "FLOKI/USDT", "SHIB/USDT", "RUNE/USDT", "FTM/USDT", "GALA/USDT",
    "SAND/USDT", "MANA/USDT", "AXS/USDT", "EGLD/USDT", "ALGO/USDT",
    "VET/USDT", "THETA/USDT", "EOS/USDT", "XLM/USDT", "HBAR/USDT",
    "MKR/USDT", "CRV/USDT", "LDO/USDT", "DYDX/USDT", "1000BONK/USDT",
    "1000PEPE/USDT", "ORDI/USDT", "STX/USDT", "IMX/USDT", "GRT/USDT",
    "QNT/USDT", "ENS/USDT", "KAS/USDT", "PENDLE/USDT", "ONDO/USDT",
]
assert len(SYMBOLS) == 60 and len(set(SYMBOLS)) == 60

SCAN_SECONDS = settings.scan_interval_seconds
CONCURRENCY = settings.concurrency
COOLDOWN_CANDLES = settings.signal_cooldown_candles
TIMEFRAME = settings.timeframe
CONTEXT_TF = settings.context_timeframe

state: dict[str, Any] = {
    "started_at": time.time(), "session_started_at": None, "scans": 0, "last_scan": None,
    "last_scan_seconds": None, "last_scan_epoch": None,
    "last_cycle_symbols_ok": 0, "last_cycle_symbols_failed": 0,
    "symbols_ok": 0, "symbols_failed": 0,
    "signals_sent": 0, "telegram_delivery_failures": 0, "last_telegram_success": None,
    "last_error": None, "running": False,
    "active_symbols": 0, "unavailable_symbols": [], "data_validation_failures": 0,
    "market_metadata_ready": False, "websocket_connected": False, "last_websocket_event": None,
    "websocket_events_received": 0, "last_websocket_pair": None,
    "websocket_connect_errors": 0, "last_websocket_error": None, "websocket_disconnects": 0,
    "websocket_candle_fresh": False, "scanner_restarts": 0,
    "telegram_configured": False,
}
# In-process deduplication only. Startup cutoff prevents replaying candles that
# closed before the current scanner session began; state is not durable across restarts.
last_alert: dict[str, tuple[str, int]] = {}
last_seen_candle: dict[str, int] = {}

async def scan_symbol(feed, notifier, semaphore, display_symbol: str, pair: str, now_ms: int, session_started_ms: int | None = None) -> None:
    async with semaphore:
        try:
            rows = feed.rows(pair)
            latest_ts = int(rows[-1][0]) if rows else 0
            # 60 completed 15m candles require at least 180 completed 5m candles.
            full_history_refreshed = False
            if (len(rows) < 180 or not latest_ts or
                    now_ms - (latest_ts + timeframe_milliseconds('5m')) >
                    timeframe_milliseconds('5m') + 30_000):
                # A failed/undersized bootstrap needs a full indicator window;
                # established caches use a small REST refresh to limit API traffic.
                refresh_limit = 320 if len(rows) < 180 else 6
                await feed.fetch_history(pair, limit=refresh_limit)
                full_history_refreshed = refresh_limit == 320
                rows = feed.rows(pair)
            df5 = validate_ohlcv(rows, '5m', now_ms)
            rows15 = aggregate_5m_to_15m(rows, now_ms)
            # A cache can contain 180+ 5m rows but still have fewer than 60
            # usable 15m bars because of historical gaps or alignment. Retry a
            # full history fetch once before declaring the symbol unhealthy.
            if len(rows15) < 60 and len(rows) >= 180 and not full_history_refreshed:
                await feed.fetch_history(pair, limit=320)
                rows = feed.rows(pair)
                df5 = validate_ohlcv(rows, '5m', now_ms)
                rows15 = aggregate_5m_to_15m(rows, now_ms)
            df15 = validate_ohlcv(rows15, '15m', now_ms)
            if len(df5) < 60 or len(df15) < 60:
                raise ValueError(f'insufficient completed candles: 5m={len(df5)}, 15m={len(df15)}')
            closed_ts = int(df5.iloc[-1].timestamp)
            five_minute_close = closed_ts + timeframe_milliseconds('5m')
            # Never emit a historical setup discovered during process startup.
            if session_started_ms is not None and five_minute_close <= session_started_ms:
                last_seen_candle[display_symbol] = closed_ts
                state['symbols_ok'] += 1
                return
            context_close = int(df15.iloc[-1].timestamp) + timeframe_milliseconds('15m')
            if context_close > five_minute_close:
                raise ValueError('15m context candle is newer than the 5m signal candle')
            if five_minute_close - context_close > timeframe_milliseconds('15m'):
                raise ValueError('15m context candle is too old for the 5m signal candle')
            if last_seen_candle.get(display_symbol) == closed_ts:
                state['symbols_ok'] += 1
                return
            signal = evaluate_signal(df5, df15)
            if not signal:
                last_seen_candle[display_symbol] = closed_ts
                state['symbols_ok'] += 1
                return
            previous = last_alert.get(display_symbol)
            if previous and previous[0] == signal['side']:
                candles_since = (closed_ts - previous[1]) // 300_000
                if candles_since < COOLDOWN_CANDLES:
                    last_seen_candle[display_symbol] = closed_ts
                    state['symbols_ok'] += 1
                    return
            signal['symbol'] = display_symbol
            delivered = await notifier.send(signal)
            # Record this candle even if delivery is unavailable. This prevents
            # retrying the same signal on every scan cycle and flooding logs.
            last_seen_candle[display_symbol] = closed_ts
            if delivered:
                last_alert[display_symbol] = (signal['side'], closed_ts)
                state['signals_sent'] += 1
                state['last_telegram_success'] = datetime.now(timezone.utc).isoformat()
                log.info('SIGNAL SENT %s %s entry=%s ADX=%.1f RSI=%.1f', display_symbol, signal['side'], signal['entry'], signal['adx'], signal['rsi'])
            else:
                state['telegram_delivery_failures'] += 1
            state['symbols_ok'] += 1
        except Exception as exc:
            state['symbols_failed'] += 1
            if isinstance(exc, ValueError) and any(word in str(exc).lower() for word in ('candle','ohlcv','interval','stale','future','timestamp','high','low','volume','price')):
                state['data_validation_failures'] += 1
            state['last_error'] = f'{display_symbol}: {type(exc).__name__}: {exc}'
            log.warning('scan failed symbol=%s pair=%s error=%s', display_symbol, pair, exc)


async def scanner_session() -> None:
    session_started_ms = int(time.time() * 1000)
    state['session_started_at'] = datetime.fromtimestamp(session_started_ms / 1000, timezone.utc).isoformat()
    pairs = {symbol: symbol_to_pair(symbol) for symbol in SYMBOLS}
    feed = CoinDCXFuturesFeed(list(pairs.values()), timeout=15.0)
    telegram_client = httpx.AsyncClient(timeout=12.0)
    notifier = TelegramNotifier(telegram_client, settings.telegram_bot_token, settings.telegram_chat_id)
    state['telegram_configured'] = notifier.configured
    semaphore = asyncio.Semaphore(CONCURRENCY)
    try:
        startup_attempt = 0
        while True:
            try:
                active = await feed.active_instruments()
                state['market_metadata_ready'] = True
                state.pop('market_metadata_retry_at', None)
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                startup_attempt += 1
                state['market_metadata_ready'] = False
                state['last_error'] = f'CoinDCX active instruments attempt {startup_attempt}: {exc}'
                delay = min(300, 5 * (2 ** min(startup_attempt - 1, 6)))
                retry_at = datetime.fromtimestamp(time.time() + delay, timezone.utc).isoformat()
                state['market_metadata_retry_at'] = retry_at
                log.exception('CoinDCX active-instrument load failed; retrying in %ss at %s', delay, retry_at)
                await asyncio.sleep(delay)

        valid = [(symbol, pair) for symbol, pair in pairs.items() if pair in active]
        feed.configure_active_pairs([pair for _, pair in valid])
        missing = sorted(set(SYMBOLS) - {symbol for symbol, _ in valid})
        log.info('CoinDCX futures universe validated: configured=%d active=%d unavailable=%s', len(SYMBOLS), len(valid), missing)
        state['active_symbols'] = len(valid)
        state['unavailable_symbols'] = missing
        if len(valid) != len(SYMBOLS):
            log.warning('CoinDCX has %d of %d configured contracts; unavailable symbols will not be substituted', len(valid), len(SYMBOLS))
        if not valid:
            raise RuntimeError('No configured symbols are active on CoinDCX USDT futures')

        async def bootstrap(symbol: str, pair: str):
            try:
                async with semaphore:
                    await feed.fetch_history(pair, limit=320)
                log.info('CoinDCX history ready symbol=%s pair=%s', symbol, pair)
            except Exception as exc:
                state['last_error'] = f'history bootstrap {symbol}: {exc}'
                log.warning('CoinDCX history bootstrap failed symbol=%s pair=%s error=%s', symbol, pair, exc)

        await asyncio.gather(*(bootstrap(symbol, pair) for symbol, pair in valid))
        feed.start_websocket()
        # Wait for the websocket handshake and all subscriptions before the first
        # scan. The wait is bounded: REST history remains the fallback if the socket
        # is unavailable, so a websocket outage cannot stall the scanner forever.
        websocket_ready = await feed.wait_until_connected(timeout=20.0)
        state["websocket_connected"] = feed.websocket_connected
        if websocket_ready:
            log.info("CoinDCX websocket ready; all %d candle channels subscribed", len(valid))
        else:
            log.warning("CoinDCX websocket not ready within 20s; starting scan with REST history fallback")
        # Ignore any setup that formed while history bootstrap/socket startup ran.
        first_scan_cutoff_ms = int(time.time() * 1000)
        while True:
            started = time.monotonic()
            state.update(running=True, symbols_ok=0, symbols_failed=0, data_validation_failures=0)
            cycle_now_ms = int(time.time() * 1000)
            results = await asyncio.gather(*(scan_symbol(feed, notifier, semaphore, symbol, pair, cycle_now_ms, first_scan_cutoff_ms) for symbol, pair in valid), return_exceptions=True)
            unexpected = [str(r) for r in results if isinstance(r, BaseException)]
            state['scans'] += 1
            state['last_scan'] = datetime.now(timezone.utc).isoformat()
            state['last_scan_epoch'] = time.time()
            state['last_cycle_symbols_ok'] = state['symbols_ok']
            state['last_cycle_symbols_failed'] = state['symbols_failed']
            state['last_scan_seconds'] = round(time.monotonic() - started, 2)
            state['websocket_connected'] = feed.websocket_connected
            state['last_websocket_event'] = datetime.fromtimestamp(feed.last_ws_event, timezone.utc).isoformat() if feed.last_ws_event else None
            state['websocket_events_received'] = feed.ws_events_received
            state['last_websocket_pair'] = feed.last_ws_pair
            state['websocket_candle_fresh'] = bool(feed.last_ws_event and time.time() - feed.last_ws_event <= 90)
            state['websocket_connect_errors'] = feed.ws_connect_errors
            state['last_websocket_error'] = feed.last_ws_error
            state['websocket_disconnects'] = feed.ws_disconnects
            if unexpected:
                state['last_error'] = '; '.join(unexpected[:3])
            elif state['symbols_failed'] == 0:
                state['last_error'] = None
            log.info('CoinDCX scan complete cycle=%d ok=%d failed=%d signals=%d ws_connected=%s duration=%.1fs', state['scans'], state['symbols_ok'], state['symbols_failed'], state['signals_sent'], feed.websocket_connected, state['last_scan_seconds'])
            await asyncio.sleep(max(1, SCAN_SECONDS - (time.monotonic() - started)))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        state['last_error'] = f'scanner fatal: {exc}'
        log.exception('CoinDCX scanner stopped unexpectedly')
        raise
    finally:
        state['running'] = False
        state['websocket_connected'] = False
        await telegram_client.aclose()
        await feed.close()

async def scanner() -> None:
    """Supervise scanner sessions so one unexpected failure does not end scanning."""
    failures = 0
    while True:
        try:
            await scanner_session()
            failures = 0
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            state["running"] = False
            state["scanner_restarts"] += 1
            state["last_error"] = f"scanner restart: {type(exc).__name__}"
            delay = min(60, 2 ** min(failures, 6))
            log.error(
                "Scanner session failed; restarting attempt=%d delay=%ss error_type=%s",
                failures, delay, type(exc).__name__,
            )
            await asyncio.sleep(delay)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scanner(), name="market-scanner")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="CoinDCX Crypto Signal Bot", version="2.0.0", lifespan=lifespan)


def build_health_payload() -> dict[str, Any]:
    """Health reflects the last fully completed scan, not partial progress."""
    last_scan_epoch = state["last_scan_epoch"]
    scan_age = max(0, int(time.time() - last_scan_epoch)) if last_scan_epoch else None
    completed_symbols = state["last_cycle_symbols_ok"] + state["last_cycle_symbols_failed"]
    cycle_complete = bool(last_scan_epoch and completed_symbols == state["active_symbols"])
    scan_fresh = bool(scan_age is not None and scan_age <= max(180, SCAN_SECONDS * 3))
    healthy = bool(
        state["running"] and state["market_metadata_ready"]
        and state["active_symbols"] > 0 and cycle_complete and scan_fresh
        and state["last_cycle_symbols_failed"] == 0
        and state["last_cycle_symbols_ok"] == state["active_symbols"]
    )
    return {
        "ok": healthy,
        "service": "cryptobyance-signal-bot", "exchange": "CoinDCX Futures",
        "scanner_running": state["running"], "market_metadata_ready": state["market_metadata_ready"],
        "websocket_connected": state["websocket_connected"], "last_websocket_event": state["last_websocket_event"],
        "websocket_events_received": state["websocket_events_received"],
        "websocket_candle_fresh": state["websocket_candle_fresh"],
        "scanner_restarts": state["scanner_restarts"],
        "last_websocket_pair": state["last_websocket_pair"],
        "websocket_connect_errors": state["websocket_connect_errors"],
        "last_websocket_error": state["last_websocket_error"],
        "websocket_disconnects": state["websocket_disconnects"],
        "market_metadata_retry_at": state.get("market_metadata_retry_at"),
        "scans": state["scans"],
        "symbols_configured": len(SYMBOLS), "symbols_active": state["active_symbols"],
        "universe_complete": state["active_symbols"] == len(SYMBOLS),
        "symbols_unavailable": state["unavailable_symbols"],
        "symbols_ok_last_scan": state["last_cycle_symbols_ok"],
        "symbols_failed_last_scan": state["last_cycle_symbols_failed"],
        "data_validation_failures": state["data_validation_failures"],
        "signals_sent": state["signals_sent"],
        "telegram_delivery_failures": state["telegram_delivery_failures"],
        "last_telegram_success": state["last_telegram_success"],
        "telegram_configured": state["telegram_configured"],
        "last_scan": state["last_scan"], "last_scan_age_seconds": scan_age,
        "last_scan_seconds": state["last_scan_seconds"],
        "session_started_at": state["session_started_at"],
        "uptime_seconds": int(time.time() - state["started_at"]),
        "last_error": state["last_error"],
    }


@app.get("/health")
async def health():
    return build_health_payload()


@app.get("/ping")
async def ping():
    """Empty, lightweight keepalive response."""
    return Response(content="", status_code=200, media_type="text/plain")
