"""Single-engine CoinDCX USDT futures crypto signal bot. Signals only; never places orders."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Response

load_dotenv()

from coindcx_feed import CoinDCXFuturesFeed, aggregate_5m_to_15m, symbol_to_pair
from strategy import evaluate_setup, timeframe_milliseconds, validate_ohlcv
from candidate_store import CandidateStore
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

# Retry stale data briefly, but align normal scans to completed 5m candle closes.
SCAN_RETRY_SECONDS = min(settings.scan_interval_seconds, 20)
CANDLE_INTERVAL_SECONDS = 5 * 60
CANDLE_CLOSE_GRACE_SECONDS = 3
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
    "last_error": None, "running": False, "stale_candle_failures": 0,
    "active_symbols": 0, "unavailable_symbols": [], "data_validation_failures": 0,
    "market_metadata_ready": False, "websocket_connected": False, "last_websocket_event": None,
    "websocket_events_received": 0, "last_websocket_pair": None,
    "websocket_connect_errors": 0, "last_websocket_error": None, "websocket_disconnects": 0,
    "websocket_candle_fresh": False, "scanner_restarts": 0,
    "telegram_configured": False, "database_connected": False,
}
# In-process deduplication only. Startup cutoff prevents replaying candles that
# closed before the current scanner session began; state is not durable across restarts.
last_alert: dict[str, tuple[str, int]] = {}
last_seen_candle: dict[str, int] = {}


def seconds_until_next_candle_scan(now_epoch: float | None = None) -> float:
    """Delay until just after the next UTC-aligned 5m candle close."""
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    next_close = (int(now_epoch // CANDLE_INTERVAL_SECONDS) + 1) * CANDLE_INTERVAL_SECONDS
    target = next_close + CANDLE_CLOSE_GRACE_SECONDS
    return max(1.0, target - now_epoch)


def scanner_sleep_seconds(now_epoch: float | None = None, stale_data: bool = False) -> float:
    """Retry stale feeds during the fresh window; otherwise wait for next 5m close."""
    until_close = seconds_until_next_candle_scan(now_epoch)
    if stale_data:
        return max(1.0, min(float(SCAN_RETRY_SECONDS), until_close))
    return until_close

async def scan_symbol(feed, notifier, semaphore, display_symbol: str, pair: str, now_ms: int, session_started_ms: int | None = None, candidate_store=None) -> None:
    """Scan completed 5m candles; never wait for ADX or 15m confirmation."""
    async with semaphore:
        try:
            rows = feed.rows(pair)
            latest_ts = int(rows[-1][0]) if rows else 0
            if len(rows) < 60 or not latest_ts:
                await feed.fetch_history(pair, limit=320)
                rows = feed.rows(pair)

            # A completed candle older than 90 seconds is too late for an
            # actionable alert. Refresh once from REST before rejecting it.
            try:
                df5 = validate_ohlcv(rows, '5m', now_ms, max_age_ms=90_000)
            except ValueError as exc:
                if 'stale 5m candles' not in str(exc).lower():
                    raise
                await feed.fetch_history(pair, limit=6)
                rows = feed.rows(pair)
                df5 = validate_ohlcv(rows, '5m', now_ms, max_age_ms=90_000)
            if len(df5) < 60:
                raise ValueError(f'insufficient completed 5m candles: {len(df5)}')
            closed_ts = int(df5.iloc[-1].timestamp)
            five_minute_close = closed_ts + timeframe_milliseconds('5m')

            # Never emit a historical setup discovered during process startup.
            if session_started_ms is not None and five_minute_close <= session_started_ms:
                last_seen_candle[display_symbol] = closed_ts
                state['symbols_ok'] += 1
                return
            if last_seen_candle.get(display_symbol) == closed_ts:
                state['symbols_ok'] += 1
                return

            # 15m is deliberately not fetched or required: it cannot delay/veto 5m.
            setup = evaluate_setup(df5)
            if not setup:
                if candidate_store is not None:
                    # Retire legacy pending candidates so they can never promote later.
                    await candidate_store.invalidate(display_symbol)
                last_seen_candle[display_symbol] = closed_ts
                state['symbols_ok'] += 1
                return

            signal = {k: v for k, v in setup.items() if k != 'adx_satisfied'}
            signal['symbol'] = display_symbol
            signal['candle_close'] = five_minute_close
            signal['signal_generated_at'] = datetime.now(timezone.utc).isoformat()
            # Retire pending rows created by older releases; the current engine
            # never promotes a candidate after its crossover candle.
            if candidate_store is not None:
                await candidate_store.invalidate(display_symbol)
            if candidate_store is None:
                raise RuntimeError('persistent candidate store is required to queue a signal')
            # Durable idempotency, same-direction cooldown and outbox stay atomic.
            queued = await candidate_store.enqueue_signal(signal, None, COOLDOWN_CANDLES)
            last_seen_candle[display_symbol] = closed_ts
            if queued:
                state['signals_queued'] = state.get('signals_queued', 0) + 1
                log.info(
                    'SIGNAL QUEUED symbol=%s side=%s candle_close=%s entry=%s ADX=%.1f RSI=%.1f',
                    display_symbol, signal['side'],
                    datetime.fromtimestamp(five_minute_close / 1000, timezone.utc).isoformat(),
                    signal['entry'], signal['adx'], signal['rsi'],
                )
            state['symbols_ok'] += 1
        except Exception as exc:
            state['symbols_failed'] += 1
            if isinstance(exc, ValueError) and any(word in str(exc).lower() for word in ('candle','ohlcv','interval','stale','future','timestamp','high','low','volume','price')):
                state['data_validation_failures'] += 1
                if 'stale 5m candles' in str(exc).lower():
                    state['stale_candle_failures'] += 1
            state['last_error'] = f'{display_symbol}: {type(exc).__name__}: {exc}'
            log.warning('scan failed symbol=%s pair=%s error=%s', display_symbol, pair, exc)


async def telegram_outbox_loop(candidate_store: CandidateStore, notifier: TelegramNotifier) -> None:
    """Deliver durable queued alerts; expired leases are reclaimed after process restarts."""
    while True:
        try:
            # Do not consume durable retry attempts while Telegram credentials are absent.
            # Keep alerts PENDING so they can be delivered after configuration is fixed.
            if not notifier.configured:
                await asyncio.sleep(5.0)
                continue
            item = await candidate_store.claim_outbox()
            if item is None:
                await asyncio.sleep(1.0)
                continue
            delivered = await notifier.send(item["payload"])
            if delivered:
                await candidate_store.finish_outbox(item["id"])
                state["signals_sent"] += 1
                state["last_telegram_success"] = datetime.now(timezone.utc).isoformat()
                log.info("TELEGRAM OUTBOX SENT id=%s attempt=%d", item["id"], item["attempts"])
            else:
                await candidate_store.retry_outbox(item["id"], item["attempts"], "TelegramRejectedOrUnavailable")
                state["telegram_delivery_failures"] += 1
                log.warning("TELEGRAM OUTBOX RETRY id=%s attempt=%d", item["id"], item["attempts"])
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state["last_error"] = f"telegram outbox: {type(exc).__name__}"
            log.error("Telegram outbox worker error_type=%s", type(exc).__name__)
            await asyncio.sleep(3.0)


async def candidate_cleanup_loop(candidate_store: CandidateStore) -> None:
    """Expire stale pending candidates nightly; never delete persisted records."""
    async def run_cleanup() -> None:
        result = await candidate_store.cleanup(int(time.time() * 1000))
        log.info(
            "PostgreSQL retention sweep complete expired_candidates=%d deleted_records=%d",
            result["expired"], result["deleted"],
        )

    # Startup cleanup catches up if the free web service slept through 02:00.
    try:
        await run_cleanup()
    except Exception:
        log.exception("PostgreSQL startup cleanup failed; will retry at the next nightly run")

    while True:
        now = datetime.now(timezone.utc)
        next_run = now.replace(hour=20, minute=30, second=0, microsecond=0)
        if next_run <= now:
            next_run += timedelta(days=1)
        await asyncio.sleep(max(1.0, (next_run - now).total_seconds()))
        try:
            await run_cleanup()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("PostgreSQL nightly cleanup failed; scanner will continue")


async def scanner_session() -> None:
    session_started_ms = int(time.time() * 1000)
    state['session_started_at'] = datetime.fromtimestamp(session_started_ms / 1000, timezone.utc).isoformat()
    pairs = {symbol: symbol_to_pair(symbol) for symbol in SYMBOLS}
    if not settings.database_url:
        raise RuntimeError('DATABASE_URL is required for persistent signal candidates; set the Render internal PostgreSQL URL')
    # Connect persistence before allocating network clients so a missing or
    # unavailable database cannot leak HTTP/Socket.IO clients on each retry.
    candidate_store = await CandidateStore.connect(settings.database_url)
    state['database_connected'] = True
    try:
        previous_checkpoint = await candidate_store.load_runtime()
        state["previous_runtime_checkpoint"] = previous_checkpoint
        if previous_checkpoint:
            log.info("Loaded previous PostgreSQL scanner checkpoint updated_at=%s",
                     previous_checkpoint["updated_at"])
    except Exception as exc:
        state["last_error"] = f"previous database checkpoint unavailable: {type(exc).__name__}"
        log.warning("Could not load previous PostgreSQL checkpoint error_type=%s", type(exc).__name__)
    feed = CoinDCXFuturesFeed(list(pairs.values()), timeout=15.0)
    telegram_client = httpx.AsyncClient(timeout=12.0)
    notifier = TelegramNotifier(telegram_client, settings.telegram_bot_token, settings.telegram_chat_id)
    state['telegram_configured'] = notifier.configured
    semaphore = asyncio.Semaphore(CONCURRENCY)
    cleanup_task = asyncio.create_task(candidate_cleanup_loop(candidate_store), name="postgres-nightly-cleanup")
    outbox_task = asyncio.create_task(telegram_outbox_loop(candidate_store, notifier), name="telegram-outbox")
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
            state.update(running=True, symbols_ok=0, symbols_failed=0, data_validation_failures=0, stale_candle_failures=0)
            cycle_now_ms = int(time.time() * 1000)
            results = await asyncio.gather(*(scan_symbol(feed, notifier, semaphore, symbol, pair, cycle_now_ms, first_scan_cutoff_ms, candidate_store) for symbol, pair in valid), return_exceptions=True)
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
            try:
                await candidate_store.persist_runtime({
                    "scanner_running": state["running"], "scans": state["scans"],
                    "last_scan": state["last_scan"], "symbols_ok": state["last_cycle_symbols_ok"],
                    "symbols_failed": state["last_cycle_symbols_failed"],
                    "websocket_connected": feed.websocket_connected,
                    "websocket_events_received": feed.ws_events_received,
                    "signals_queued": state.get("signals_queued", 0),
                    "signals_sent": state["signals_sent"],
                    "last_error": state["last_error"],
                })
            except Exception as exc:
                log.error("Scanner runtime checkpoint failed error_type=%s", type(exc).__name__)
            log.info('CoinDCX scan complete cycle=%d ok=%d failed=%d signals=%d ws_connected=%s duration=%.1fs', state['scans'], state['symbols_ok'], state['symbols_failed'], state['signals_sent'], feed.websocket_connected, state['last_scan_seconds'])
            stale_data = state['stale_candle_failures'] > 0
            delay = scanner_sleep_seconds(stale_data=stale_data)
            log.info('Next scan scheduled delay=%.1fs reason=%s', delay, 'stale-data-retry' if stale_data else 'next-5m-close')
            await asyncio.sleep(delay)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        state['last_error'] = f'scanner fatal: {exc}'
        log.exception('CoinDCX scanner stopped unexpectedly')
        raise
    finally:
        state['running'] = False
        state['websocket_connected'] = False
        state['database_connected'] = False
        cleanup_task.cancel()
        outbox_task.cancel()
        for background_task in (cleanup_task, outbox_task):
            try:
                await background_task
            except asyncio.CancelledError:
                pass
        await telegram_client.aclose()
        await candidate_store.close()
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
            detail = str(exc)
            if settings.database_url:
                detail = detail.replace(settings.database_url, "[redacted database URL]")
            state["last_error"] = f"scanner restart: {type(exc).__name__}: {detail}"
            delay = min(60, 2 ** min(failures, 6))
            log.error(
                "Scanner session failed; restarting attempt=%d delay=%ss error_type=%s detail=%s",
                failures, delay, type(exc).__name__, detail,
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
        "stale_candle_failures": state["stale_candle_failures"],
        "signals_sent": state["signals_sent"],
        "telegram_delivery_failures": state["telegram_delivery_failures"],
        "last_telegram_success": state["last_telegram_success"],
        "telegram_configured": state["telegram_configured"], "database_connected": state["database_connected"],
        "signals_queued": state.get("signals_queued", 0),
        "previous_runtime_checkpoint": state.get("previous_runtime_checkpoint"),
        "last_scan": state["last_scan"], "last_scan_age_seconds": scan_age,
        "last_scan_seconds": state["last_scan_seconds"],
        "session_started_at": state["session_started_at"],
        "uptime_seconds": int(time.time() - state["started_at"]),
        "last_error": state["last_error"],
    }


@app.get("/health")
async def health():
    return build_health_payload()


@app.get("/ping", response_class=Response)
async def ping() -> Response:
    """Return an empty 204 response; do not perform scanner or external work."""
    return Response(status_code=204)
