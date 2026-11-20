"""Single-engine CoinDCX USDT futures crypto signal bot. Signals only; never places orders."""
from __future__ import annotations

import asyncio
import csv
import hmac
import io
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from dataclasses import replace
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel

load_dotenv()

from coindcx_feed import CoinDCXFuturesFeed, aggregate_5m_to_15m, symbol_to_pair
from strategy import evaluate_setup, evaluate_state_inputs, indicators, timeframe_milliseconds, validate_ohlcv
from journal import DASHBOARD_HTML, SignalJournal, rejected_observation
from candidate_store import CandidateStore
from telegram import TelegramNotifier

from config import settings, SYMBOLS, STRATEGY_KEY, CONFIG_VERSION

LOG_LEVEL = settings.log_level
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("cryptobyance")

# Fixed, deterministic universe: liquid USDT-margined perpetual contracts.
assert len(SYMBOLS) == 60 and len(set(SYMBOLS)) == 60

# Scan only around completed 5m candle closes. Stale data waits for the
# next close rather than repeatedly retrying the same expired candle.
CANDLE_INTERVAL_SECONDS = 5 * 60
CANDLE_CLOSE_GRACE_SECONDS = 3
CONCURRENCY = settings.concurrency
COOLDOWN_CANDLES = settings.signal_cooldown_candles
TIMEFRAME = settings.timeframe
CONTEXT_TF = settings.context_timeframe

RUNTIME_CONFIG_KEYS = (
    "scan_interval_seconds", "concurrency", "signal_cooldown_candles",
    "timeframe", "context_timeframe", "adx_min", "ema_fast", "ema_slow",
    "rsi_period", "adx_period", "rsi_long_min", "rsi_long_max",
    "rsi_short_min", "rsi_short_max", "context_rsi_midline",
    "swing_lookback_bars", "tp1_r", "tp2_r", "atr_stop_multiplier",
    "min_risk_reward", "outcome_horizon_bars", "outcome_cost_pct",
    "training_min_samples", "training_min_bucket_samples", "log_level",
)


def _runtime_config_payload(runtime_settings) -> dict:
    return {key: getattr(runtime_settings, key) for key in RUNTIME_CONFIG_KEYS}


def apply_runtime_config(config_payload: dict, version: str) -> None:
    """Hot-reload approved settings consistently across imported strategy modules."""
    global settings, CONFIG_VERSION, COOLDOWN_CANDLES, CONCURRENCY, TIMEFRAME, CONTEXT_TF, LOG_LEVEL
    import config as config_module
    import strategy as strategy_module
    import candidate_store as candidate_store_module
    import journal as journal_module

    values = {key: config_payload[key] for key in RUNTIME_CONFIG_KEYS if key in config_payload}
    updated = replace(settings, **values)
    if (updated.rsi_long_min >= updated.rsi_long_max
            or updated.rsi_short_min >= updated.rsi_short_max
            or updated.tp2_r <= updated.tp1_r):
        raise ValueError("approved runtime configuration has invalid RSI or target bands")

    settings = updated
    CONFIG_VERSION = str(version)
    config_module.settings = updated
    config_module.CONFIG_VERSION = str(version)
    strategy_module.settings = updated
    strategy_module.ADX_MIN = updated.adx_min
    strategy_module.EMA_FAST = updated.ema_fast
    strategy_module.EMA_SLOW = updated.ema_slow
    strategy_module.RSI_PERIOD = updated.rsi_period
    strategy_module.ADX_PERIOD = updated.adx_period
    strategy_module.ATR_STOP_MULT = updated.atr_stop_multiplier
    strategy_module.MIN_RR = updated.min_risk_reward
    candidate_store_module.settings = updated
    journal_module.ADX_MIN = updated.adx_min
    journal_module.ATR_STOP_MULT = updated.atr_stop_multiplier
    journal_module.MIN_RR = updated.min_risk_reward
    COOLDOWN_CANDLES = updated.signal_cooldown_candles
    CONCURRENCY = updated.concurrency
    TIMEFRAME = updated.timeframe
    CONTEXT_TF = updated.context_timeframe
    LOG_LEVEL = updated.log_level
    logging.getLogger().setLevel(updated.log_level)
    state["config_version"] = str(version)


state: dict[str, Any] = {
    "started_at": time.time(), "session_started_at": None, "scans": 0, "last_scan": None,
    "last_scan_seconds": None, "last_scan_epoch": None,
    "last_cycle_symbols_ok": 0, "last_cycle_symbols_failed": 0,
    "symbols_ok": 0, "symbols_failed": 0,
    "signals_sent": 0, "telegram_delivery_failures": 0, "last_telegram_success": None,
    "last_error": None, "running": False, "stale_candle_failures": 0, "stale_candle_sample": None,
    "active_symbols": 0, "unavailable_symbols": [], "data_validation_failures": 0,
    "market_metadata_ready": False, "websocket_connected": False, "last_websocket_event": None,
    "websocket_events_received": 0, "last_websocket_pair": None,
    "websocket_connect_errors": 0, "last_websocket_error": None, "websocket_disconnects": 0,
    "websocket_candle_fresh": False, "scanner_restarts": 0,
    "telegram_configured": False, "database_connected": False,
    "outcome_tracking_errors": 0, "outcome_rows_updated": 0,
    "performance_report_errors": 0, "performance_summary": None,
}
# In-process deduplication only. Startup cutoff prevents replaying candles that
# closed before the current scanner session began; state is not durable across restarts.
last_alert: dict[str, tuple[str, int]] = {}
last_seen_candle: dict[str, int] = {}
journal_service: SignalJournal | None = None


class FeedbackRequest(BaseModel):
    signal_id: int
    category: str
    comment: str = ""

class CandidateFeedbackRequest(BaseModel):
    candidate_id: int
    category: str = "MISSED_OPPORTUNITY"
    comment: str = ""

class RelabelRequest(BaseModel):
    signal_id: int
    category: str
    comment: str = ""


class TrainingInstructionRequest(BaseModel):
    instruction: str


class TrainingInstructionReviewRequest(BaseModel):
    decision: str
    reviewer_note: str = ""


class ManualObservationRequest(BaseModel):
    symbol: str
    direction: str
    candle_open_ms: int
    entry: float
    stop: float
    target: float
    comment: str = ""


def seconds_until_next_candle_scan(now_epoch: float | None = None) -> float:
    """Delay until just after the next UTC-aligned 5m candle close."""
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    next_close = (int(now_epoch // CANDLE_INTERVAL_SECONDS) + 1) * CANDLE_INTERVAL_SECONDS
    target = next_close + CANDLE_CLOSE_GRACE_SECONDS
    return max(1.0, target - now_epoch)


def scanner_sleep_seconds(now_epoch: float | None = None, stale_data: bool = False) -> float:
    """Schedule the next scan at the next completed 5m candle close."""
    now_epoch = time.time() if now_epoch is None else float(now_epoch)
    # Once a candle is older than the 90s actionability window, retrying it
    # every few seconds cannot make it valid again. Wait for the next close
    # plus grace; scan_symbol still performs one REST refresh and enforces
    # the unchanged strict 90s freshness limit.
    return seconds_until_next_candle_scan(now_epoch)

def _effective_candle_clock(reference_ms: int) -> int:
    """Never validate market data against an earlier cycle-start timestamp."""
    return max(int(reference_ms), int(time.time() * 1000))


async def scan_symbol(feed, notifier, semaphore, display_symbol: str, pair: str, now_ms: int, session_started_ms: int | None = None, candidate_store=None, journal=None) -> None:
    """Scan completed 5m candles; 15m EMA trend is a mandatory direction veto."""
    async with semaphore:
        rows = []
        try:
            # Require one REST-confirmed final OHLCV for the newly closed 5m
            # candle per pair/interval. Socket.IO may have supplied only a
            # provisional last update at the instant the candle closed.
            confirm_closed = getattr(feed, 'refresh_latest_closed_candle', None)
            if confirm_closed is not None:
                await confirm_closed(pair, _effective_candle_clock(now_ms))
            rows = feed.rows(pair)
            latest_ts = int(rows[-1][0]) if rows else 0
            if len(rows) < 60 or not latest_ts:
                await feed.fetch_history(pair, limit=320)
                rows = feed.rows(pair)

            # A completed candle older than 90 seconds is too late for an
            # actionable alert. Refresh once from REST before rejecting it.
            try:
                df5 = validate_ohlcv(rows, '5m', _effective_candle_clock(now_ms), max_age_ms=90_000)
            except ValueError as exc:
                if 'stale 5m candles' not in str(exc).lower():
                    raise
                await feed.fetch_history(pair, limit=6)
                rows = feed.rows(pair)
                df5 = validate_ohlcv(rows, '5m', _effective_candle_clock(now_ms), max_age_ms=90_000)
            if len(df5) < 60:
                raise ValueError(f'insufficient completed 5m candles: {len(df5)}')
            closed_ts = int(df5.iloc[-1].timestamp)
            five_minute_close = closed_ts + timeframe_milliseconds('5m')

            # Outcome tracking is observational and must never block signal generation.
            # Only later completed 5m candles are considered by the store.
            if candidate_store is not None:
                try:
                    updated_outcomes = await candidate_store.update_open_outcomes(
                        display_symbol, df5.to_dict("records")
                    )
                    state["outcome_rows_updated"] += int(updated_outcomes)
                    if journal is not None:
                        await journal.update_rejected_outcomes(display_symbol, df5.to_dict("records"))
                except Exception as outcome_error:
                    state["outcome_tracking_errors"] = state.get("outcome_tracking_errors", 0) + 1
                    log.warning(
                        "outcome tracking failed symbol=%s error_type=%s",
                        display_symbol, type(outcome_error).__name__,
                    )

            # Never emit a historical setup discovered during process startup.
            if session_started_ms is not None and five_minute_close <= session_started_ms:
                last_seen_candle[display_symbol] = closed_ts
                state['symbols_ok'] += 1
                return
            if last_seen_candle.get(display_symbol) == closed_ts:
                state['symbols_ok'] += 1
                return

            # Build mandatory 15m trend context from aligned groups of three completed
            # 5m candles. Missing, stale, gapped, or insufficient 15m data is
            # passed as None and the strategy safely vetoes every signal.
            df15 = None
            try:
                validation_now_ms = _effective_candle_clock(now_ms)
                # Revalidate the primary candle after any awaited outcome work.
                # This prevents a slow symbol scan from using the cycle-start clock.
                df5 = validate_ohlcv(rows, '5m', validation_now_ms, max_age_ms=90_000)
                if int(df5.iloc[-1].timestamp) != closed_ts:
                    raise ValueError('latest 5m candle changed during scan; defer to next cycle')
                rows15 = aggregate_5m_to_15m(rows, validation_now_ms)
                if len(rows15) >= 35:
                    candidate15 = validate_ohlcv(rows15, '15m', validation_now_ms)
                    if len(candidate15) >= 35:
                        df15 = candidate15
            except ValueError as context_error:
                log.debug('15m mandatory trend unavailable; signals vetoed symbol=%s reason=%s', display_symbol, context_error)
            # Backward-compatible path for isolated scanner adapters/tests that
            # do not implement the v2 durable state API. The deployed
            # CandidateStore always takes the staged branch below.
            if candidate_store is None or not hasattr(candidate_store, "load_symbol_states"):
                setup = evaluate_setup(df5, df15)
                if not setup or candidate_store is None:
                    last_seen_candle[display_symbol] = closed_ts
                    state['symbols_ok'] += 1
                    return
            else:
                persisted_states = await candidate_store.load_symbol_states(
                    STRATEGY_KEY, display_symbol
                )
                armed_sides = {side for side, value in persisted_states.items() if value == "ARMED"}
                state_inputs = evaluate_state_inputs(df5, df15, armed_sides=armed_sides)
                if state_inputs is None:
                    log.warning("state scan deferred because indicator context is incomplete symbol=%s", display_symbol)
                    last_seen_candle[display_symbol] = closed_ts
                    state['symbols_ok'] += 1
                    return
                triggered_side = None
                for side in ("LONG", "SHORT"):
                    item = state_inputs[side]
                    action = await candidate_store.advance_symbol_state(
                        strategy_key=STRATEGY_KEY,
                        symbol=display_symbol, direction=side,
                        candle_open_ms=closed_ts,
                        setup_valid=item["setup_valid"],
                        invalidation=item["invalidation"],
                        trigger=item["trigger"],
                        cooldown_candles=COOLDOWN_CANDLES,
                    )
                    if action == "TRIGGER":
                        triggered_side = side
                if triggered_side is None:
                    if journal is not None and armed_sides:
                        try:
                            observation = rejected_observation(df5, df15)
                            if observation:
                                observation["symbol"] = display_symbol
                                await journal.record_rejected(observation)
                        except Exception as journal_error:
                            log.warning('rejected-candidate journal failed symbol=%s error_type=%s', display_symbol, type(journal_error).__name__)
                    last_seen_candle[display_symbol] = closed_ts
                    state['symbols_ok'] += 1
                    return
                setup = evaluate_setup(df5, df15)
                if not setup or setup.get("side") != triggered_side:
                    log.warning("state trigger lacked matching executable signal symbol=%s side=%s", display_symbol, triggered_side)
                    last_seen_candle[display_symbol] = closed_ts
                    state['symbols_ok'] += 1
                    return
            signal = {k: v for k, v in setup.items() if k != 'adx_satisfied'}
            signal['symbol'] = display_symbol
            signal['config_version'] = CONFIG_VERSION
            signal['candle_close'] = five_minute_close
            signal['signal_generated_at'] = datetime.now(timezone.utc).isoformat()
            # Point-in-time research snapshot: telemetry only; never feeds back into entry rules.
            try:
                ind = indicators(df5.copy())
                current = ind.iloc[-1]
                close_series = df5["close"].astype(float)
                macd_line = close_series.ewm(span=12, adjust=False).mean() - close_series.ewm(span=26, adjust=False).mean()
                macd_signal = macd_line.ewm(span=9, adjust=False).mean()
                volume_mean = df5["volume"].rolling(20, min_periods=20).mean().iloc[-1]
                signal["indicator_snapshot"] = {
                    "version": 1, "ema9": float(current.ema9), "ema21": float(current.ema21),
                    "rsi14": float(current.rsi), "adx14": float(current.adx),
                    "plus_di": float(current.plus_di), "minus_di": float(current.minus_di),
                    "vwap": float(current.vwap), "atr14": float(current.atr),
                    "relative_volume_20": float(df5["volume"].iloc[-1] / volume_mean) if volume_mean and volume_mean > 0 else None,
                    "macd_line": float(macd_line.iloc[-1]), "macd_signal": float(macd_signal.iloc[-1]),
                    "macd_histogram": float(macd_line.iloc[-1] - macd_signal.iloc[-1]),
                    "open": float(current.open), "high": float(current.high),
                    "low": float(current.low), "close": float(current.close),
                    "volume": float(current.volume), "context_15m": signal.get("context"),
                    "context_rsi14": signal.get("context_rsi14"),
                    "context_rsi_aligned": signal.get("context_rsi_aligned"),
                    "context_rsi_score": signal.get("context_rsi_score"),
                }
            except Exception as snapshot_error:
                log.warning("signal snapshot enrichment failed symbol=%s error_type=%s", display_symbol, type(snapshot_error).__name__)
            if candidate_store is None:
                raise RuntimeError('persistent candidate store is required to queue a signal')
            # Durable idempotency, same-direction cooldown and outbox stay atomic.
            # DB work above may also take time. Recheck immediately before the
            # durable enqueue so a candle that aged out during processing cannot alert.
            final_df5 = validate_ohlcv(rows, '5m', _effective_candle_clock(now_ms), max_age_ms=90_000)
            if int(final_df5.iloc[-1].timestamp) != closed_ts:
                raise ValueError('latest 5m candle changed before signal enqueue; defer to next cycle')
            queued = await candidate_store.enqueue_signal(
                signal, None, COOLDOWN_CANDLES,
                strategy_key=STRATEGY_KEY,
            )
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
                    if state.get('stale_candle_sample') is None:
                        latest_open_ms = int(rows[-1][0]) if rows else None
                        latest_close_ms = latest_open_ms + timeframe_milliseconds('5m') if latest_open_ms is not None else None
                        state['stale_candle_sample'] = {
                            'symbol': display_symbol, 'pair': pair,
                            'latest_open_ms': latest_open_ms,
                            'latest_close_ms': latest_close_ms,
                            'age_seconds': round((now_ms - latest_close_ms) / 1000, 1) if latest_close_ms is not None else None,
                            'websocket_last_event': state.get('last_websocket_event'),
                            'websocket_last_pair': state.get('last_websocket_pair'),
                        }
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
            delivered = await notifier.send(item["payload"], signal_id=item.get("signal_id"))
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


async def daily_training_review_loop(journal: SignalJournal) -> None:
    """Create one durable research-only review per 24-hour cycle."""
    while True:
        try:
            summary = await journal.daily_training_review()
            await journal.save_daily_training_review(summary)
            log.info("24-hour training review saved decision=%s resolved=%s",
                     summary.get("decision"), summary.get("resolved_sample"))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("24-hour training review failed error_type=%s", type(exc).__name__)
        await asyncio.sleep(24 * 60 * 60)


async def scanner_session() -> None:
    global journal_service
    session_started_ms = int(time.time() * 1000)
    state['session_started_at'] = datetime.fromtimestamp(session_started_ms / 1000, timezone.utc).isoformat()
    pairs = {symbol: symbol_to_pair(symbol) for symbol in SYMBOLS}
    if not settings.database_url:
        raise RuntimeError('DATABASE_URL is required for persistent signal candidates; set the Render internal PostgreSQL URL')
    # Connect persistence before allocating network clients so a missing or
    # unavailable database cannot leak HTTP/Socket.IO clients on each retry.
    candidate_store = await CandidateStore.connect(settings.database_url)
    try:
        journal_service = SignalJournal(candidate_store)
        await journal_service.initialize()
        await journal_service.ensure_initial_config(
            CONFIG_VERSION, _runtime_config_payload(settings)
        )
        active_config = await journal_service.active_config()
        if active_config:
            apply_runtime_config(active_config["config"], active_config["version"])
    except BaseException:
        await candidate_store.close()
        raise
    state['database_connected'] = True
    try:
        state["performance_summary"] = await candidate_store.performance_summary()
    except Exception as report_error:
        state["performance_report_errors"] = state.get("performance_report_errors", 0) + 1
        log.warning("Initial performance summary unavailable error_type=%s", type(report_error).__name__)
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
    training_review_task = asyncio.create_task(daily_training_review_loop(journal_service), name="daily-training-review")
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
            state.update(running=True, symbols_ok=0, symbols_failed=0, data_validation_failures=0, stale_candle_failures=0, stale_candle_sample=None)
            cycle_now_ms = int(time.time() * 1000)
            results = await asyncio.gather(*(scan_symbol(feed, notifier, semaphore, symbol, pair, cycle_now_ms, first_scan_cutoff_ms, candidate_store, journal_service) for symbol, pair in valid), return_exceptions=True)
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
            try:
                state["performance_summary"] = await candidate_store.performance_summary()
                totals = state["performance_summary"].get("totals", {})
                log.info(
                    "PERFORMANCE REPORT delivered=%s tracked=%s target=%s stop_loss=%s ambiguous=%s open=%s avg_realized_r=%s data_gaps=%s sent_without_outcome=%s",
                    totals.get("delivered_signals", 0), totals.get("tracked_outcomes", 0),
                    totals.get("targets", 0), totals.get("stop_losses", 0),
                    totals.get("ambiguous", 0), totals.get("open", 0),
                    totals.get("avg_realized_r"), totals.get("data_gaps", 0),
                    totals.get("sent_without_outcome", 0),
                )
                risk_rows = {
                    row.get("status"): row
                    for row in state["performance_summary"].get("risk_by_outcome", [])
                }
                stop_profile = risk_rows.get("STOP_LOSS", {})
                target_profile = risk_rows.get("TARGET", {})
                log.info(
                    "GLOBAL STOP_TARGET PROFILE stop_n=%s stop_avg_mfe_r=%s stop_median_mfe_r=%s "
                    "stop_mfe_050r_before_exit=%s stop_mfe_100r_before_exit=%s "
                    "target_n=%s target_avg_mae_r=%s target_median_mae_r=%s "
                    "target_mae_050r_before_exit=%s target_mae_100r_before_exit=%s",
                    stop_profile.get("count", 0), stop_profile.get("avg_mfe_r"),
                    stop_profile.get("median_mfe_r"),
                    stop_profile.get("stop_mfe_050r_before_exit", 0),
                    stop_profile.get("stop_mfe_100r_before_exit", 0),
                    target_profile.get("count", 0), target_profile.get("avg_mae_r"),
                    target_profile.get("median_mae_r"),
                    target_profile.get("target_mae_050r_before_exit", 0),
                    target_profile.get("target_mae_100r_before_exit", 0),
                )
            except Exception as report_error:
                state["performance_report_errors"] = state.get("performance_report_errors", 0) + 1
                log.warning("Performance summary refresh failed error_type=%s", type(report_error).__name__)
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
                    "outcome_rows_updated": state.get("outcome_rows_updated", 0),
                    "outcome_tracking_errors": state.get("outcome_tracking_errors", 0),
                    "signals_sent": state["signals_sent"],
                    "last_error": state["last_error"],
                })
            except Exception as exc:
                log.error("Scanner runtime checkpoint failed error_type=%s", type(exc).__name__)
            log.info('CoinDCX scan complete cycle=%d ok=%d failed=%d signals_queued_total=%d signals_sent_total=%d ws_connected=%s duration=%.1fs', state['scans'], state['symbols_ok'], state['symbols_failed'], state.get('signals_queued', 0), state['signals_sent'], feed.websocket_connected, state['last_scan_seconds'])
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
        training_review_task.cancel()
        for background_task in (cleanup_task, outbox_task, training_review_task):
            try:
                await background_task
            except asyncio.CancelledError:
                pass
        await telegram_client.aclose()
        await candidate_store.close()
        journal_service = None
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
    scan_fresh = bool(scan_age is not None and scan_age <= max(180, CANDLE_INTERVAL_SECONDS + CANDLE_CLOSE_GRACE_SECONDS + 60))
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
        "stale_candle_sample": state["stale_candle_sample"],
        "signals_sent": state["signals_sent"],
        "telegram_delivery_failures": state["telegram_delivery_failures"],
        "last_telegram_success": state["last_telegram_success"],
        "telegram_configured": state["telegram_configured"], "database_connected": state["database_connected"],
        "signals_queued": state.get("signals_queued", 0),
        "outcome_rows_updated": state.get("outcome_rows_updated", 0),
        "outcome_tracking_errors": state.get("outcome_tracking_errors", 0),
        "performance_report_errors": state.get("performance_report_errors", 0),
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


@app.get("/performance")
async def performance():
    """Aggregate signal outcomes only; never return entries, stops, targets or Telegram payloads."""
    return {
        "available": state.get("performance_summary") is not None,
        "updated_at": state.get("last_scan"),
        "report_errors": state.get("performance_report_errors", 0),
        "summary": state.get("performance_summary"),
    }


@app.get("/journal", response_class=Response)
async def journal_dashboard() -> Response:
    return Response(content=DASHBOARD_HTML, media_type="text/html")


def require_journal_token(request: Request) -> None:
    expected = os.getenv("JOURNAL_API_TOKEN", "").strip()
    supplied = request.headers.get("x-journal-token", "")
    authorization = request.headers.get("authorization", "")
    if not supplied and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if not expected:
        raise HTTPException(status_code=503, detail="JOURNAL_API_TOKEN is not configured")
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid journal token")


def get_journal() -> SignalJournal:
    if journal_service is None:
        raise HTTPException(status_code=503, detail="Signal journal is starting or temporarily unavailable")
    return journal_service


@app.get("/journal/api/entries")
async def journal_entries(request: Request, limit: int = 100):
    require_journal_token(request)
    return await get_journal().entries(limit)


@app.get("/journal/api/candidates")
async def journal_candidates(request: Request, limit: int = 100):
    require_journal_token(request)
    return await get_journal().candidates(limit)


@app.post("/journal/api/feedback")
async def journal_feedback(payload: FeedbackRequest, request: Request):
    require_journal_token(request)
    try:
        feedback_id = await get_journal().add_feedback(payload.signal_id, payload.category, payload.comment, "dashboard")
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"saved": True, "feedback_id": feedback_id}


@app.post("/journal/api/candidate-feedback")
async def journal_candidate_feedback(payload: CandidateFeedbackRequest, request: Request):
    require_journal_token(request)
    try:
        feedback_id = await get_journal().add_candidate_feedback(
            payload.candidate_id, payload.category, payload.comment, "dashboard"
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"saved": True, "feedback_id": feedback_id}

@app.post("/journal/api/relabel")
async def journal_relabel(payload: RelabelRequest, request: Request):
    require_journal_token(request)
    try:
        feedback_id = await get_journal().relabel_signal(
            payload.signal_id, payload.category, payload.comment
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"saved": True, "relabelled": True, "feedback_id": feedback_id}


@app.get("/journal/api/pending")
async def journal_pending(request: Request, limit: int = 100):
    require_journal_token(request)
    return await get_journal().pending_entries(limit)


@app.get("/journal/api/stats")
async def journal_stats(request: Request, days: int = 30):
    require_journal_token(request)
    if days < 1 or days > 365:
        raise HTTPException(status_code=422, detail="days must be between 1 and 365")
    return await get_journal().stats(days)


@app.post("/journal/api/observation")
async def journal_manual_observation(payload: ManualObservationRequest, request: Request):
    require_journal_token(request)
    try:
        candidate_id = await get_journal().record_manual_opportunity(
            payload.symbol, payload.direction, payload.candle_open_ms,
            payload.entry, payload.stop, payload.target, payload.comment
        )
        await get_journal().add_candidate_feedback(candidate_id, "MISSED_OPPORTUNITY", payload.comment, "dashboard")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {"saved": True, "candidate_id": candidate_id}


@app.post("/journal/api/train")
async def run_offline_training(request: Request):
    require_journal_token(request)
    return await get_journal().train()


@app.get("/journal/api/proposals")
async def list_training_proposals(request: Request, limit: int = 20):
    require_journal_token(request)
    return await get_journal().training_proposals(limit)


@app.post("/journal/api/proposals/{proposal_id}/approve")
async def approve_training_proposal(proposal_id: int, request: Request):
    require_journal_token(request)
    try:
        result = await get_journal().approve_proposal(proposal_id, "journal_api")
        apply_runtime_config(result["config"], result["version"])
        return {"approved": True, **result}
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/journal/api/proposals/{proposal_id}/reject")
async def reject_training_proposal(proposal_id: int, request: Request):
    require_journal_token(request)
    try:
        return await get_journal().reject_proposal(proposal_id, "journal_api")
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/journal/api/rollback")
async def rollback_runtime_config(request: Request):
    require_journal_token(request)
    try:
        result = await get_journal().rollback_config("journal_api")
        apply_runtime_config(result["config"], result["version"])
        return {"rolled_back": True, **result}
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/journal/api/config-history")
async def journal_config_history(request: Request, limit: int = 30):
    require_journal_token(request)
    return await get_journal().config_history(limit)


@app.post("/journal/api/training-instructions")
async def add_training_instruction(payload: TrainingInstructionRequest, request: Request):
    require_journal_token(request)
    try:
        return await get_journal().add_training_instruction(payload.instruction)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/journal/api/training-instructions")
async def list_training_instructions(request: Request, limit: int = 100):
    require_journal_token(request)
    return await get_journal().training_instructions(limit)


@app.post("/journal/api/training-instructions/{instruction_id}/review")
async def review_training_instruction(instruction_id: int, payload: TrainingInstructionReviewRequest,
                                      request: Request):
    require_journal_token(request)
    try:
        return await get_journal().review_training_instruction(
            instruction_id, payload.decision, payload.reviewer_note)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/learning/daily-review")
async def daily_learning_review(request: Request):
    require_journal_token(request)
    return await get_journal().daily_training_review()


@app.get("/journal/export")
async def journal_export(request: Request):
    require_journal_token(request)
    journal = get_journal()
    payload = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "signals": await journal.entries(5000),
        "rejected_candidates": await journal.candidates(5000),
        "learning": await journal.learning_summary(),
    }
    return Response(
        content=json.dumps(payload, default=str, ensure_ascii=False, indent=2),
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="cryptobyance-journal.json"'},
    )

@app.get("/journal/export.csv")
async def journal_export_csv(request: Request):
    require_journal_token(request)
    entries = await get_journal().entries(5000)
    output = io.StringIO()
    fields = [
        "signal_id", "symbol", "direction", "delivered_at", "config_version",
        "entry", "stop", "target", "target1", "target2", "atr",
        "adx", "rsi", "ema9", "ema21", "vwap", "relative_volume_20",
        "outcome_status", "first_hit", "result", "realized_r",
        "best_favorable_r", "worst_adverse_r", "best_favorable_pct",
        "worst_adverse_pct", "best_favorable_atr", "worst_adverse_atr",
        "feedback_labels", "feedback_notes", "feature_snapshot",
    ]
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for item in entries:
        payload = item.get("payload") or {}
        snapshot = payload.get("indicator_snapshot") or payload.get("diagnostics") or {}
        feedback = item.get("feedback") or []
        writer.writerow({
            "signal_id": item.get("id"),
            "symbol": item.get("symbol"),
            "direction": item.get("direction"),
            "delivered_at": item.get("delivered_at"),
            "config_version": payload.get("config_version"),
            "entry": payload.get("entry"),
            "stop": payload.get("stop"),
            "target": payload.get("target"),
            "target1": payload.get("target1"),
            "target2": payload.get("target2"),
            "atr": payload.get("atr"),
            "adx": payload.get("adx"),
            "rsi": payload.get("rsi"),
            "ema9": payload.get("ema9"),
            "ema21": payload.get("ema21"),
            "vwap": payload.get("vwap"),
            "relative_volume_20": snapshot.get("relative_volume_20"),
            "outcome_status": item.get("outcome_status"),
            "first_hit": item.get("first_hit"),
            "result": item.get("result"),
            "realized_r": item.get("realized_r"),
            "best_favorable_r": item.get("best_favorable_r"),
            "worst_adverse_r": item.get("worst_adverse_r"),
            "best_favorable_pct": item.get("best_favorable_pct"),
            "worst_adverse_pct": item.get("worst_adverse_pct"),
            "best_favorable_atr": item.get("best_favorable_atr"),
            "worst_adverse_atr": item.get("worst_adverse_atr"),
            "feedback_labels": "|".join(str(f.get("category", "")) for f in feedback),
            "feedback_notes": " | ".join(str(f.get("comment", "")) for f in feedback),
            "feature_snapshot": json.dumps(snapshot, ensure_ascii=False, default=str),
        })
    return Response(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="cryptobyance-signals.csv"'},
    )


@app.get("/learning")
async def learning_summary(request: Request):
    require_journal_token(request)
    return await get_journal().learning_summary()


async def _telegram_send_message(chat_id: str, message: str) -> None:
    token = settings.telegram_bot_token
    if not token:
        raise RuntimeError("Telegram bot token is not configured")
    async with httpx.AsyncClient(timeout=10.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "disable_web_page_preview": True},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError("Telegram sendMessage was rejected")


async def _telegram_send_csv(chat_id: str, entries: list[dict]) -> None:
    token = settings.telegram_bot_token
    if not token:
        raise RuntimeError("Telegram bot token is not configured")
    output = io.StringIO()
    fields = ["signal_id", "symbol", "direction", "delivered_at", "config_version",
              "entry", "stop", "target", "target1", "target2", "outcome_status",
              "first_hit", "result", "realized_r", "best_favorable_r",
              "worst_adverse_r", "feedback_labels", "feedback_notes", "feature_snapshot"]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in entries:
        payload = item.get("payload") or {}
        feedback = item.get("feedback") or []
        writer.writerow({
            "signal_id": item.get("id"), "symbol": item.get("symbol"),
            "direction": item.get("direction"), "delivered_at": item.get("delivered_at"),
            "config_version": payload.get("config_version"), "entry": payload.get("entry"),
            "stop": payload.get("stop"), "target": payload.get("target"),
            "target1": payload.get("target1"), "target2": payload.get("target2"),
            "outcome_status": item.get("outcome_status"), "first_hit": item.get("first_hit"),
            "result": item.get("result"), "realized_r": item.get("realized_r"),
            "best_favorable_r": item.get("best_favorable_r"),
            "worst_adverse_r": item.get("worst_adverse_r"),
            "feedback_labels": "|".join(str(f.get("category", "")) for f in feedback),
            "feedback_notes": " | ".join(str(f.get("comment", "")) for f in feedback),
            "feature_snapshot": json.dumps(payload.get("indicator_snapshot") or payload.get("diagnostics") or {}, default=str),
        })
    async with httpx.AsyncClient(timeout=20.0) as client:
        response = await client.post(
            f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id": chat_id, "caption": "CryptoByance delivered-signal export"},
            files={"document": ("cryptobyance-signals.csv", output.getvalue().encode("utf-8"), "text/csv")},
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError("Telegram sendDocument was rejected")


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    expected = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    supplied = request.headers.get("x-telegram-bot-api-secret-token", "")
    if not expected:
        raise HTTPException(status_code=503, detail="TELEGRAM_WEBHOOK_SECRET is not configured")
    if not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Invalid Telegram webhook secret")
    update = await request.json()
    if not isinstance(update, dict):
        return {"ok": True, "ignored": True}
    message = update.get("message")
    if isinstance(message, dict):
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        if not chat_id or chat_id != str(settings.telegram_chat_id):
            return {"ok": True, "ignored": True}
        command_text = str(message.get("text") or "").strip()
        if not command_text.startswith("/"):
            return {"ok": True, "ignored": True}
        tokens = command_text.split()
        command = tokens[0].split("@", 1)[0].lower()
        journal = get_journal()
        try:
            if command == "/train":
                proposal = await journal.train()
                if proposal.get("status") == "PROPOSED":
                    message_text = (
                        f"Training proposal #{proposal['proposal_id']} · {proposal['proposed_config_version']}\\n"
                        f"Base: {proposal['base_config_version']} · Samples: {proposal['summary']['sample_count']}\\n"
                        f"Changes: {json.dumps(proposal['changes'], separators=(',', ':'))}\\n"
                        f"Expected OOS net R: {proposal['expected_effect']['out_of_sample']['expectancy_r']}\\n"
                        "Not activated. Human approval is required."
                    )
                else:
                    message_text = (
                        f"Training: {proposal.get('status')}\\n"
                        f"Samples: {proposal.get('summary', {}).get('sample_count', 0)}\\n"
                        f"{proposal.get('reason', 'No proposal was generated.')}"
                    )
                await _telegram_send_message(chat_id, message_text[:4000])
            elif command == "/proposals":
                proposals = await journal.training_proposals(10)
                if not proposals:
                    message_text = "No training proposals are stored."
                else:
                    message_text = "Training proposals:\\n" + "\\n".join(
                        f"#{x['id']} {x['status']} {x['base_config_version']}→{x['proposed_config_version']} · {x['changes']}"
                        for x in proposals
                    )
                await _telegram_send_message(chat_id, message_text[:4000])
            elif command == "/stats":
                days = int(tokens[1]) if len(tokens) > 1 else 30
                if not 1 <= days <= 365:
                    raise ValueError("days must be between 1 and 365")
                report = await journal.stats(days)
                totals = report["totals"]
                false_rate = totals.get("false_signal_rate")
                false_text = f"{100.0 * false_rate:.1f}%" if false_rate is not None else "n/a"
                message_text = (
                    f"Stats ({days}d)\nDelivered: {totals.get('delivered', 0)}\n"
                    f"Wins: {totals.get('wins', 0)} | Losses: {totals.get('losses', 0)} | "
                    f"Timeouts: {totals.get('timeouts', 0)}\n"
                    f"Unresolved: {totals.get('unresolved', 0)}\n"
                    f"False-signal rate (Fake/Bad/Late): {false_text}\n"
                    f"Average net R: {totals.get('avg_net_realized_r')}"
                )
                await _telegram_send_message(chat_id, message_text)
            elif command == "/pending":
                pending = await journal.pending_entries(20)
                if not pending:
                    message_text = "No delivered signals are waiting for feedback."
                else:
                    message_text = "Pending feedback:\n" + "\n".join(
                        f"#{x['id']} {x['symbol']} {x['direction']} · {x.get('outcome_status') or 'OPEN'}"
                        for x in pending
                    )
                await _telegram_send_message(chat_id, message_text[:4000])
            elif command == "/relabel":
                if len(tokens) != 3:
                    raise ValueError("Usage: /relabel <signal_id> <GOOD|BAD|LATE|FAKE|SKIP>")
                aliases = {
                    "GOOD": "GOOD_ENTRY", "BAD": "BAD_SIGNAL", "LATE": "LATE_ENTRY",
                    "FAKE": "FAKE_SIGNAL", "SKIP": "SKIP",
                }
                category = aliases.get(tokens[2].upper(), tokens[2].upper())
                feedback_id = await journal.relabel_signal(int(tokens[1]), category)
                await _telegram_send_message(chat_id, f"Signal #{tokens[1]} relabelled as {category} (feedback #{feedback_id}).")
            elif command == "/export":
                await _telegram_send_csv(chat_id, await journal.entries(5000))
            else:
                await _telegram_send_message(chat_id, "Commands: /train, /proposals, /stats [days], /pending, /relabel <signal_id> <GOOD|BAD|LATE|FAKE|SKIP>, /export")
            return {"ok": True, "command": command}
        except (ValueError, LookupError) as exc:
            await _telegram_send_message(chat_id, f"Command error: {exc}")
            return {"ok": True, "command": command, "error": str(exc)}
        except Exception as exc:
            log.warning("Telegram journal command failed command=%s error_type=%s", command, type(exc).__name__)
            await _telegram_send_message(chat_id, "Command failed; check the bot service logs.")
            return {"ok": True, "command": command, "error": type(exc).__name__}
    callback = update.get("callback_query")
    if not isinstance(callback, dict):
        return {"ok": True, "ignored": True}
    parts = str(callback.get("data", "")).split("|")
    if len(parts) != 3 or parts[0] != "jfb":
        return {"ok": True, "ignored": True}
    try:
        signal_id = int(parts[1])
        feedback_id = await get_journal().add_telegram_feedback(signal_id, parts[2])
    except (ValueError, LookupError):
        feedback_id = None
    token = settings.telegram_bot_token
    callback_id = callback.get("id")
    if token and callback_id:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(
                    f"https://api.telegram.org/bot{token}/answerCallbackQuery",
                    json={"callback_query_id": callback_id, "text": "Feedback saved" if feedback_id else "Feedback not saved"},
                )
        except Exception:
            log.warning("Telegram callback acknowledgement failed")
    return {"ok": True, "feedback_id": feedback_id}


@app.get("/ping", response_class=Response)
async def ping() -> Response:
    """Return an empty 204 response; do not perform scanner or external work."""
    return Response(status_code=204)

