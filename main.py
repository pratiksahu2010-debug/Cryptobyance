"""Single-engine Binance USD-M crypto signal bot. Signals only; never places orders."""
from __future__ import annotations

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import ccxt.async_support as ccxt
import httpx
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
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

SCAN_SECONDS = max(20, int(os.getenv("SCAN_INTERVAL_SECONDS", "60")))
CONCURRENCY = max(1, int(os.getenv("CONCURRENCY", "8")))
ADX_MIN = float(os.getenv("ADX_MIN", "17"))
EMA_FAST = int(os.getenv("EMA_FAST", "9"))
EMA_SLOW = int(os.getenv("EMA_SLOW", "21"))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", "14"))
ADX_PERIOD = int(os.getenv("ADX_PERIOD", "14"))
COOLDOWN_CANDLES = max(1, int(os.getenv("SIGNAL_COOLDOWN_CANDLES", "3")))
ATR_STOP_MULT = float(os.getenv("ATR_STOP_MULTIPLIER", "1.5"))
MIN_RR = float(os.getenv("MIN_RISK_REWARD", "1.5"))
TIMEFRAME = os.getenv("TIMEFRAME", "5m")
CONTEXT_TF = os.getenv("CONTEXT_TIMEFRAME", "15m")

state: dict[str, Any] = {
    "started_at": time.time(), "scans": 0, "last_scan": None,
    "last_scan_seconds": None, "symbols_ok": 0, "symbols_failed": 0,
    "signals_sent": 0, "last_error": None, "running": False,
}
# In-process deduplication. Free Render restarts reset this; the cooldown is also
# based on candle open-time, so a restart cannot emit an old candle's signal.
last_alert: dict[str, tuple[str, int]] = {}
last_seen_candle: dict[str, int] = {}

def candles_to_frame(raw: list) -> pd.DataFrame:
    df = pd.DataFrame(raw, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "taker_buy_base",
        "taker_buy_quote", "ignore"
    ])
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["close", "volume"]).reset_index(drop=True)

def indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate indicators on OHLCV bars; VWAP resets at UTC day boundary."""
    x = df.copy()
    x["ema9"] = x.close.ewm(span=EMA_FAST, adjust=False).mean()
    x["ema21"] = x.close.ewm(span=EMA_SLOW, adjust=False).mean()

    delta = x.close.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / RSI_PERIOD, adjust=False, min_periods=RSI_PERIOD).mean()
    rs = gain / loss.replace(0, np.nan)
    x["rsi"] = 100 - (100 / (1 + rs))
    x.loc[(loss == 0) & (gain > 0), "rsi"] = 100
    x.loc[(loss == 0) & (gain == 0), "rsi"] = 50

    prev_close = x.close.shift(1)
    tr = pd.concat([
        x.high - x.low, (x.high - prev_close).abs(), (x.low - prev_close).abs()
    ], axis=1).max(axis=1)
    up = x.high.diff()
    down = -x.low.diff()
    plus_dm = up.where((up > down) & (up > 0), 0.0)
    minus_dm = down.where((down > up) & (down > 0), 0.0)
    atr = tr.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / atr.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean() / atr.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    x["adx"] = dx.ewm(alpha=1 / ADX_PERIOD, adjust=False, min_periods=ADX_PERIOD).mean()
    x["atr"] = atr

    typical = (x.high + x.low + x.close) / 3
    utc_day = pd.to_datetime(x.timestamp, unit="ms", utc=True).dt.date
    pv = typical * x.volume
    x["vwap"] = pv.groupby(utc_day).cumsum() / x.volume.groupby(utc_day).cumsum().replace(0, np.nan)
    return x

def evaluate_signal(df5: pd.DataFrame, df15: pd.DataFrame) -> dict | None:
    """Single strict-threshold engine. Uses only fully closed candles."""
    if len(df5) < 60 or len(df15) < 60:
        return None
    # Binance returns the currently-forming last candle; exclude it.
    a = indicators(df5.iloc[:-1].copy())
    b = indicators(df15.iloc[:-1].copy())
    if len(a) < 35 or len(b) < 35:
        return None
    c, p = a.iloc[-1], b.iloc[-1]
    needed = [c.ema9, c.ema21, c.vwap, c.rsi, c.adx, c.atr,
              p.ema9, p.ema21, p.close]
    if not all(pd.notna(v) and np.isfinite(float(v)) for v in needed):
        return None

    long_ok = (
        c.ema9 > c.ema21 and c.close > c.vwap and c.rsi > 50
        and c.adx >= ADX_MIN and p.ema9 > p.ema21 and p.close > p.ema21
    )
    short_ok = (
        c.ema9 < c.ema21 and c.close < c.vwap and c.rsi < 50
        and c.adx >= ADX_MIN and p.ema9 < p.ema21 and p.close < p.ema21
    )
    if not (long_ok or short_ok):
        return None

    side = "LONG" if long_ok else "SHORT"
    entry = float(c.close)
    # ATR-based protective stop; target is 1.5R by default. This is trade
    # planning metadata only; the bot never submits exchange orders.
    risk = max(float(c.atr) * ATR_STOP_MULT, entry * 0.001)
    stop = entry - risk if side == "LONG" else entry + risk
    target = entry + risk * MIN_RR if side == "LONG" else entry - risk * MIN_RR
    return {
        "side": side, "entry": entry, "stop": stop, "target": target,
        "rr": MIN_RR, "rsi": float(c.rsi), "adx": float(c.adx),
        "vwap": float(c.vwap), "candle": int(c.timestamp),
        "context": "15m bullish" if side == "LONG" else "15m bearish",
    }

def fmt_price(value: float) -> str:
    if value >= 1000: return f"{value:,.2f}"
    if value >= 1: return f"{value:.4f}"
    return f"{value:.8f}".rstrip("0").rstrip(".")

async def send_telegram(client: httpx.AsyncClient, signal: dict) -> bool:
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN", "").strip(), os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        log.error("Telegram credentials missing; signal not delivered")
        return False
    emoji = "🟢" if signal["side"] == "LONG" else "🔴"
    text = (
        f"{emoji} <b>{signal['side']} SIGNAL</b>\n"
        f"<b>{signal['symbol']}</b> · Binance USDT Perpetual\n"
        f"Entry: <code>{fmt_price(signal['entry'])}</code>\n"
        f"Stop loss: <code>{fmt_price(signal['stop'])}</code>\n"
        f"Target: <code>{fmt_price(signal['target'])}</code>\n"
        f"Risk/Reward: 1:{signal['rr']:.2f}\n\n"
        f"EMA 9/21: aligned\nVWAP: correct side\n"
        f"RSI(14): {signal['rsi']:.1f}\nADX(14): {signal['adx']:.1f}\n"
        f"15m context: {signal['context']}\n"
        f"5m candle close: {datetime.fromtimestamp(signal['candle']/1000, timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n\n"
        f"<i>Signal only. Confirm execution conditions independently; not financial advice.</i>"
    )
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = await client.post(url, json={
            "chat_id": chat_id, "text": text, "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=12)
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            raise RuntimeError(str(payload))
        return True
    except Exception as exc:
        log.exception("Telegram delivery failed: %s", exc)
        state["last_error"] = f"telegram: {exc}"
        return False

async def scan_symbol(exchange, client, semaphore, display_symbol: str, exchange_symbol: str) -> None:
    async with semaphore:
        try:
            raw5, raw15 = await asyncio.gather(
                exchange.fetch_ohlcv(exchange_symbol, timeframe=TIMEFRAME, limit=300),
                exchange.fetch_ohlcv(exchange_symbol, timeframe=CONTEXT_TF, limit=120),
            )
            df5, df15 = candles_to_frame(raw5), candles_to_frame(raw15)
            if len(df5) < 61 or len(df15) < 61:
                raise ValueError(f"insufficient candles: 5m={len(df5)}, 15m={len(df15)}")
            closed_ts = int(df5.iloc[-2].timestamp)
            # Process each newly closed 5m bar once.
            if last_seen_candle.get(display_symbol) == closed_ts:
                state["symbols_ok"] += 1
                return
            signal = evaluate_signal(df5, df15)
            if not signal:
                last_seen_candle[display_symbol] = closed_ts
                state["symbols_ok"] += 1
                return
            previous = last_alert.get(display_symbol)
            if previous and previous[0] == signal["side"]:
                candles_since = (closed_ts - previous[1]) // 300_000
                if candles_since < COOLDOWN_CANDLES:
                    last_seen_candle[display_symbol] = closed_ts
                    state["symbols_ok"] += 1
                    return
            signal["symbol"] = display_symbol
            delivered = await send_telegram(client, signal)
            if delivered:
                last_alert[display_symbol] = (signal["side"], closed_ts)
                last_seen_candle[display_symbol] = closed_ts
                state["signals_sent"] += 1
                log.info("SIGNAL SENT %s %s entry=%s ADX=%.1f RSI=%.1f",
                         display_symbol, signal["side"], signal["entry"], signal["adx"], signal["rsi"])
            state["symbols_ok"] += 1
        except Exception as exc:
            state["symbols_failed"] += 1
            state["last_error"] = f"{display_symbol}: {type(exc).__name__}: {exc}"
            log.warning("scan failed symbol=%s error=%s", display_symbol, exc)

async def scanner() -> None:
    exchange_class = getattr(ccxt, os.getenv("EXCHANGE_ID", "binanceusdm"))
    exchange = exchange_class({
        "enableRateLimit": True, "timeout": 15000,
        "options": {"defaultType": "future", "adjustForTimeDifference": True},
    })
    client = httpx.AsyncClient()
    semaphore = asyncio.Semaphore(CONCURRENCY)
    try:
        await exchange.load_markets()
        # CCXT names USD-M perpetuals as BTC/USDT:USDT (not BTC/USDT).
        # Resolve our human-readable static list to the exact active swap symbol.
        resolved = {}
        for market in exchange.markets.values():
            if (market.get("active", True) and market.get("swap")
                    and market.get("quote") == "USDT" and market.get("settle") == "USDT"):
                resolved[f"{market.get('base')}/{market.get('quote')}"] = market["symbol"]
        valid = [(configured, resolved[configured]) for configured in SYMBOLS if configured in resolved]
        missing = sorted(set(SYMBOLS) - {configured for configured, _ in valid})
        log.info("market universe validated: configured=%d active=%d unavailable=%s",
                 len(SYMBOLS), len(valid), missing)
        if len(valid) != 60:
            log.warning("Universe has %d active markets, expected 60; unavailable contracts will be skipped", len(valid))
        while True:
            started = time.monotonic()
            state.update(running=True, symbols_ok=0, symbols_failed=0)
            results = await asyncio.gather(
                *(scan_symbol(exchange, client, semaphore, configured, market_symbol)
                  for configured, market_symbol in valid),
                return_exceptions=True,
            )
            unexpected = [str(r) for r in results if isinstance(r, BaseException)]
            state["scans"] += 1
            state["last_scan"] = datetime.now(timezone.utc).isoformat()
            state["last_scan_seconds"] = round(time.monotonic() - started, 2)
            if unexpected:
                state["last_error"] = "; ".join(unexpected[:3])
            log.info("scan complete cycle=%d ok=%d failed=%d signals=%d duration=%.1fs",
                     state["scans"], state["symbols_ok"], state["symbols_failed"],
                     state["signals_sent"], state["last_scan_seconds"])
            await asyncio.sleep(max(1, SCAN_SECONDS - (time.monotonic() - started)))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        state["last_error"] = f"scanner fatal: {exc}"
        log.exception("scanner stopped unexpectedly")
        raise
    finally:
        state["running"] = False
        await client.aclose()
        await exchange.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(scanner(), name="market-scanner")
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="CryptoByance Signal Bot", version="1.0.0", lifespan=lifespan)

@app.get("/")
async def root():
    return {"service": "cryptobyance-signal-bot", "mode": "signals_only",
            "symbols_configured": len(SYMBOLS), "timeframe": TIMEFRAME,
            "context_timeframe": CONTEXT_TF}

@app.get("/health")
async def health():
    return {"ok": True, "service": "cryptobyance-signal-bot",
            "scanner_running": state["running"], "scans": state["scans"],
            "symbols_configured": len(SYMBOLS), "symbols_ok_last_scan": state["symbols_ok"],
            "symbols_failed_last_scan": state["symbols_failed"],
            "signals_sent": state["signals_sent"], "last_scan": state["last_scan"],
            "last_scan_seconds": state["last_scan_seconds"],
            "uptime_seconds": int(time.time() - state["started_at"]),
            "last_error": state["last_error"]}

@app.get("/ping")
async def ping():
    return {"ok": True}
