# CryptoByance — CoinDCX Futures LONG/SHORT signal bot

A Python signal-only scanner for a fixed 60-symbol target universe of CoinDCX USDT-margined perpetual futures. It sends Telegram alerts and never places, modifies, or cancels orders.

## Strategy — one engine only

**5-minute closed-candle entry + 15-minute closed-candle context**

LONG requires all:
- 5m EMA(9) > EMA(21)
- 5m close > UTC-day cumulative VWAP
- 5m RSI(14) > 50
- 5m ADX(14) >= configured minimum (default 17)
- 15m EMA(9) > EMA(21) and 15m close > EMA(21)

SHORT is the exact inverse. ADX measures strength, not direction. No ORB, breakout, momentum, or secondary signal module is included. All conditions are mandatory. The engine evaluates completed candles only.

ATR(14) is used only to calculate the informational stop distance (1.5 ATR by default); it is not an entry filter. Target is 1.5R by default. These are planning levels, not exchange orders or guaranteed fills.

## Universe

The bot retains the original 60 configured symbols:
BTC, ETH, BNB, SOL, XRP, DOGE, ADA, AVAX, LINK, DOT, TRX, LTC, BCH, NEAR, APT, ARB, OP, SUI, SEI, INJ, ATOM, ETC, FIL, ICP, AAVE, UNI, TIA, JUP, WIF, PEPE, FLOKI, SHIB, RUNE, FTM, GALA, SAND, MANA, AXS, EGLD, ALGO, VET, THETA, EOS, XLM, HBAR, MKR, CRV, LDO, DYDX, 1000BONK, 1000PEPE, ORDI, STX, IMX, GRT, QNT, ENS, KAS, PENDLE, ONDO.

At startup the bot compares each exact pair (for example `BTC/USDT` → `B-BTC_USDT`) with CoinDCX's live active USDT futures instruments. Missing contracts are reported in logs and `/health`; the bot does not silently substitute a similarly named contract (for example a 1000x token contract).

## CoinDCX market data

- Active futures instruments: public CoinDCX active-instruments endpoint.
- Historical 5m candles: public CoinDCX futures candlestick REST endpoint.
- Live updates: public CoinDCX futures Socket.IO 5m candlestick channels.
- 15m context: built from three contiguous, completed 5m candles to keep context aligned with the trigger timeframe.
- Reconnects: Socket.IO reconnection with bounded retry delay.
- Recovery: REST history is refreshed for symbols whose candle cache is missing or stale.
- Public market data only. No CoinDCX API key or secret is required.
- The application contains no order placement, position, or trading-permission code.

## Telegram

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Alerts identify CoinDCX USDT Perpetual and include entry, informational stop/target, RR, RSI, ADX, and candle close time.

## Code architecture

The runtime is split by responsibility:
- `main.py`: FastAPI lifecycle, scan orchestration, and health endpoints.
- `coindcx_feed.py`: CoinDCX public REST/Socket.IO adapter and 15m aggregation.
- `strategy.py`: pure OHLCV validation, indicators, and the single LONG/SHORT decision rule.
- `telegram.py`: alert formatting and bounded Telegram delivery retries with secret-safe logging.

The strategy module has no network or application-lifecycle dependencies. Telegram delivery retries rate limits, server errors, and transient network failures up to three attempts; permanent 4xx rejections are not retried. The scanner supervisor restarts a failed scanner session with bounded backoff. On startup/restart, candles that closed before the new scanner session began are ignored so old setups are not replayed as fresh alerts.

Signal deduplication and same-direction cooldown state are currently in process memory. A process restart resets that state; durable cross-deploy deduplication would require a persistent store. No persistent store is added to this lightweight signal-only build.

## Run locally

Python 3.11 recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env
uvicorn main:app --host 0.0.0.0 --port 8000
```

## Render deployment

The existing Render web service remains attached to this repository's `main` branch. Keep one Uvicorn worker because the scanner and websocket cache are in-process. Set the two Telegram environment variables in Render. Auto-deploy will pick up commits to `main`.

Check:
- `/health`: market metadata ready, active/unavailable symbols, websocket status, last websocket event, scan counters and errors.
- `/ping`: lightweight keepalive.
- Render logs: look for CoinDCX instrument validation, per-symbol history bootstrap, websocket subscription, and completed scan-cycle messages.

Render Free instances can spin down and restart; scanning is not continuous while the service sleeps. A periodic request to `/ping` can reduce idle spin-down but cannot guarantee uninterrupted signals. Use an always-on instance for continuous monitoring.

## Performance validation

No indicator recipe guarantees a particular win rate. Backtest this exact rule set out of sample with fees, funding, spread and realistic slippage. Forward-test alerts before risking capital.
