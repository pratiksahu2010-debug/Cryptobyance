# CryptoByance — CoinDCX Futures LONG/SHORT signal bot

A Python signal-only scanner for a fixed 60-symbol target universe of CoinDCX USDT-margined perpetual futures. It sends Telegram alerts and never places, modifies, or cancels orders.

## Strategy — one engine only

**5-minute completed-candle entry. 15-minute context is not an entry gate.**

LONG requires all conditions on the same latest completed 5m candle:
- A fresh EMA(9) crossing above EMA(21) on this candle (not an older EMA alignment).
- 5m close above UTC-day cumulative VWAP.
- 5m RSI(14) above 50.
- 5m ADX(14) at or above `ADX_MIN` (default 20), with +DI greater than -DI.

SHORT is the exact inverse: fresh EMA(9) cross below EMA(21), close below VWAP, RSI below 50, ADX at/above the threshold, and -DI greater than +DI. ADX below threshold rejects immediately; the engine never waits for ADX to rise on later candles. 15m data cannot delay or veto a valid 5m setup. No ORB, breakout, momentum, or secondary signal module is included. The engine evaluates completed candles only.

ATR(14) is used only to calculate the informational stop distance (1.5 ATR by default); it is not an entry filter. Target is 1.5R by default. These are planning levels, not exchange orders or guaranteed fills.

## Universe

The bot retains the original 60 configured symbols:
BTC, ETH, BNB, SOL, XRP, DOGE, ADA, AVAX, LINK, DOT, TRX, LTC, BCH, NEAR, APT, ARB, OP, SUI, SEI, INJ, ATOM, ETC, FIL, ICP, AAVE, UNI, TIA, JUP, WIF, PEPE, FLOKI, SHIB, RUNE, FTM, GALA, SAND, MANA, AXS, EGLD, ALGO, VET, THETA, EOS, XLM, HBAR, MKR, CRV, LDO, DYDX, 1000BONK, 1000PEPE, ORDI, STX, IMX, GRT, QNT, ENS, KAS, PENDLE, ONDO.

At startup the bot compares each exact pair (for example `BTC/USDT` → `B-BTC_USDT`) with CoinDCX's live active USDT futures instruments. Missing contracts are reported in logs and `/health`; the bot does not silently substitute a similarly named contract (for example a 1000x token contract).

## CoinDCX market data

- Active futures instruments: public CoinDCX active-instruments endpoint.
- Historical 5m candles: public CoinDCX futures candlestick REST endpoint.
- Live updates: public CoinDCX futures Socket.IO 5m candlestick channels.
- 15m aggregation remains available for diagnostics/optional context only; it is not fetched or required by the live entry scanner.
- Reconnects: Socket.IO reconnection with bounded retry delay.
- Recovery: REST history is refreshed for symbols whose candle cache is missing or stale.
- Public market data only. No CoinDCX API key or secret is required.
- The application contains no order placement, position, or trading-permission code.

## Telegram

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Alerts identify CoinDCX USDT Perpetual and include a one-click TradingView link to the matching Binance USDT-margined perpetual reference chart on the 5-minute interval, plus entry, informational stop/target, RR, RSI, ADX, and candle close time. The chart is a Binance reference chart; signal data and contract validation remain CoinDCX-based. Symbols that cannot be safely converted to a USDT ticker omit the link rather than generating a malformed URL.

## Code architecture

`DATABASE_URL` is required for durable signal history, duplicate protection, cooldowns and the Telegram outbox; configure it with the Render internal database URL. Legacy pending-candidate rows are invalidated and are never promoted into delayed alerts. The candidate store does not place trades.

The runtime is split by responsibility:
- `config.py`: immutable, validated environment settings; secrets are excluded from its representation.
- `main.py`: FastAPI lifecycle, scan orchestration, outbox delivery worker, cleanup schedule and health endpoints.
- `coindcx_feed.py`: CoinDCX public REST/Socket.IO adapter and optional 15m aggregation.
- `strategy.py`: pure OHLCV validation, indicators, and the single LONG/SHORT decision rule.
- `telegram.py`: alert formatting and bounded Telegram delivery retries with secret-safe logging.
- `candidate_store.py`: PostgreSQL schema, legacy-candidate invalidation, signal history, durable cooldown, transactional Telegram outbox and scanner checkpoints.

### PostgreSQL persistence design

PostgreSQL is the durable source of truth for bot workflow state:
- `signal_candidates`: pending ADX candidates, evaluated-candle checkpoint and three-follow-up-candle expiry.
- `signal_history`: one durable record per symbol/direction/5m candle, protected by a unique idempotency key.
- `telegram_outbox`: queued alert payloads, attempt count, retry time, delivery lease and terminal status. A worker claims rows with `FOR UPDATE SKIP LOCKED`; expired leases are recovered after restarts.
- `signal_cooldowns`: same-direction cooldown survives service restarts. Per-symbol transaction advisory locks serialize concurrent signal creation.
- `scanner_runtime`: last completed scan and WebSocket health checkpoint, loaded at startup for diagnostics.

Signal history, candidate transition, cooldown update and outbox insertion commit in one database transaction. Telegram delivery is retried from the outbox after temporary failures. Delivery is **at-least-once**: a rare network failure after Telegram accepts a message but before PostgreSQL records the acknowledgement can still result in a duplicate Telegram message. The database idempotency key prevents duplicate signal/outbox creation; it cannot make Telegram's external API exactly-once.

The WebSocket candle cache and indicator calculations stay in memory; the bot does not write every tick to PostgreSQL. REST history repopulates the candle cache after a restart. This avoids excessive database writes while preserving candidates, signals, cooldowns and alert-delivery work.

The strategy module has no network or application-lifecycle dependencies. Telegram's own sender retries rate limits, server errors and transient network failures up to three attempts; the durable outbox retries failed delivery cycles with backoff and marks an alert DEAD after 12 cycles for manual review. The scanner supervisor restarts a failed scanner session with bounded backoff. On startup/restart, candles that closed before the new scanner session began are ignored so old setups are not replayed as fresh alerts.

A retention sweep runs on startup and nightly at 02:00 India time. It marks pending candidates EXPIRED after their evaluation window but does not delete database rows. All candidate records (including expired/invalid and triggered records), signal history, Telegram outbox records (including SENT/DEAD), cooldown state and runtime checkpoints are retained. No automatic PostgreSQL DELETE or time-based data pruning is performed. Scanner runtime has one upserted checkpoint row; logs remain in Render's logging service rather than being copied into PostgreSQL.

## Run locally

Python 3.11 recommended.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Set TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, and DATABASE_URL in .env
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
