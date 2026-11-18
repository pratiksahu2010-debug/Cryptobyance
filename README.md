# CryptoByance — CoinDCX Futures LONG/SHORT signal bot

A Python signal-only scanner for a fixed 60-symbol target universe of CoinDCX USDT-margined perpetual futures. It sends Telegram alerts and never places, modifies, or cancels orders.

## Strategy — one engine only

**5-minute completed-candle entry with mandatory 15-minute EMA trend confirmation.**

LONG requires all conditions on the latest completed 5m candle:
- EMA(9) > EMA(21) (alignment; a fresh crossover is not required).
- 5m close above UTC-day cumulative VWAP.
- 5m RSI(14) above 50.
- 5m ADX(14) at or above `ADX_MIN` (default 20), with +DI greater than -DI.
- EMA(9) and EMA(21) both rising over the preceding three-bar window.
- Current EMA alignment age is no more than three completed 5m candles.
- The close is no more than 1 ATR above VWAP, to avoid chasing an extended move.

SHORT is the exact inverse: EMA(9) < EMA(21), close below VWAP, RSI below 50, ADX at/above the threshold, -DI greater than +DI, both EMAs falling over the preceding three-bar window, alignment age no more than three candles, and the close no more than 1 ATR below VWAP. ADX below threshold rejects immediately; the engine never waits for ADX to rise on later candles. A mandatory 15m trend veto permits LONG only when 15m EMA(9) > EMA(21), and SHORT only when 15m EMA(9) < EMA(21); missing, stale, invalid, insufficient or neutral 15m data blocks signals. Both timeframes use completed candles only. These are global rules applied equally to every configured symbol; there are no symbol-specific thresholds or exceptions. No ORB, breakout, momentum, or secondary signal module is included.

The new early-entry and extension guards are conservative strategy changes, not a proven profitability guarantee. They must be forward-observed and independently backtested before any claim that expectancy improved.

ATR(14) is used only to calculate the informational stop distance (1.5 ATR by default); it is not an entry filter. Target is 1.5R by default. These are planning levels, not exchange orders or guaranteed fills.

## Universe

The bot retains the original 60 configured symbols:
BTC, ETH, BNB, SOL, XRP, DOGE, ADA, AVAX, LINK, DOT, TRX, LTC, BCH, NEAR, APT, ARB, OP, SUI, SEI, INJ, ATOM, ETC, FIL, ICP, AAVE, UNI, TIA, JUP, WIF, PEPE, FLOKI, SHIB, RUNE, FTM, GALA, SAND, MANA, AXS, EGLD, ALGO, VET, THETA, EOS, XLM, HBAR, MKR, CRV, LDO, DYDX, 1000BONK, 1000PEPE, ORDI, STX, IMX, GRT, QNT, ENS, KAS, PENDLE, ONDO.

At startup the bot compares each exact pair (for example `BTC/USDT` → `B-BTC_USDT`) with CoinDCX's live active USDT futures instruments. Missing contracts are reported in logs and `/health`; the bot does not silently substitute a similarly named contract (for example a 1000x token contract).

## CoinDCX market data

- Active futures instruments: public CoinDCX active-instruments endpoint.
- Historical 5m candles: public CoinDCX futures candlestick REST endpoint.
- Live updates: public CoinDCX futures Socket.IO 5m candlestick channels.
- The scanner aggregates aligned groups of three completed 5m candles into 15m bars. At least 35 valid, contiguous 15m bars are required; the latest 15m EMA(9)/EMA(21) direction is a mandatory veto. Signal calculation also requires the latest 60 completed 5m bars to be contiguous; gaps in either indicator window block signals until a complete window is restored.
- Reconnects: Socket.IO reconnection with bounded retry delay.
- Recovery: REST history is refreshed for symbols whose candle cache is missing or stale.
- Public market data only. No CoinDCX API key or secret is required.
- The application contains no order placement, position, or trading-permission code.

## Telegram

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Alerts identify CoinDCX USDT Perpetual and include a one-click TradingView link to the matching Binance USDT-margined perpetual reference chart on the 5-minute interval, plus entry, ATR-derived stop/target, RR, RSI, ADX, mandatory 15m trend confirmation, and candle close time. The chart is a Binance reference chart; signal data and contract validation remain CoinDCX-based. Symbols that cannot be safely converted to a USDT ticker omit the link rather than generating a malformed URL.

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
- `/performance`: aggregate delivered-signal outcomes, OPEN/TARGET/STOP_LOSS/AMBIGUOUS counts, average realized R, MFE/MAE, data gaps, sent-without-outcome integrity count, per-symbol/direction breakdown and Telegram outbox status. It never returns individual entry/stop/target payloads.
- `/ping`: lightweight keepalive.
- Render logs: look for CoinDCX instrument validation, per-symbol history bootstrap, websocket subscription, and completed scan-cycle messages.

Render Free instances can spin down and restart; scanning is not continuous while the service sleeps. A periodic request to `/ping` can reduce idle spin-down but cannot guarantee uninterrupted signals. Use an always-on instance for continuous monitoring.

## Performance validation

No indicator recipe guarantees a particular win rate. Backtest this exact rule set out of sample with fees, funding, spread and realistic slippage. Forward-test alerts before risking capital.

### Global stop-loss / target research

The current planning levels remain an ATR-derived stop (default 1.5 ATR) and a 1.5R target. They are not being widened, tightened, trailed, or moved to breakeven by this diagnostic release.

The performance report now includes global (all symbols combined) TARGET and STOP_LOSS excursion profiles:
- Average, median, 75th and 90th percentile MFE/MAE in R.
- Average bars observed until resolution.
- Counts of STOP_LOSS outcomes that achieved at least +0.5R / +1.0R MFE on a candle strictly before the stop candle.
- Counts of TARGET outcomes that experienced at least 0.5R / 1.0R MAE on a candle strictly before the target candle.
- Separate LONG/SHORT outcome summaries, without symbol-specific strategy parameters.

The tracker stores the candle timestamp at which the maximum favourable/adverse excursion was observed. Excursions on the exit candle are deliberately not counted as occurring before the exit because OHLC candles cannot reveal the order of their high and low. Historical closed outcomes cannot be retroactively assigned these timestamps; the timed-excursion sample grows from the deployment forward. Existing outcomes remain intact.

Use this evidence to decide whether a global fixed stop, ATR stop, breakeven rule, trailing stop, partial target, or time-based exit deserves a controlled backtest. Do not infer a profitable management rule from aggregate MFE/MAE alone: each candidate must be replayed candle-by-candle with chronological ordering, the same entry assumptions, and realistic fees/funding/slippage. This release is diagnostic only and does not change any live stop or target level.

## Signal diagnostics (research-only)

Every emitted signal stores a versioned `diagnostics` snapshot inside its persisted signal payload. It records the 5m EMA spread and slopes normalized by ATR, consecutive EMA-alignment age (capped at six bars), price-to-VWAP distance and VWAP slope normalized by ATR, RSI14, ADX14, directional-indicator spread, ATR as a percentage of price, and the 15m EMA spread/slope.

These fields are **observability data only**. They do not add entry filters, change the existing LONG/SHORT rules, modify stops or targets, or affect Telegram delivery. No quality score or threshold has been enabled. Use the collected snapshots with independently labelled trade outcomes before considering any strategy change.

## Delivered-signal outcome tracking

Only Telegram-confirmed delivered alerts belong in PostgreSQL `signal_outcomes`; queued, retrying, or permanently failed alerts are excluded. On startup, the store repairs legacy rows by deleting outcomes for non-`SENT` alerts and backfilling missing outcome rows for delivered alerts. Outcome-row creation is committed atomically with the signal-history transition to `SENT`. For each delivered signal, subsequent completed 5m candles update maximum favourable excursion (MFE) and maximum adverse excursion (MAE), both in units of initial risk (R). The first stop/target resolution is recorded; a candle touching both levels is labelled `AMBIGUOUS` because OHLC data cannot establish which level traded first. A missing 5m interval sets `data_gap=true` and ends that outcome as `AMBIGUOUS`, because the bot cannot prove whether the stop or target was reached during the missing interval.

Only alerts with `signal_history.status='SENT'` are included. The signal candle is excluded from outcome measurement because the assumed entry is its close. Open outcomes remain open until a stop or target is reached; there is no invented time-based exit. This is research telemetry, not simulated exchange fills, and it does not change signals or execute orders.

Read-only summary query for Render PostgreSQL:

```sql
SELECT
  status,
  COUNT(*) AS signals,
  ROUND(AVG(realized_r)::numeric, 3) AS avg_realized_r,
  ROUND(AVG(best_favorable_r)::numeric, 3) AS avg_mfe_r,
  ROUND(AVG(worst_adverse_r)::numeric, 3) AS avg_mae_r,
  COUNT(*) FILTER (WHERE data_gap) AS signals_with_data_gaps
FROM signal_outcomes
GROUP BY status
ORDER BY status;
```

Breakdown by symbol and direction:

```sql
SELECT
  symbol, direction, status, COUNT(*) AS signals,
  ROUND(AVG(realized_r)::numeric, 3) AS avg_realized_r,
  ROUND(AVG(best_favorable_r)::numeric, 3) AS avg_mfe_r,
  ROUND(AVG(worst_adverse_r)::numeric, 3) AS avg_mae_r
FROM signal_outcomes
GROUP BY symbol, direction, status
ORDER BY symbol, direction, status;
```


### Performance report interpretation

`/performance` reads aggregate metrics from the same internal PostgreSQL connection used by the scanner and refreshes after each completed scan. `avg_realized_r` includes only TARGET and STOP_LOSS outcomes; OPEN and AMBIGUOUS rows are excluded. MFE/MAE are descriptive excursions, not realized returns. A candle gap or a bar touching both stop and target is AMBIGUOUS and is excluded from the realized-R average. These are candle-based research estimates assuming entry at the signal candle close; they are not actual exchange fills and do not include fees, slippage, funding or liquidation effects. Treat small samples as descriptive only, not proof of an edge.
