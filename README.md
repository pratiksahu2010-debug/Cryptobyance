# CryptoByance — CoinDCX Futures LONG/SHORT signal bot

A Python signal-only scanner for a fixed 60-symbol target universe of CoinDCX USDT-margined perpetual futures. It sends Telegram alerts and never places, modifies, or cancels orders.

## Strategy — one simple LONG/SHORT engine

**5-minute completed-candle entry with a mandatory 15-minute EMA trend veto.**

LONG requires all four 5m conditions on the latest completed candle:
- EMA(9) > EMA(21).
- Close above session VWAP (VWAP resets at the UTC day boundary).
- RSI(14) > 50.
- ADX(14) >= 20 (configurable with `ADX_MIN`; default 20).

SHORT is the exact inverse: EMA(9) < EMA(21), close below VWAP, RSI(14) < 50, and ADX(14) >= 20. No additional +DI/-DI, EMA-slope, fresh-crossover, EMA-alignment-age, or VWAP-extension gates are used.

**15-minute veto:** LONG is allowed only when 15m EMA(9) > EMA(21); SHORT only when 15m EMA(9) < EMA(21). A neutral, missing, stale, invalid, or insufficient 15m context blocks the signal. Both timeframes use completed candles only; at least 35 contiguous 15m bars and 60 contiguous 5m bars are required.

**ATR risk levels:** ATR(14) on the 5m entry timeframe sets stop distance (`ATR_STOP_MULTIPLIER`, default 1.5 ATR). Target is the configured risk multiple (`MIN_RISK_REWARD`, default 1.5R). ATR is used for risk levels, not as an extra directional entry filter. These are informational planning levels, not exchange orders or guaranteed fills.

This is intentionally a small, explainable ruleset. It can produce more signals than the previous version because restrictive slope, age, DI, and extension gates have been removed. Validate it with chronological backtests and forward observation; no indicator combination guarantees profitability.
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

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Alerts identify CoinDCX USDT Perpetual and include a TradingView market-directory link for the symbol, where you can select an exchange that lists the market. This is deliberately not presented as a CoinDCX chart: the bot's signal data and contract validation remain CoinDCX-based. Alerts also include entry, ATR-derived stop/target, RR, RSI, ADX, mandatory 15m trend confirmation, and candle close time. Symbols that cannot be safely converted to a USDT ticker omit the link.

## Code architecture

`DATABASE_URL` is required for durable signal history, duplicate protection, cooldowns and the Telegram outbox. It must be a PostgreSQL connection string for your chosen provider (for Neon, use the pooled connection string and require TLS, e.g. `sslmode=require`). Do not paste database credentials into source control. Legacy pending-candidate rows are invalidated and are never promoted into delayed alerts. The candidate store does not place trades.

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

The existing Render web service remains attached to this repository's `main` branch. Keep one Uvicorn worker because the scanner and websocket cache are in-process. Configure `DATABASE_URL`, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID` in Render. To use the protected journal dashboard and APIs, also set `JOURNAL_API_TOKEN`; to receive Telegram inline-button feedback, set `TELEGRAM_WEBHOOK_SECRET` and register the webhook as described below. Auto-deploy will pick up commits to `main`.

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

`/performance` reads aggregate metrics from the configured PostgreSQL connection used by the scanner and refreshes after each completed scan. `avg_realized_r` includes only TARGET and STOP_LOSS outcomes; OPEN and AMBIGUOUS rows are excluded. MFE/MAE are descriptive excursions, not realized returns. A candle gap or a bar touching both stop and target is AMBIGUOUS and is excluded from the realized-R average. These are candle-based research estimates assuming entry at the signal candle close; they are not actual exchange fills and do not include fees, slippage, funding or liquidation effects. Treat small samples as descriptive only, not proof of an edge.


## Self-learning signal journal

This release adds a research journal alongside the existing CoinDCX signal engine. It does not change signal eligibility, cooldowns, stop/target calculations, or order execution (the bot remains signal-only).

### What is recorded

- Every Telegram-delivered signal already has a permanent `signal_history` row and delivered-signal outcome tracking. The journal exposes these records with their entry/stop/target, point-in-time indicator snapshot, outcome, MFE/MAE and feedback history.
- Signal snapshots include EMA 9/21, RSI 14, ADX 14, +DI/-DI, VWAP, ATR, relative volume, MACD and candle OHLCV. Snapshot fields are observational only.
- Meaningful directional setups rejected by the current strategy are stored separately in `journal_candidates`, with the available indicator snapshot and explicit rejection reasons. Their hypothetical outcome tracker uses later completed 5m candles, excludes the candidate candle, marks gaps ambiguous, and never treats a same-candle stop/target collision as a win.
- Manual feedback is append-only. Signal feedback categories include Good Entry, Bad Signal, Late Entry, Stop-Loss Too Tight, Target Too Close, Wrong Direction and Needs Review. Rejected candidates can be labelled Missed Opportunity or Needs Review. A manual observation endpoint is also available for a candidate you identify during backtesting.

### Render environment

Set these additional environment variables in Render:
- `JOURNAL_API_TOKEN`: a long random private token used to protect journal data and feedback APIs.
- `TELEGRAM_WEBHOOK_SECRET`: a random secret (1–256 characters) used to authenticate Telegram callback updates.

After deployment, open `/journal` and enter `JOURNAL_API_TOKEN`. The dashboard shows delivered signals, feedback, rejected candidates and the learning summary. The data endpoints are protected by the token. Export the collected journal as JSON from `/journal/export`.

### Telegram feedback buttons

Signal messages include inline feedback buttons. To receive button presses, configure Telegram's webhook once after deployment (replace the service URL with your Render URL):

```bash
curl -X POST "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/setWebhook" \
  -d "url=https://YOUR-SERVICE.onrender.com/telegram/webhook" \
  -d "secret_token=$TELEGRAM_WEBHOOK_SECRET"
```

Telegram must be able to reach the public HTTPS webhook. Dashboard feedback works independently of webhook configuration.

### API

- `GET /journal`: dashboard.
- `GET /journal/api/entries`: delivered signal journal (header `X-Journal-Token`).
- `GET /journal/api/candidates`: rejected/manual candidate journal.
- `POST /journal/api/feedback`: append feedback to a delivered signal.
- `POST /journal/api/candidate-feedback`: label a rejected candidate as Missed Opportunity or Needs Review.
- `POST /journal/api/observation`: add a manually observed setup. Required JSON fields: `symbol`, `direction`, `candle_open_ms` (UTC-aligned 5m candle-open timestamp), `entry`, `stop`, `target`; optional `comment`.
- `GET /learning`: aggregate feedback, outcome and rejection-reason analysis.
- `GET /journal/export`: JSON export of signals, candidates and learning summary.
- `POST /telegram/webhook`: Telegram inline feedback callback; authenticated by Telegram's webhook secret header.

All journal API routes except the dashboard and Telegram webhook require `X-Journal-Token: YOUR_TOKEN`. Manual observation and feedback are audit records, not trade orders.

### Natural-language timing instructions

Open `/journal`, enter your `JOURNAL_API_TOKEN`, and use **Teach the bot your preferred timing** to write instructions in plain language. Examples:

- `For LONG, prefer the first retest after MACD turns positive and relative volume is above 1.5.`
- `Avoid SHORT entries when price is more than 1 ATR below VWAP.`
- `Prefer BTCUSDT LONG setups between 09:00 and 12:00 IST.`
- `I missed ETHUSDT LONG at 10:15; the first pullback after volume expansion was the entry I wanted.`

The parser extracts a reviewable category, direction, symbol, time window, indicator mentions, entry pattern and explicit relative-volume threshold when present. It stores the original sentence alongside the parsed version. Review instructions through the authenticated API:

- `POST /journal/api/training-instructions` with JSON `{"instruction":"..."}`.
- `GET /journal/api/training-instructions` to inspect pending and reviewed instructions.
- `POST /journal/api/training-instructions/{id}/review` with `{"decision":"APPROVED_RESEARCH","reviewer_note":"..." }` or `{"decision":"REJECTED","reviewer_note":"..." }`.
- `GET /learning/daily-review` for the rolling 24-hour signal, feedback, missed-opportunity and instruction summary.

**Approval means approved for offline research only.** It does not alter the live strategy, thresholds, symbols, stops, targets or Telegram signals. The parser is intentionally conservative: it extracts what it recognizes and preserves the raw sentence; it does not pretend to understand unsupported conditions. Always inspect the parsed instruction before approving it. The service also saves a rolling 24-hour research review at startup and every 24 hours while running; reviews are retained in PostgreSQL `training_daily_reviews`. A Render Free sleep/restart pauses the timer, so a fresh review is produced when the service starts again.

### Learning safeguards

The learning endpoint reports sample sizes, outcomes, feedback and rejected-candidate statistics. It can recommend further testing but never changes live thresholds or strategy code. Only accept a strategy adjustment after chronological walk-forward replay with realistic fees, funding, spread and slippage. The illustrative “Good Entry” example discussed during design is not inserted as a real trade or feedback record.
