# CryptoByance — CoinDCX Futures LONG/SHORT signal bot

A Python signal-only scanner for a fixed 60-symbol target universe of CoinDCX USDT-margined perpetual futures. It sends Telegram alerts and never places, modifies, or cancels orders.

## Strategy — per-symbol staged LONG/SHORT state machine

**5-minute completed-candle processing.** Every symbol has independent durable LONG and SHORT state rows in PostgreSQL. The lifecycle is `IDLE → ARMED → TRIGGERED → COOLDOWN → IDLE`; a setup candle only arms the direction and can never alert on that same candle.

**IDLE setup (all conditions on the latest closed 5m bar):**
- LONG: ADX(14) >= `ADX_MIN` (default 20), +DI > -DI, EMA(9) > EMA(21), and close above session VWAP.
- SHORT: ADX(14) >= `ADX_MIN`, -DI > +DI, EMA(9) < EMA(21), and close below session VWAP.
- The existing 15m EMA(9)/EMA(21) direction veto is retained: bullish for LONG and bearish for SHORT.
- 5m RSI(14) remains the mandatory entry trigger and is calculated only for a direction already in ARMED state.
- 15m RSI(14) is an optional confidence signal: at/above `CONTEXT_RSI_MIDLINE` (default 50) supports LONG; at/below it supports SHORT. It is displayed in alerts and stored in the feature snapshot, but never blocks a valid 5m signal.

**ARMED trigger (a later closed 5m bar):**
- LONG: RSI crosses upward through `RSI_LONG_MIN` (default 52) and closes inside `RSI_LONG_MIN..RSI_LONG_MAX` (default 52..70).
- SHORT: RSI crosses downward through `RSI_SHORT_MAX` (default 42) and closes inside `RSI_SHORT_MIN..RSI_SHORT_MAX` (default 30..42).
- A trigger is accepted only while the setup remains valid. Any 5m EMA9/EMA21 cross against the direction, close crossing VWAP against the direction, ADX below minimum, DI cross against the direction, or completed 15m EMA9/EMA21 reversal/loss of directional alignment invalidates an ARMED setup. If 15m EMA values are unavailable or invalid, state processing is deferred rather than falsely invalidating the setup.

**Cooldown and re-arming:** `SIGNAL_COOLDOWN_CANDLES` defaults to 4 (20 minutes on 5m). After the bar count expires, the direction remains in COOLDOWN-WAIT until its setup has invalidated at least once during or after cooldown. It then returns to IDLE; a later qualifying candle must arm it again before another alert is possible. State and the processed-candle checkpoint persist across restarts. Reprocessing the same/older candle is a no-op. Signal history, state transition, cooldown and Telegram outbox are committed transactionally.

**15-minute veto:** LONG is allowed only when 15m EMA(9) > EMA(21); SHORT only when 15m EMA(9) < EMA(21). Missing, stale, invalid, or insufficient 15m context defers state processing rather than firing on stale data. At least 35 contiguous 15m bars and 60 contiguous 5m bars are required.

**Production risk levels:** the scanner uses the prior swing low for LONG or prior swing high for SHORT, using `SWING_LOOKBACK_BARS` (default 10; signal candle excluded). TP1 and TP2 are configured at `TP1_R` (default 1R) and `TP2_R` (default 2R) of that swing-based risk. Production signal payloads identify this as `risk_model=swing_structure` and include the actual risk distance and percentage. These are informational planning levels, not exchange orders or guaranteed fills.

**Legacy compatibility:** the public `evaluate_signal()` helper intentionally retains its historical ATR stop and single `target` contract (`ATR_STOP_MULTIPLIER` and `MIN_RISK_REWARD`). Its supplemental `target1`/`target2` fields are recalculated from that same ATR risk distance, and its payload identifies `risk_model=legacy_atr`. These two legacy environment variables do not affect the production scanner or Telegram alerts.

All setup thresholds, RSI bands, the 15m RSI confidence midline (`CONTEXT_RSI_MIDLINE`, default 50) and cooldown length are configurable through environment settings loaded by `config.py`. The bot remains signal-only and no indicator combination guarantees profitability; validate with chronological backtests and forward observation before risking capital.

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

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Alerts identify CoinDCX USDT Perpetual and include a TradingView market-directory link for the symbol, where you can select an exchange that lists the market. This is deliberately not presented as a CoinDCX chart: the bot's signal data and contract validation remain CoinDCX-based. Alerts include entry, bar time, ADX, 5m RSI, EMA9/21 values, VWAP, swing-based stop, TP1, TP2, mandatory 15m EMA trend confirmation, optional 15m RSI confidence (explicitly marked non-blocking), and the signal ID used by feedback buttons. Symbols that cannot be safely converted to a USDT ticker omit the link.

## Code architecture

For Render/production, set `DATABASE_URL` to a PostgreSQL connection string (for Neon, use the pooled connection string and require TLS, e.g. `sslmode=require`). Render deliberately fails startup if this value is missing; it must not silently fall back to an ephemeral local database.

For local development, leaving `DATABASE_URL` empty selects the standard-library SQLite backend at `./cryptobyance-local.db`. Override the path with `LOCAL_SQLITE_PATH`, or set an explicit URL such as `DATABASE_URL=sqlite:///./dev.db` (absolute paths use four slashes, e.g. `sqlite:////tmp/cryptobyance.db`). SQLite runs with one serialized connection, WAL mode, and foreign-key checks. It supports local scanner state, signal/outbox delivery, outcomes, feedback, training and shadow records, but is intended for one local process only—not Render, multiple workers, or concurrent production replicas. Use PostgreSQL for production. Do not paste database credentials into source control. The new state machine does not use legacy pending-candidate rows; they are not promoted into delayed alerts. The candidate store does not place trades.

The runtime is split by responsibility:
- `config.py`: immutable, validated environment settings; secrets are excluded from its representation.
- `main.py`: FastAPI lifecycle, scan orchestration, outbox delivery worker, cleanup schedule and health endpoints.
- `coindcx_feed.py`: CoinDCX public REST/Socket.IO adapter and optional 15m aggregation.
- `strategy.py`: pure OHLCV validation, indicators, per-direction setup/invalidation/RSI trigger evaluation, and risk levels.
- `state_machine.py`: pure lifecycle transitions and PostgreSQL persistence for per-symbol/per-direction state.
- `telegram.py`: alert formatting and bounded Telegram delivery retries with secret-safe logging.
- `candidate_store.py`: database schema, state transitions, signal history, durable cooldown, transactional Telegram outbox and scanner checkpoints.
- `sqlite_store.py`: serialized, standard-library SQLite adapter used only for local development; PostgreSQL remains the production backend.

### Persistence design

In production, PostgreSQL is the durable source of truth for bot workflow state. The local SQLite backend uses the same logical tables and workflow operations, but serializes access through one connection and omits PostgreSQL-only row/advisory lock syntax because its transaction begins with `BEGIN IMMEDIATE`.
- `strategy_signal_states`: one durable IDLE/ARMED/COOLDOWN state per strategy, symbol and direction, with cycle ID, invalidation history and processed-candle checkpoint.
- `strategy_state_events`: append-only transition audit log.
- `signal_candidates`: legacy pending-candidate table retained for compatibility; the current state machine does not promote its rows.
- `signal_history`: one durable record per symbol/direction/5m candle, protected by a unique idempotency key.
- `telegram_outbox`: queued alert payloads, attempt count, retry time, delivery lease and terminal status. A worker claims rows with `FOR UPDATE SKIP LOCKED`; expired leases are recovered after restarts.
- `signal_cooldowns`: same-direction cooldown survives service restarts. Per-symbol transaction advisory locks serialize concurrent signal creation.
- `scanner_runtime`: last completed scan and WebSocket health checkpoint, loaded at startup for diagnostics.

On a trigger, signal history, ARMED → TRIGGERED → COOLDOWN audit events, durable cooldown update and outbox insertion commit in one database transaction. Telegram delivery is retried from the outbox after temporary failures. Delivery is **at-least-once**: a rare network failure after Telegram accepts a message but before PostgreSQL records the acknowledgement can still result in a duplicate Telegram message. The database idempotency key prevents duplicate signal/outbox creation; it cannot make Telegram's external API exactly-once.

The WebSocket candle cache and indicator calculations stay in memory; the bot does not write every tick to PostgreSQL. REST history repopulates the candle cache after a restart. This avoids excessive database writes while preserving candidates, signals, cooldowns and alert-delivery work.

The strategy module has no network or application-lifecycle dependencies. Telegram's own sender retries rate limits, server errors and transient network failures up to three attempts; the durable outbox retries failed delivery cycles with backoff and marks an alert DEAD after 12 cycles for manual review. The scanner supervisor restarts a failed scanner session with bounded backoff. On startup/restart, candles that closed before the new scanner session began are ignored so old setups are not replayed as fresh alerts.

A retention sweep runs on startup and nightly at 02:00 India time. It marks pending candidates EXPIRED after their evaluation window and applies the bounded telemetry policy described below. Candidate audit rows, signal history, outcomes, feedback, config history/events, and unsent/dead Telegram outbox records are retained. Sent outbox rows and non-critical state/shadow/research telemetry are pruned after `DB_RETENTION_DAYS`. Scanner runtime uses an upserted checkpoint row; logs remain in Render's logging service rather than being copied into PostgreSQL.

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

Only Telegram-confirmed delivered alerts belong in PostgreSQL `signal_outcomes`; queued, retrying, or permanently failed alerts are excluded. Outcome-row creation is committed atomically with the signal-history transition to `SENT`. Existing delivered rows are backfilled on startup, with legacy target fields retained.

Each delivered signal is measured over the next **12 completed 5m bars by default** (one hour). Configure `OUTCOME_HORIZON_BARS` to change the horizon. `OUTCOME_COST_PCT` is the estimated round-trip cost as a percentage of entry notional; the default `0.10` means 0.10%. Both values are snapshotted on each outcome row. Net realized R subtracts this estimated cost from the gross result.

The tracker records MFE and MAE in R, percentage-of-entry terms, and ATR multiples (when entry ATR is available). It records the first barrier as `TP1`, `TP2`, or `SL`, the number of bars to resolution, and the result `WIN`, `LOSS`, or `TIMEOUT`. If a single OHLC candle touches the stop and either target, the outcome is conservatively recorded as an `SL / LOSS`. If neither target nor stop is reached within the configured horizon, the final close is used for a cost-adjusted mark-to-market R and the result is `TIMEOUT`. A missing 5m interval is marked `AMBIGUOUS` with `data_gap=true`; the bot does not invent a path through missing candles.

Only alerts with `signal_history.status='SENT'` are included. The signal candle is excluded because entry is assumed at its close. These are candle-based research estimates, not simulated exchange fills; they do not execute orders or change live strategy thresholds.

Read-only summary query for Render PostgreSQL:

```sql
SELECT
  status, first_hit, result,
  COUNT(*) AS signals,
  ROUND(AVG(realized_r)::numeric, 3) AS avg_net_realized_r,
  ROUND(AVG(best_favorable_r)::numeric, 3) AS avg_mfe_r,
  ROUND(AVG(worst_adverse_r)::numeric, 3) AS avg_mae_r,
  ROUND(AVG(best_favorable_pct)::numeric, 3) AS avg_mfe_pct,
  ROUND(AVG(worst_adverse_pct)::numeric, 3) AS avg_mae_pct,
  ROUND(AVG(best_favorable_atr)::numeric, 3) AS avg_mfe_atr,
  ROUND(AVG(worst_adverse_atr)::numeric, 3) AS avg_mae_atr,
  ROUND(AVG(bars_to_resolution)::numeric, 1) AS avg_bars_to_resolution,
  COUNT(*) FILTER (WHERE data_gap) AS signals_with_data_gaps
FROM signal_outcomes
GROUP BY status, first_hit, result
ORDER BY status, first_hit, result;
```

Breakdown by symbol and direction:

```sql
SELECT
  symbol, direction, status, first_hit, result, COUNT(*) AS signals,
  ROUND(AVG(realized_r)::numeric, 3) AS avg_net_realized_r,
  ROUND(AVG(best_favorable_r)::numeric, 3) AS avg_mfe_r,
  ROUND(AVG(worst_adverse_r)::numeric, 3) AS avg_mae_r
FROM signal_outcomes
GROUP BY symbol, direction, status, first_hit, result
ORDER BY symbol, direction, status, first_hit, result;
```


### Performance report interpretation

`/performance` reads aggregate metrics from the configured PostgreSQL connection used by the scanner and refreshes after each completed scan. `avg_realized_r` includes TARGET, STOP_LOSS and TIMEOUT outcomes; OPEN and AMBIGUOUS rows are excluded. Realized R is net of the configured estimated round-trip cost. MFE/MAE are descriptive excursions, not realized returns. A candle gap is AMBIGUOUS and excluded from the realized-R average. A bar touching both stop and target is conservatively counted as a LOSS. These are candle-based research estimates assuming entry at the signal candle close; they are not actual exchange fills; the configurable cost is an estimate and may not capture actual fees, slippage, funding or liquidation effects. Treat small samples as descriptive only, not proof of an edge.


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

Telegram must be able to reach the public HTTPS webhook. Dashboard feedback works independently of webhook configuration. Reply to a delivered bot alert with ordinary text to attach a `NEEDS_REVIEW` note to that signal. A reply note is not a grade and does not remove the signal from `/pending`; use the inline buttons or `/relabel` for a manual label.

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

## Offline threshold trainer (proposal-only)

Run `/train` in the configured Telegram chat or call the protected `POST /journal/api/train` endpoint. The trainer joins delivered signals, point-in-time feature snapshots, cost-adjusted outcomes, and the latest manual feedback label. Manual Good/Bad/Late/Fake labels override the automatic WIN/LOSS/TIMEOUT classification; conflicts are counted in the report.

The trainer refuses to generate a proposal below `TRAINING_MIN_SAMPLES=100` resolved labeled signals. Bucket metrics are suppressed below `TRAINING_MIN_BUCKET_SAMPLES=30`. It searches only bounded, stricter variants of the currently active ADX and RSI-band filters, selects candidates on the older 70% of the chronological dataset, and requires positive expectancy improvement in both sequential out-of-sample folds. It reports train/test metrics, expectancy gap, retained sample size, manual-label conflicts, and feature-bucket analysis. It will return an explicit insufficient-data or no-OOS-improvement result rather than guessing.

Use `/proposals` or the protected `GET /journal/api/proposals` endpoint to review stored proposals. Run a proposal in isolated shadow mode with `/shadow <id> [days]` (1–30 days), inspect it with `/shadow_status`, and stop it early with `/shadow_stop`. Shadow mode evaluates the same completed 5m/15m candles with a separate durable state-machine key, stores would-be signals in `shadow_signal_history`, and never inserts them into the Telegram outbox. Live thresholds remain unchanged. When the shadow period expires or is stopped, the proposal returns to `PROPOSED` so it can be explicitly approved or rejected. Then approve with `/approve <id>`, reject with `/reject <id>`, and restore the previous approved configuration with `/rollback`. The protected API equivalents are `POST /journal/api/proposals/{id}/approve`, `POST /journal/api/proposals/{id}/reject`, and `POST /journal/api/rollback`. Every activation and rollback is written to `config_history` and `config_history_events` with actor, time, reason, and previous version. The approved config is hot-reloaded in the running scanner and loaded again on restart; the Telegram chat ID must match the configured owner chat before these commands are accepted.

EMA-spread, VWAP-distance, relative-volume, cooldown-bar, directional RSI-band, and UTC-hour buckets are reported as observational diagnostics. Cooldown and spread buckets are not proposed for live changes without replayable counterfactual evidence. The trainer does not propose changing cooldown from outcome-only data without a replayable counterfactual. Proposal reports also estimate baseline and candidate resolved signals per day on the chronological out-of-sample window; this is explicitly an estimate from resolved delivered signals, not a forecast of every future alert.

## Weekly digest and database retention

A weekly digest is generated for the last completed UTC week and sent to the configured Telegram chat on Monday shortly after 00:00 UTC. It reports resolved sample count, win rate, expectancy after configured costs, false-signal rate, best/worst symbols (minimum three resolved samples), and the weakest sufficiently sampled feature bucket. Digests are persisted before delivery and retried after failures/restarts; they never change live settings.

`DB_RETENTION_DAYS` (default 365, allowed 30–3650) prunes only non-critical telemetry: state transition events, shadow signal rows, daily research summaries and weekly digests after successful send, sent Telegram outbox rows, and stale scanner checkpoints. Signal history, outcomes, feedback, proposals, configuration history, and configuration audit events are retained indefinitely.

## Historical replay / backtest

The repository includes a deterministic replay CLI that uses the same production `evaluate_state_inputs`, `advance_state`, `evaluate_setup`, and forward outcome labeler. It does not connect to CoinDCX, Telegram, or a broker and never places orders.

Prepare two CSVs with these columns: `timestamp,open,high,low,close,volume`. Timestamps must be UTC Unix milliseconds representing candle-open time. The 5m and 15m files must be sorted/unique, UTC-aligned, valid OHLCV, and fully contiguous; missing candles are rejected instead of silently skipped.

Run:

```bash
python backtest.py --candles-5m data/BTCUSDT_5m.csv --candles-15m data/BTCUSDT_15m.csv --trades-out results/trades.csv --summary-out results/summary.json
```

Optional overrides: `--cooldown-bars`, `--horizon-bars`, and `--cost-pct`. By default the replay uses the current runtime ADX/RSI settings, cooldown, 12-bar outcome horizon, and estimated round-trip cost. A 15m candle is visible only after its close is at or before the current 5m candle close; no future 15m data is used. Entries are assumed at the signal candle close, outcomes are measured only on later 5m candles, and the summary reports net expectancy, win/loss/timeout counts, profit factor, max drawdown, and MFE/MAE. Results are historical estimates, not exchange fills. The output also includes a `training_preview`: resolved synthetic replay outcomes are passed through the same offline trainer and walk-forward guards, but are not inserted into the live journal or used to change settings.

## Signal feature snapshot

Each delivered signal stores a versioned point-in-time `indicator_snapshot` alongside the production setup diagnostics. The snapshot includes current and previous RSI, EMA9/EMA21 and signed EMA spread percentage, price-to-VWAP percentage distance, ADX, DI values, ATR and ATR% of price, relative volume, MACD, 15m market trend/context RSI, UTC time and weekday, bars since arming, armed-candle timestamp, durable state-cycle ID, cooldown setting, and config version. For this crypto strategy, `index_trend` is explicitly `null`; the available market-direction context is the completed 15m EMA trend.

These fields are research telemetry only. They do not add entry gates or alter the live signal decision.


### Shadow mode API

- `POST /journal/api/proposals/{id}/shadow?days=3` starts a shadow run.
- `GET /journal/api/shadow` returns the active proposal, expiry, and would-be signal counts.
- `POST /journal/api/shadow/stop` ends the run and returns the proposal to `PROPOSED`.

All endpoints require the configured `JOURNAL_API_TOKEN`. Only one shadow proposal can run at a time. Shadow state is isolated by proposal config version; it does not share live cooldown or state-cycle records.


### Local SQLite quick start

1. Copy `.env.example` to `.env`.
2. Leave `DATABASE_URL` empty for the default `./cryptobyance-local.db`, or set `LOCAL_SQLITE_PATH` to another file.
3. Install dependencies and run `uvicorn main:app --reload`.
4. The first startup creates/migrates the local database. Keep the DB file private and back it up before deleting or replacing it.

SQLite integration tests cover schema creation, durable state/idempotency, signal-to-outbox delivery, outcome creation, feedback, analytics, and config history. SQLite is a local convenience backend; do not copy its file into Render or share it across workers.


## Known limitations and verification status

- CI runs compile checks and synthetic/unit/integration tests. Live CoinDCX REST/WebSocket connectivity, Render deployment, production PostgreSQL migrations, and real Telegram delivery/webhook callbacks have not been verified from this development session.
- The historical replay assumes entry at the signal candle close. It models an estimated round-trip percentage cost but does not model exchange-specific bid/ask spread, funding, market impact, partial fills, or latency. It is not an execution simulator.
- Shadow mode records would-be signals using an isolated persisted state machine. It does not simulate exchange fills or send shadow alerts.
- This CoinDCX crypto strategy has no separate index feed; `index_trend` is stored as `null`, while the completed 15m EMA trend is captured.
- Trainer buckets are descriptive and require the configured minimum samples. Only bounded ADX/RSI threshold candidates are searched automatically; cooldown, EMA spread, VWAP distance, relative volume, and time-of-day remain diagnostics until a replayable counterfactual is available.
- SQLite is a single-process local-development backend. Render must use PostgreSQL and one Uvicorn worker. Do not use the SQLite file for multiple workers or production replicas.
- Weekly digests require valid Telegram credentials and chat configuration. Failed sends remain pending and are retried; without credentials they are retained but not delivered.
- Retention defaults to 365 days for non-critical telemetry. Core signal history, outcomes, feedback, proposals, config history, and config audit events are not automatically deleted.

See [CHANGELOG.md](CHANGELOG.md) for the implementation history.
