# Changelog

All notable implementation changes for the state-machine bot are recorded here.

## 2026-11-20

- **PR #60 — Source archive:** added an automatic GitHub Actions ZIP artifact containing the complete source tree and deployment/schema files, excluding local secrets.
- **PR #58 — Training bucket coverage:** added explicit ATR% feature snapshots and diagnostics for cooldown bars, directional RSI bands, EMA spread magnitude, VWAP distance, relative volume, and UTC-hour buckets.
- **PR #57 — Feedback and replay training:** Telegram text replies to bot alerts are stored as `NEEDS_REVIEW` notes without grading; notes do not remove a signal from `/pending` or override manual grades. Replay outcomes now pass through the offline trainer and walk-forward guards. Proposals include OOS resolved-signals-per-day estimates.
- **PR #56 — Weekly digest and retention:** added idempotent Monday UTC weekly Telegram digests and configurable `DB_RETENTION_DAYS` telemetry pruning while retaining core signal/outcome/feedback/config audit data.
- **PR #55 — SQLite local fallback:** added a standard-library SQLite backend for local single-process runs; Render continues to require PostgreSQL.
- **PR #54 — Shadow mode:** added isolated proposal shadow evaluation, durable would-be signal records, expiry, status reporting, and stop controls. Shadow signals never enter the live Telegram outbox.
- **PR #53 — Feature snapshots:** added point-in-time indicator, timing, state-cycle, and config-version snapshots.
- **PR #52 — Historical replay:** added deterministic no-lookahead 5m/15m replay and cost-aware outcome summaries.
- **PR #50/#51 — Feedback trainer and config governance:** added offline training guards, proposals, explicit approval/rejection, hot reload, config history, and rollback.
- **PR #46 — Risk-model consistency:** documented production swing-structure risk and aligned legacy ATR helper targets with its own stop model.
- **PR #45 — Cooldown consistency:** aligned Render cooldown default with the four-candle configuration.
- **PR #44 — Higher-timeframe invalidation:** invalidated ARMED setups when the mandatory 15m EMA trend reverses.
- **PR #43 — 15m RSI confidence:** added non-blocking 15m RSI confidence metadata without changing the mandatory 5m trigger.

## Verification notes

- GitHub Actions compile and unit/integration checks passed for PR #58: **209 tests passed**. The latest CI run included the SQLite integration tests, weekly digest persistence, Telegram reply-note handling, replay-to-trainer preview, and independent Wilder indicator reference checks.
- Synthetic and local SQLite tests do not prove live CoinDCX connectivity, Render startup, production PostgreSQL migration behavior, or real Telegram delivery.
