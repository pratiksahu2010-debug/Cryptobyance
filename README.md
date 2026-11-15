# CryptoByance — single-engine LONG/SHORT signal bot

A Python signal-only scanner for 60 statically configured Binance USDⓈ-M USDT perpetual markets. It sends Telegram alerts; it never places, modifies, or cancels orders.

## Strategy (one engine only)

**5-minute closed candle entry + 15-minute closed-candle context**

LONG requires all:
- 5m EMA(9) > EMA(21)
- 5m close > UTC-day cumulative VWAP
- 5m RSI(14) > 50
- 5m ADX(14) >= configured minimum (default 17)
- 15m EMA(9) > EMA(21) and 15m close > EMA(21)

SHORT is the exact inverse: EMA9 < EMA21, close < VWAP, RSI < 50, ADX >= 17, and bearish 15m EMA/context.

ADX measures strength, not direction. No separate ORB, breakout, momentum, or secondary signal module is included. All conditions are mandatory. The engine evaluates completed candles only, so it does not repaint or alert on a still-forming bar.

ATR(14) is used only to calculate the informational stop distance (1.5 ATR by default); it is not an entry filter. Target is 1.5R by default. These are planning levels, not exchange orders or guaranteed fills.

## 60-symbol universe

BTC, ETH, BNB, SOL, XRP, DOGE, ADA, AVAX, LINK, DOT, TRX, LTC, BCH, NEAR, APT, ARB, OP, SUI, SEI, INJ, ATOM, ETC, FIL, ICP, AAVE, UNI, TIA, JUP, WIF, PEPE, FLOKI, SHIB, RUNE, FTM, GALA, SAND, MANA, AXS, EGLD, ALGO, VET, THETA, EOS, XLM, HBAR, MKR, CRV, LDO, DYDX, 1000BONK, 1000PEPE, ORDI, STX, IMX, GRT, QNT, ENS, KAS, PENDLE, ONDO.

The exchange market list is validated at startup. Binance may delist, rename, or restrict a contract; unavailable symbols are logged and excluded rather than silently substituted.

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

Create a Telegram bot with BotFather, send it a message, and obtain the chat ID. Only a Telegram bot token and chat ID are required. Binance public market data does not require API credentials.

## Render deployment

1. Push this repository to GitHub.
2. In Render choose **New → Blueprint**, select this repository, and apply `render.yaml`.
3. Add `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in the service environment.
4. Deploy, then inspect `/health` and logs. A healthy HTTP process does not prove that Telegram delivery works; verify a test delivery and observe scan-cycle logs.

### Free-plan limitation — important

Render Free web services spin down after 15 minutes without inbound traffic, and local filesystem data is ephemeral. While asleep, the scanner does not run and signals can be missed. A free instance can also restart at any time. Use an external uptime monitor to request `/ping` every 5 minutes if you want to reduce idle spin-down, but this is a best-effort workaround, not a reliability guarantee. For continuous unattended alerts, use an always-on paid service. See Render's current free-instance limitations: https://render.com/docs/free

## Operational safeguards

- One process / one scanner task; do not increase Render worker count.
- Bounded concurrent REST polling, CCXT rate limiting, request timeout, per-symbol exception isolation.
- One evaluation per newly closed 5m candle, with same-direction cooldown.
- Health endpoint exposes cycle count, scan duration, successful/failed symbol counts, last error, and sent signal count.
- No exchange API keys and no order-placement code.
- In-memory cooldown/deduplication resets on restart. Do not interpret it as durable trade history.

## Performance validation before relying on it

No indicator recipe guarantees a 60% win rate. Backtest this exact rule set on out-of-sample data with fees, funding, spread and realistic slippage. Track win rate, profit factor, expectancy, max drawdown, average R, signal count and results by symbol/regime. Avoid tuning on the same period used for final evaluation. Forward-test alerts before risking capital.
