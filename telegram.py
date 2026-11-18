"""Telegram alert delivery with bounded retries and secret-safe diagnostics."""
from __future__ import annotations

import asyncio
import logging
from html import escape
from datetime import datetime, timezone

import httpx

log = logging.getLogger("cryptobyance.telegram")

# httpx emits request URLs at INFO level. Telegram's bot token is part of that
# URL, so keep transport loggers above INFO even when application logging is DEBUG/INFO.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def fmt_price(value: float) -> str:
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.8f}".rstrip("0").rstrip(".")


class TelegramNotifier:
    """Send signal-only alerts. Never logs request URLs, tokens, or response bodies."""

    def __init__(self, client: httpx.AsyncClient, token: str = "", chat_id: str = "", attempts: int = 3):
        self.client = client
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self.attempts = max(1, min(int(attempts), 3))
        self.configured = bool(self.token and self.chat_id)
        self._missing_config_logged = False

    @staticmethod
    def tradingview_url(symbol: str) -> str:
        """Build a TradingView market directory URL without implying exchange parity."""
        normalized = str(symbol or "").strip().upper()
        if not normalized.endswith("/USDT"):
            return ""
        base = normalized[:-5]
        if not base or not all(ch.isalnum() for ch in base):
            return ""
        # CoinDCX's embedded TradingView charts do not establish a public
        # standalone COINDCX: ticker. Open the symbol's market directory so
        # users can deliberately select an available exchange instead of
        # silently being sent to a different venue's perpetual contract.
        return f"https://www.tradingview.com/symbols/{base}USDT/markets/"

    @staticmethod
    def format_message(signal: dict, signal_id: int | None = None) -> str:
        emoji = "🟢" if signal["side"] == "LONG" else "🔴"
        # Exchange timestamps identify candle OPEN; show the true close time.
        candle_close_ms = signal.get("candle_close", signal["candle"] + 300_000)
        candle_time = datetime.fromtimestamp(
            candle_close_ms / 1000, timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        chart_url = TelegramNotifier.tradingview_url(signal.get("symbol", ""))
        chart_link = (
            f'<a href="{escape(chart_url, quote=True)}">📊 OPEN TRADINGVIEW MARKET LIST · SELECT EXCHANGE</a>\n\n'
            if chart_url else ""
        )
        snapshot = signal.get("indicator_snapshot") or {}
        ema9 = signal.get("ema9", snapshot.get("ema9"))
        ema21 = signal.get("ema21", snapshot.get("ema21"))
        vwap = signal.get("vwap", snapshot.get("vwap"))
        ema9_text = fmt_price(float(ema9)) if ema9 is not None else "N/A"
        ema21_text = fmt_price(float(ema21)) if ema21 is not None else "N/A"
        vwap_text = fmt_price(float(vwap)) if vwap is not None else "N/A"
        target1 = signal.get("target1", signal.get("target"))
        target2 = signal.get("target2", signal.get("target"))
        signal_ref = f"\n<b>Signal ID: <code>{int(signal_id)}</code></b>" if signal_id is not None else ""
        return (
            f"{emoji} <b>{signal['side']} SIGNAL</b>\n"
            + f"<b>{signal['symbol']}</b> · CoinDCX USDT Perpetual\n"
            + chart_link
            + f"<b>Entry: <code>{fmt_price(signal['entry'])}</code></b>\n"
            + f"<b>Bar time: <code>{candle_time}</code></b>\n"
            + f"<b>ADX(14): <code>{signal['adx']:.1f}</code> · RSI(14): <code>{signal['rsi']:.1f}</code></b>\n"
            + f"<b>EMA 9/21: <code>{ema9_text}</code> / <code>{ema21_text}</code></b>\n"
            + f"<b>VWAP: <code>{vwap_text}</code></b>\n"
            + f"<b>Stop loss (swing): <code>{fmt_price(signal['stop'])}</code></b>\n"
            + f"<b>TP1: <code>{fmt_price(target1)}</code></b>\n"
            + f"<b>TP2: <code>{fmt_price(target2)}</code></b>\n"
            + f"<b>15m trend: {escape(str(signal.get('context', 'unavailable')))}</b>"
            + signal_ref
            + "\n\n<i>Signal only. Confirm execution conditions independently; not financial advice.</i>"
        )

    async def send(self, signal: dict, signal_id: int | None = None) -> bool:
        if not self.configured:
            if not self._missing_config_logged:
                log.error(
                    "Telegram is not configured; set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID"
                )
                self._missing_config_logged = True
            return False

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        body = {
            "chat_id": self.chat_id,
            "text": self.format_message(signal, signal_id=signal_id),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if signal_id is not None:
            body["reply_markup"] = {
                "inline_keyboard": [
                    [
                        {"text": "👍 Good entry", "callback_data": f"jfb|{int(signal_id)}|GOOD_ENTRY"},
                        {"text": "👎 Bad signal", "callback_data": f"jfb|{int(signal_id)}|BAD_SIGNAL"},
                    ],
                    [
                        {"text": "⏰ Late entry", "callback_data": f"jfb|{int(signal_id)}|LATE_ENTRY"},
                        {"text": "🔎 Review", "callback_data": f"jfb|{int(signal_id)}|NEEDS_REVIEW"},
                    ],
                ]
            }

        for attempt in range(1, self.attempts + 1):
            try:
                response = await self.client.post(url, json=body, timeout=12)
                status = response.status_code
                try:
                    payload = response.json()
                except ValueError:
                    payload = {}

                if 200 <= status < 300 and isinstance(payload, dict) and payload.get("ok") is True:
                    return True

                retry_after = None
                if isinstance(payload, dict):
                    retry_after = (payload.get("parameters") or {}).get("retry_after")

                retryable = status == 429 or status >= 500
                if not retryable:
                    log.error("Telegram rejected alert status=%s", status)
                    return False
                if attempt == self.attempts:
                    log.error("Telegram delivery exhausted retries status=%s", status)
                    return False

                try:
                    delay = min(5.0, max(1.0, float(retry_after))) if retry_after is not None else float(attempt)
                except (TypeError, ValueError):
                    delay = float(attempt)
                await asyncio.sleep(delay)

            except httpx.RequestError as exc:
                # Exception strings may contain the bot-token URL; log only its type.
                if attempt == self.attempts:
                    log.error("Telegram network delivery failed after retries error_type=%s", type(exc).__name__)
                    return False
                await asyncio.sleep(float(attempt))
            except Exception as exc:
                log.error("Telegram delivery failed error_type=%s", type(exc).__name__)
                return False

        return False
