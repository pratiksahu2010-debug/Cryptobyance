"""Telegram alert delivery with bounded retries and secret-safe diagnostics."""
from __future__ import annotations

import asyncio
import logging
from html import escape
from datetime import datetime, timezone
from urllib.parse import urlencode

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
        """Build the 5-minute Binance USDT perpetual chart URL for a signal symbol."""
        normalized = str(symbol or "").strip().upper()
        if not normalized.endswith("/USDT"):
            return ""
        base = normalized[:-5]
        if not base or not all(ch.isalnum() for ch in base):
            return ""
        query = urlencode({"symbol": f"BINANCE:{base}USDT.P", "interval": "5"})
        return f"https://www.tradingview.com/chart/?{query}"

    @staticmethod
    def format_message(signal: dict) -> str:
        emoji = "🟢" if signal["side"] == "LONG" else "🔴"
        # Exchange timestamps identify candle OPEN; show the true close time.
        candle_close_ms = signal.get("candle_close", signal["candle"] + 300_000)
        candle_time = datetime.fromtimestamp(
            candle_close_ms / 1000, timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")
        chart_url = TelegramNotifier.tradingview_url(signal.get("symbol", ""))
        chart_link = (
            f'<a href="{escape(chart_url, quote=True)}">📊 OPEN BINANCE PERPETUAL CHART · 5m</a>\n\n'
            if chart_url else ""
        )
        return (
            f"{emoji} <b>{signal['side']} SIGNAL</b>\n"
            f"<b>{signal['symbol']}</b> · CoinDCX USDT Perpetual\n"
            + chart_link
            + f"Entry: <code>{fmt_price(signal['entry'])}</code>\n"
            f"Stop loss: <code>{fmt_price(signal['stop'])}</code>\n"
            f"Target: <code>{fmt_price(signal['target'])}</code>\n"
            f"Risk/Reward: 1:{signal['rr']:.2f}\n\n"
            f"EMA 9/21: aligned with signal direction\nVWAP: correct side\n"
            f"RSI(14): {signal['rsi']:.1f}\nADX(14): {signal['adx']:.1f}\n"
            f"15m trend filter: {escape(str(signal.get('context', 'unavailable')))} (mandatory direction confirmation)\n"
            f"5m candle close: {candle_time}\n\n"
            "<i>Signal only. Confirm execution conditions independently; not financial advice.</i>"
        )

    async def send(self, signal: dict) -> bool:
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
            "text": self.format_message(signal),
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
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
