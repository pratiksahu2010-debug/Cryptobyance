"""Validated, immutable runtime settings loaded once at process startup."""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field


def _integer(name: str, default: str, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _number(name: str, default: str, minimum: float, maximum: float | None = None) -> float:
    try:
        value = float(os.getenv(name, default))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(value) or value < minimum or (maximum is not None and value > maximum):
        upper = f" and at most {maximum}" if maximum is not None else ""
        raise ValueError(f"{name} must be at least {minimum}{upper}")
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    scan_interval_seconds: int
    concurrency: int
    signal_cooldown_candles: int
    timeframe: str
    context_timeframe: str
    adx_min: float
    ema_fast: int
    ema_slow: int
    rsi_period: int
    adx_period: int
    atr_stop_multiplier: float
    min_risk_reward: float
    log_level: str
    telegram_bot_token: str = field(repr=False)
    telegram_chat_id: str = field(repr=False)
    database_url: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "Settings":
        timeframe = os.getenv("TIMEFRAME", "5m").strip()
        context_timeframe = os.getenv("CONTEXT_TIMEFRAME", "15m").strip()
        if timeframe != "5m" or context_timeframe != "15m":
            raise ValueError("This strategy requires TIMEFRAME=5m and CONTEXT_TIMEFRAME=15m")

        ema_fast = _integer("EMA_FAST", "9", 1, 100)
        ema_slow = _integer("EMA_SLOW", "21", 2, 200)
        if ema_fast >= ema_slow:
            raise ValueError("EMA_FAST must be smaller than EMA_SLOW")

        log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")

        return cls(
            scan_interval_seconds=_integer("SCAN_INTERVAL_SECONDS", "60", 20, 600),
            concurrency=_integer("CONCURRENCY", "8", 1, 16),
            signal_cooldown_candles=_integer("SIGNAL_COOLDOWN_CANDLES", "4", 1, 100),
            timeframe=timeframe,
            context_timeframe=context_timeframe,
            adx_min=_number("ADX_MIN", "20", 0.01, 100),
            ema_fast=ema_fast,
            ema_slow=ema_slow,
            rsi_period=_integer("RSI_PERIOD", "14", 2, 100),
            adx_period=_integer("ADX_PERIOD", "14", 2, 100),
            atr_stop_multiplier=_number("ATR_STOP_MULTIPLIER", "1.5", 0.000001),
            min_risk_reward=_number("MIN_RISK_REWARD", "1.5", 0.000001),
            log_level=log_level,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            database_url=os.getenv("DATABASE_URL", "").strip(),
        )


settings = Settings.from_env()
