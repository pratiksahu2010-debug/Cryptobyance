"""Validated, immutable runtime settings loaded once at process startup."""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field


CONFIG_VERSION = "v2"
STRATEGY_KEY = "coindcx_per_symbol_v2"

SYMBOLS = (
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "AVAX/USDT", "LINK/USDT", "DOT/USDT",
    "TRX/USDT", "LTC/USDT", "BCH/USDT", "NEAR/USDT", "APT/USDT",
    "ARB/USDT", "OP/USDT", "SUI/USDT", "SEI/USDT", "INJ/USDT",
    "ATOM/USDT", "ETC/USDT", "FIL/USDT", "ICP/USDT", "AAVE/USDT",
    "UNI/USDT", "TIA/USDT", "JUP/USDT", "WIF/USDT", "PEPE/USDT",
    "FLOKI/USDT", "SHIB/USDT", "RUNE/USDT", "FTM/USDT", "GALA/USDT",
    "SAND/USDT", "MANA/USDT", "AXS/USDT", "EGLD/USDT", "ALGO/USDT",
    "VET/USDT", "THETA/USDT", "EOS/USDT", "XLM/USDT", "HBAR/USDT",
    "MKR/USDT", "CRV/USDT", "LDO/USDT", "DYDX/USDT", "1000BONK/USDT",
    "1000PEPE/USDT", "ORDI/USDT", "STX/USDT", "IMX/USDT", "GRT/USDT",
    "QNT/USDT", "ENS/USDT", "KAS/USDT", "PENDLE/USDT", "ONDO/USDT",
 )

assert len(SYMBOLS) == 60 and len(set(SYMBOLS)) == 60


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
    rsi_long_min: float
    rsi_long_max: float
    rsi_short_min: float
    rsi_short_max: float
    context_rsi_midline: float
    swing_lookback_bars: int
    tp1_r: float
    tp2_r: float
    atr_stop_multiplier: float
    min_risk_reward: float
    outcome_horizon_bars: int
    outcome_cost_pct: float
    training_min_samples: int
    training_min_bucket_samples: int
    db_retention_days: int
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

        rsi_long_min = _number("RSI_LONG_MIN", "52", 0, 100)
        rsi_long_max = _number("RSI_LONG_MAX", "70", 0, 100)
        rsi_short_min = _number("RSI_SHORT_MIN", "30", 0, 100)
        rsi_short_max = _number("RSI_SHORT_MAX", "42", 0, 100)
        context_rsi_midline = _number("CONTEXT_RSI_MIDLINE", "50", 0, 100)
        if rsi_long_min >= rsi_long_max or rsi_short_min >= rsi_short_max:
            raise ValueError("RSI band minimums must be below their maximums")
        tp1_r = _number("TP1_R", "1", 0.01, 20)
        tp2_r = _number("TP2_R", "2", 0.01, 40)
        if tp2_r <= tp1_r:
            raise ValueError("TP2_R must be greater than TP1_R")

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
            rsi_long_min=rsi_long_min,
            rsi_long_max=rsi_long_max,
            rsi_short_min=rsi_short_min,
            rsi_short_max=rsi_short_max,
            context_rsi_midline=context_rsi_midline,
            swing_lookback_bars=_integer("SWING_LOOKBACK_BARS", "10", 2, 100),
            tp1_r=tp1_r,
            tp2_r=tp2_r,
            atr_stop_multiplier=_number("ATR_STOP_MULTIPLIER", "1.5", 0.000001),
            min_risk_reward=_number("MIN_RISK_REWARD", "1.5", 0.000001),
            outcome_horizon_bars=_integer("OUTCOME_HORIZON_BARS", "12", 1, 288),
            outcome_cost_pct=_number("OUTCOME_COST_PCT", "0.10", 0, 5),
            training_min_samples=_integer("TRAINING_MIN_SAMPLES", "100", 30, 100000),
            training_min_bucket_samples=_integer("TRAINING_MIN_BUCKET_SAMPLES", "30", 5, 10000),
            db_retention_days=_integer("DB_RETENTION_DAYS", "365", 30, 3650),
            log_level=log_level,
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            database_url=os.getenv("DATABASE_URL", "").strip(),
        )


settings = Settings.from_env()
