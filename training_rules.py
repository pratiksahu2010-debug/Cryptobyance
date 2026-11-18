"""Natural-language training instruction intake.

Instructions are parsed into auditable research preferences. Approval never
changes the live strategy; a separate offline replay/promotion process is required.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

TIME_RE = re.compile(r"(?<!\d)([01]?\d|2[0-3]):([0-5]\d)\s*(?:-|to|until|and)\s*([01]?\d|2[0-3]):([0-5]\d)(?!\d)", re.I)
NUMBER_RE = re.compile(r"\b(?:rvol|relative\s+volume)\s*(?:>=|>|at\s+least)\s*(\d+(?:\.\d+)?)", re.I)
SYMBOL_RE = re.compile(r"\b([A-Z0-9]{2,18})(?:/|-)?USDT\b", re.I)


def parse_instruction(raw_text: str) -> dict[str, Any]:
    text = " ".join(str(raw_text or "").strip().split())
    if not text or len(text) > 1500:
        raise ValueError("instruction must contain 1–1500 characters")
    lower = text.lower()
    if any(x in lower for x in ("avoid", "skip", "don't take", "do not take", "reject")):
        category = "AVOID_SETUP"
    elif any(x in lower for x in ("missed", "should have", "wish bot", "look for")):
        category = "MISSED_OPPORTUNITY"
    elif any(x in lower for x in ("prefer", "wait for", "enter", "entry", "first retest", "breakout")):
        category = "PREFER_ENTRY"
    else:
        category = "GENERAL"
    time_match = TIME_RE.search(text)
    rvol_match = NUMBER_RE.search(text)
    symbols = sorted(set(x.upper() for x in SYMBOL_RE.findall(text)))
    indicators = [name for name in ("macd", "rvol", "relative volume", "vwap", "ema", "rsi", "adx", "atr", "roc", "retest", "breakout", "volume") if name in lower]
    timing = None
    if time_match:
        timing = {
            "start_local": f"{int(time_match.group(1)):02d}:{time_match.group(2)}",
            "end_local": f"{int(time_match.group(3)):02d}:{time_match.group(4)}",
            "timezone": "Asia/Kolkata",
        }
    return {
        "schema_version": 1,
        "category": category,
        "symbols": symbols,
        "direction": "LONG" if "long" in lower or "call" in lower else "SHORT" if "short" in lower or "put" in lower else "ANY",
        "time_window": timing,
        "min_relative_volume": float(rvol_match.group(1)) if rvol_match else None,
        "indicators_mentioned": indicators,
        "entry_terms": [term for term in ("first retest", "pullback", "breakout", "crossover", "reclaim", "rejection", "expansion") if term in lower],
        "requires_human_review": True,
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }
