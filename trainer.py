"""Offline, conservative threshold research with chronological validation.

The trainer only returns a proposal. It never mutates runtime configuration.
"""
from __future__ import annotations

import itertools
import math
from collections import defaultdict
from typing import Any, Iterable

OUTCOME_LABELS = {"WIN", "LOSS", "TIMEOUT"}
FALSE_LABELS = {"BAD_SIGNAL", "LATE_ENTRY", "FAKE_SIGNAL", "WRONG_DIRECTION"}
MANUAL_RESULT = {
    "GOOD_ENTRY": "WIN",
    "BAD_SIGNAL": "LOSS",
    "LATE_ENTRY": "LOSS",
    "FAKE_SIGNAL": "LOSS",
    "WRONG_DIRECTION": "LOSS",
}
MIN_EXPECTANCY_IMPROVEMENT_R = 0.02


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def prepare_samples(rows: Iterable[dict]) -> tuple[list[dict], int]:
    """Normalize joined DB rows and apply the latest manual label over auto result."""
    samples = []
    conflicts = 0
    for row in rows:
        features = row.get("features") or {}
        manual = str(row.get("manual_label") or "").upper() or None
        auto = str(row.get("auto_result") or "").upper() or None
        effective = MANUAL_RESULT.get(manual, auto)
        realized_r = _finite(row.get("realized_r"))
        if effective not in OUTCOME_LABELS or realized_r is None:
            continue
        if manual in MANUAL_RESULT and auto in OUTCOME_LABELS and MANUAL_RESULT[manual] != auto:
            conflicts += 1
        timestamp = int(row.get("candle_open_ms") or 0)
        hour = features.get("hour_utc")
        if hour is None and timestamp > 0:
            hour = (timestamp // 3_600_000) % 24
        samples.append({
            "signal_id": int(row.get("signal_id") or 0),
            "candle_open_ms": timestamp,
            "direction": str(row.get("direction") or "").upper(),
            "features": dict(features),
            "adx": _finite(features.get("adx14", features.get("adx"))),
            "rsi": _finite(features.get("rsi14", features.get("rsi"))),
            "realized_r": realized_r,
            "auto_result": auto,
            "manual_label": manual,
            "effective_result": effective,
            "hour_utc": int(hour) if hour is not None else None,
        })
    samples.sort(key=lambda item: (item["candle_open_ms"], item["signal_id"]))
    return samples, conflicts


def _metrics(samples: list[dict]) -> dict:
    if not samples:
        return {"sample_count": 0, "win_rate": None, "expectancy_r": None,
                "average_r": None, "false_signal_rate": None}
    wins = sum(item["effective_result"] == "WIN" for item in samples)
    # WRONG_DIRECTION is an explicit manual negative label and belongs
    # in the denominator, just like BAD_SIGNAL/LATE_ENTRY/FAKE_SIGNAL.
    manual = [item for item in samples if item["manual_label"] in MANUAL_RESULT]
    false_count = sum(item["manual_label"] in FALSE_LABELS for item in manual)
    expectancy = sum(item["realized_r"] for item in samples) / len(samples)
    return {
        "sample_count": len(samples),
        "win_rate": round(wins / len(samples), 4),
        "expectancy_r": round(expectancy, 4),
        "average_r": round(expectancy, 4),
        "false_signal_rate": round(false_count / len(manual), 4) if manual else None,
    }


def _bucket(value: float, edges: list[float]) -> str:
    if value < edges[0]:
        return f"<{edges[0]:g}"
    for left, right in zip(edges, edges[1:]):
        if left <= value < right:
            return f"{left:g}-{right:g}"
    return f"{edges[-1]:g}+"


def _directional_vwap_distance_pct(sample: dict) -> float | None:
    """Return VWAP distance signed in the candidate's trade direction.

    Positive means price is above VWAP for LONG or below VWAP for SHORT.
    Negative means the candidate is on the wrong side of VWAP.
    """
    direction = str(sample.get("direction") or "").upper()
    if direction not in {"LONG", "SHORT"}:
        return None
    features = sample.get("features") or {}
    distance = _finite(features.get("price_vwap_distance_pct"))
    if distance is None:
        close = _finite(features.get("close"))
        vwap = _finite(features.get("vwap"))
        if close is None or vwap in (None, 0):
            return None
        distance = (close - vwap) / abs(vwap) * 100.0
    return distance if direction == "LONG" else -distance


def _analysis_buckets(samples: list[dict], min_bucket_samples: int) -> dict:
    definitions = {
        "adx14": ([20, 25, 30], lambda x: x["adx"]),
        "rsi14": ([40, 50, 60, 70], lambda x: x["rsi"]),
        "rsi_long_band": ([40, 50, 52, 60, 70], lambda x: (
            x["rsi"] if x["direction"] == "LONG" else None
        )),
        "rsi_short_band": ([25, 30, 35, 42, 50], lambda x: (
            x["rsi"] if x["direction"] == "SHORT" else None
        )),
        "ema_spread_pct": ([0.05, 0.15, 0.30], lambda x: (
            abs(_finite(x["features"].get("ema_spread_pct")))
            if _finite(x["features"].get("ema_spread_pct")) is not None
            else (abs(_finite(x["features"].get("ema9")) - _finite(x["features"].get("ema21"))) /
                  abs(_finite(x["features"].get("ema21"))) * 100
                  if _finite(x["features"].get("ema9")) is not None
                  and _finite(x["features"].get("ema21")) not in (None, 0) else None)
        )),
        "vwap_distance_pct": ([0.10, 0.30, 0.60], lambda x: (
            abs(_finite(x["features"].get("price_vwap_distance_pct")))
            if x["features"].get("price_vwap_distance_pct") is not None
            else (abs(_finite(x["features"].get("close")) - _finite(x["features"].get("vwap"))) /
                  abs(_finite(x["features"].get("vwap"))) * 100
                  if _finite(x["features"].get("close")) is not None
                  and _finite(x["features"].get("vwap")) not in (None, 0) else None)
        )),
        "directional_vwap_distance_pct": ([0.10, 0.30, 0.60], _directional_vwap_distance_pct),
        "relative_volume_20": ([1, 1.5, 2], lambda x: _finite(x["features"].get("relative_volume_20"))),
        "cooldown_candles": ([2, 3, 4, 5, 6, 8], lambda x: _finite(x["features"].get("cooldown_candles"))),
        "hour_utc": ([6, 12, 18], lambda x: float(x["hour_utc"]) if x["hour_utc"] is not None else None),
    }
    output = {}
    for name, (edges, getter) in definitions.items():
        groups: dict[str, list[dict]] = defaultdict(list)
        for sample in samples:
            value = getter(sample)
            if value is None:
                continue
            groups[_bucket(float(value), edges)].append(sample)
        buckets = []
        for label, group in sorted(groups.items()):
            count = len(group)
            buckets.append({
                "bucket": label,
                "sample_count": count,
                "metrics": _metrics(group) if count >= min_bucket_samples else None,
                "suppressed_below_minimum": count < min_bucket_samples,
            })
        output[name] = {
            "available_samples": sum(len(group) for group in groups.values()),
            "minimum_bucket_samples": min_bucket_samples,
            "buckets": buckets,
        }
    return output


def _passes(sample: dict, config: dict) -> bool:
    adx, rsi = sample["adx"], sample["rsi"]
    if adx is None or rsi is None:
        return False
    if adx < config["adx_min"]:
        return False
    if sample["direction"] == "LONG":
        return config["rsi_long_min"] <= rsi <= config["rsi_long_max"]
    if sample["direction"] == "SHORT":
        return config["rsi_short_min"] <= rsi <= config["rsi_short_max"]
    return False


def _candidate_configs(current: dict) -> list[dict]:
    adx_values = sorted({current["adx_min"], current["adx_min"] + 2, current["adx_min"] + 4})
    long_min_values = sorted({current["rsi_long_min"], current["rsi_long_min"] + 2,
                              current["rsi_long_min"] + 4})
    long_max_values = sorted({current["rsi_long_max"], current["rsi_long_max"] - 2})
    short_min_values = sorted({current["rsi_short_min"], current["rsi_short_min"] + 2})
    short_max_values = sorted({current["rsi_short_max"], current["rsi_short_max"] - 2,
                               current["rsi_short_max"] - 4})
    candidates = []
    for values in itertools.product(adx_values, long_min_values, long_max_values,
                                    short_min_values, short_max_values):
        candidate = dict(zip(
            ("adx_min", "rsi_long_min", "rsi_long_max", "rsi_short_min", "rsi_short_max"),
            values,
        ))
        if (candidate["rsi_long_min"] >= candidate["rsi_long_max"]
                or candidate["rsi_short_min"] >= candidate["rsi_short_max"]):
            continue
        # Only preserve or tighten current filters; never propose a looser rule.
        if any(candidate[key] < current[key] for key in candidate):
            continue
        candidates.append(candidate)
    return candidates


def _directional_validation(samples: list[dict], base: dict, min_samples: int, min_bucket_samples: int) -> dict:
    """Validate candidate filters independently for LONG and SHORT."""
    report = {}
    for direction in ("LONG", "SHORT"):
        side = [item for item in samples if item["direction"] == direction]
        result = {"sample_count": len(side), "baseline": _metrics(side), "status": "INSUFFICIENT_DATA"}
        if len(side) < min_samples:
            result["reason"] = f"Need at least {min_samples} resolved {direction} samples; found {len(side)}."
            report[direction] = result
            continue
        split = int(len(side) * 0.70)
        train, oos = side[:split], side[split:]
        if len(train) < min_bucket_samples or len(oos) < 20:
            result["reason"] = "Insufficient chronological train/OOS samples for this direction."
            report[direction] = result
            continue
        best = None
        for config in _candidate_configs(base):
            selected = [item for item in train if _passes(item, config)]
            if len(selected) < min_bucket_samples:
                continue
            metrics = _metrics(selected)
            changes = sum(abs(config[key] - base[key]) > 1e-9 for key in base)
            distance = sum(abs(config[key] - base[key]) for key in base)
            score = (metrics["expectancy_r"], -changes, -distance)
            if best is None or score > best["score"]:
                best = {"config": config, "metrics": metrics, "score": score}
        if best is None:
            result["reason"] = "No candidate retained the minimum directional training sample."
            report[direction] = result
            continue
        baseline_oos = _metrics(oos)
        selected_oos = [item for item in oos if _passes(item, best["config"])]
        candidate_oos = _metrics(selected_oos)
        improvement = (candidate_oos["expectancy_r"] - baseline_oos["expectancy_r"]
                       if candidate_oos["expectancy_r"] is not None and baseline_oos["expectancy_r"] is not None else None)
        passed = (len(selected_oos) >= 10 and improvement is not None
                  and improvement >= MIN_EXPECTANCY_IMPROVEMENT_R
                  and candidate_oos["expectancy_r"] is not None and candidate_oos["expectancy_r"] > 0
                  and best["metrics"]["expectancy_r"] > 0
                  and candidate_oos["expectancy_r"] >= best["metrics"]["expectancy_r"] * 0.5)
        result.update({
            "status": "PASS" if passed else "NO_OOS_IMPROVEMENT",
            "train": best["metrics"], "baseline_oos": baseline_oos,
            "candidate_oos": candidate_oos, "candidate_config": best["config"],
            "expectancy_improvement_r": round(improvement, 4) if improvement is not None else None,
            "selected_oos_sample_count": len(selected_oos),
            "reason": None if passed else "Candidate failed independent directional OOS guards.",
        })
        report[direction] = result
    return report

def build_training_proposal(
    rows: Iterable[dict],
    current_config: dict,
    *,
    base_config_version: str = "v2",
    min_samples: int = 100,
    min_bucket_samples: int = 30,
) -> dict:
    """Build a research proposal only when sample and walk-forward guards pass."""
    samples, conflicts = prepare_samples(rows)
    analysis = _analysis_buckets(samples, min_bucket_samples)
    directional_validation = _directional_validation(samples, {
        key: float(current_config[key])
        for key in ("adx_min", "rsi_long_min", "rsi_long_max", "rsi_short_min", "rsi_short_max")
    }, min_samples, min_bucket_samples)
    base = {
        key: float(current_config[key])
        for key in ("adx_min", "rsi_long_min", "rsi_long_max", "rsi_short_min", "rsi_short_max")
    }
    summary = {
        "sample_count": len(samples),
        "manual_label_conflicts": conflicts,
        "minimum_samples": min_samples,
        "minimum_bucket_samples": min_bucket_samples,
        "bucket_analysis": analysis,
        "directional_validation": directional_validation,
    }
    if len(samples) < min_samples:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": f"Need at least {min_samples} resolved labeled signals; found {len(samples)}.",
            "base_config_version": base_config_version,
            "summary": summary,
        }

    split = int(len(samples) * 0.70)
    train = samples[:split]
    oos = samples[split:]
    if len(train) < min_bucket_samples or len(oos) < 2 * 10:
        return {
            "status": "INSUFFICIENT_DATA",
            "reason": "Not enough chronological train/test data for walk-forward validation.",
            "base_config_version": base_config_version,
            "summary": summary,
        }
    baseline_train = _metrics(train)
    best = None
    for candidate in _candidate_configs(base):
        selected = [item for item in train if _passes(item, candidate)]
        if len(selected) < min_bucket_samples:
            continue
        metric = _metrics(selected)
        changes = sum(abs(candidate[key] - base[key]) > 1e-9 for key in base)
        delta = sum(abs(candidate[key] - base[key]) for key in base)
        score = (metric["expectancy_r"], -changes, -delta)
        if best is None or score > best["score"]:
            best = {"config": candidate, "metrics": metric, "score": score}

    if best is None:
        return {
            "status": "NO_VALID_CANDIDATE",
            "reason": "No stricter parameter set retained the minimum training sample.",
            "base_config_version": base_config_version,
            "summary": summary,
        }

    midpoint = len(oos) // 2
    test_folds = [oos[:midpoint], oos[midpoint:]]
    fold_results = []
    stable = True
    for index, fold in enumerate(test_folds, start=1):
        baseline_fold = _metrics(fold)
        selected_fold = [item for item in fold if _passes(item, best["config"])]
        candidate_fold = _metrics(selected_fold)
        improvement = (
            candidate_fold["expectancy_r"] - baseline_fold["expectancy_r"]
            if candidate_fold["expectancy_r"] is not None and baseline_fold["expectancy_r"] is not None
            else None
        )
        if (len(selected_fold) < 10 or improvement is None
                or improvement < MIN_EXPECTANCY_IMPROVEMENT_R
                or candidate_fold["expectancy_r"] is None
                or candidate_fold["expectancy_r"] <= 0):
            stable = False
        fold_results.append({
            "fold": index,
            "baseline": baseline_fold,
            "candidate": candidate_fold,
            "expectancy_improvement_r": round(improvement, 4) if improvement is not None else None,
        })

    changes = {
        key: {"from": base[key], "to": best["config"][key]}
        for key in base if abs(best["config"][key] - base[key]) > 1e-9
    }
    oos_candidate_samples = [item for item in oos if _passes(item, best["config"])]
    oos_metrics = _metrics(oos_candidate_samples)
    oos_span_days = max(
        1.0, (oos[-1]["candle_open_ms"] - oos[0]["candle_open_ms"]) / 86_400_000
    )
    baseline_resolved_signals_per_day = round(len(oos) / oos_span_days, 2)
    candidate_resolved_signals_per_day = round(
        len(oos_candidate_samples) / oos_span_days, 2
    )
    train_test_gap = (
        best["metrics"]["expectancy_r"] - oos_metrics["expectancy_r"]
        if oos_metrics["expectancy_r"] is not None else None
    )
    train_expectancy = best["metrics"]["expectancy_r"]
    test_expectancy = oos_metrics["expectancy_r"]
    train_test_collapse = (
        train_expectancy > 0
        and (test_expectancy is None or test_expectancy < train_expectancy * 0.5)
    )
    directional_pass = all(directional_validation[side].get("status") == "PASS" for side in ("LONG", "SHORT"))
    if (not changes or not stable or not directional_pass or test_expectancy is None
            or test_expectancy <= 0 or train_test_collapse):
        return {
            "status": "NO_OOS_IMPROVEMENT",
            "reason": "No stricter candidate passed combined and independent LONG/SHORT out-of-sample improvement and train/test-collapse guards.",
            "base_config_version": base_config_version,
            "summary": summary,
            "walk_forward": {
                "train": best["metrics"],
                "baseline_train": baseline_train,
                "folds": fold_results,
                "train_test_gap_r": round(train_test_gap, 4) if train_test_gap is not None else None,
            },
        }

    proposed_version = f"v{int(str(base_config_version).lstrip('v') or 2) + 1}"
    return {
        "status": "PROPOSED",
        "base_config_version": base_config_version,
        "proposed_config_version": proposed_version,
        "changes": changes,
        "proposed_config": best["config"],
        "summary": summary,
        "expected_effect": {
            "train": best["metrics"],
            "out_of_sample": oos_metrics,
            "baseline_train": baseline_train,
            "walk_forward_folds": fold_results,
            "train_test_gap_r": round(train_test_gap, 4) if train_test_gap is not None else None,
            "signals_retained_pct": round(100 * len(oos_candidate_samples) / len(oos), 2),
            "baseline_resolved_signals_per_day": baseline_resolved_signals_per_day,
            "candidate_resolved_signals_per_day": candidate_resolved_signals_per_day,
            "signal_rate_basis": (
                "Estimated from resolved delivered signals in the chronological OOS sample; "
                "not a forecast of all future alerts."
            ),
        },
        "confidence_note": (
            "Research proposal only. It passed both chronological out-of-sample folds, "
            "retained the configured minimum samples, and only tightens existing filters. "
            "Human approval is required before any live configuration change."
        ),
    }
