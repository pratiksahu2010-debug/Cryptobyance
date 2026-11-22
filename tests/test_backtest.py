import unittest

import pandas as pd
from unittest.mock import patch

from backtest import (
    build_replay_training_preview, completed_context_15m,
    run_backtest, validate_history_frame,
)


def frame(interval_ms, count, *, start=0):
    rows = []
    for index in range(count):
        rows.append({
            "timestamp": start + index * interval_ms,
            "open": 100.0, "high": 101.0, "low": 99.0,
            "close": 100.0, "volume": 10.0,
        })
    return pd.DataFrame(rows)


class BacktestTests(unittest.TestCase):
    def test_historical_frame_requires_contiguous_candles(self):
        candles = frame(300_000, 4)
        candles.loc[3, "timestamp"] += 300_000
        with self.assertRaisesRegex(ValueError, "missing or overlapping"):
            validate_history_frame(candles, 300_000, "5m")

    def test_historical_frame_rejects_duplicate_timestamps(self):
        candles = frame(300_000, 4)
        candles.loc[3, "timestamp"] = candles.loc[2, "timestamp"]
        with self.assertRaisesRegex(ValueError, "duplicate"):
            validate_history_frame(candles, 300_000, "5m")

    def test_context_filter_excludes_unclosed_15m_candle(self):
        candles = frame(900_000, 4)
        available = completed_context_15m(candles, current_5m_close_ms=1_800_000)
        self.assertEqual(available["timestamp"].tolist(), [0, 900_000])

    def test_resolved_replay_outcomes_enter_the_offline_trainer(self):
        preview = build_replay_training_preview([{
            "signal_id": 1, "signal_candle_open_ms": 1_800_000,
            "side": "LONG", "entry": 100.0, "stop": 98.0,
            "target1": 102.0, "target2": 104.0,
            "ema9": 101.0, "ema21": 100.0, "vwap": 99.5,
            "adx": 27.0, "rsi": 58.0,
            "diagnostics": {"adx14": 27.0, "rsi14": 58.0},
            "result": "WIN", "realized_r": 1.0,
        }])
        self.assertEqual(preview["status"], "INSUFFICIENT_DATA")
        self.assertEqual(preview["sample_count"], 1)
        self.assertTrue(preview["synthetic_replay_data"])
        self.assertFalse(preview["persisted_to_database"])
        self.assertFalse(preview["live_settings_changed"])

    def test_replay_runs_the_normal_5m_model_without_15m_data(self):
        five = frame(300_000, 80)
        inputs = {
            "LONG": {"setup_valid": True, "invalidation": False, "trigger": True},
            "SHORT": {"setup_valid": False, "invalidation": False, "trigger": False},
        }
        setup = {
            "side": "LONG", "entry": 100.0, "stop": 98.0,
            "target": 102.0, "target1": 102.0, "target2": 104.0,
            "risk_distance": 2.0, "atr": 2.0, "risk_model": "swing_structure",
            "adx": 25.0, "rsi": 55.0, "ema9": 101.0, "ema21": 100.0,
            "vwap": 99.5, "diagnostics": {},
        }
        with patch("backtest.evaluate_state_inputs", return_value=inputs) as state_inputs:
            with patch("backtest.evaluate_setup", return_value=setup):
                result = run_backtest(
                    five, None, cooldown_candles=4, horizon_bars=12, cost_pct=0.10
                )
        self.assertEqual(result["summary"]["signals"], 1)
        self.assertEqual(result["trades"][0]["side"], "LONG")
        self.assertTrue(all(call.args[1] is None for call in state_inputs.call_args_list))

    def test_short_15m_history_does_not_delay_5m_replay(self):
        five = frame(300_000, 80)
        short_context = frame(900_000, 10)
        inputs = {
            "LONG": {"setup_valid": True, "invalidation": False, "trigger": True},
            "SHORT": {"setup_valid": False, "invalidation": False, "trigger": False},
        }
        setup = {
            "side": "LONG", "entry": 100.0, "stop": 98.0,
            "target": 102.0, "target1": 102.0, "target2": 104.0,
            "risk_distance": 2.0, "atr": 2.0, "risk_model": "swing_structure",
            "adx": 25.0, "rsi": 55.0, "ema9": 101.0, "ema21": 100.0,
            "vwap": 99.5, "diagnostics": {},
        }
        with patch("backtest.evaluate_state_inputs", return_value=inputs) as state_inputs:
            with patch("backtest.evaluate_setup", return_value=setup):
                result = run_backtest(five, short_context)
        self.assertEqual(result["summary"]["signals"], 1)
        self.assertTrue(all(len(call.args[1]) < 35 for call in state_inputs.call_args_list))

    def test_flat_market_replay_produces_no_false_signals(self):
        five = frame(300_000, 200)
        fifteen = frame(900_000, 40)
        result = run_backtest(five, fifteen, cooldown_candles=4, horizon_bars=12, cost_pct=0.10)
        self.assertEqual(result["summary"]["signals"], 0)
        self.assertTrue(result["summary"]["no_lookahead"])
        self.assertEqual(result["trades"], [])
        self.assertEqual(result["training_preview"]["status"], "INSUFFICIENT_DATA")
        self.assertFalse(result["training_preview"]["persisted_to_database"])


if __name__ == "__main__":
    unittest.main()
