import unittest

from entry_timing import train_entry_timing


class EntryTimingTests(unittest.TestCase):

    def test_daily_trainer_selects_supported_five_minute_entry_delay():
        signals = []
        candles_by_symbol = {}
        for index in range(20):
            symbol = f"COIN{index}/USDT"
            signal_ts = 1_800_000_000_000 + index * 10_000_000
            signals.append({
                "signal_id": index + 1,
                "symbol": symbol,
                "direction": "LONG",
                "candle_open_ms": signal_ts,
                "entry": 100.0,
                "stop": 99.0,
                "target1": 101.0,
                "risk_per_unit": 1.0,
                "outcome_horizon_bars": 12,
                "cost_pct": 0.0,
            })
            candles = {}
            for bar in range(1, 16):
                candles[signal_ts + bar * 300_000] = {
                    "open": 100.0 if bar == 1 else (99.9 if bar == 2 else 100.9),
                    "high": 100.95,
                    "low": 99.8,
                    "close": 100.9,
                    "volume": 10.0,
                }
            candles_by_symbol[symbol] = candles

        report = train_entry_timing(signals, candles_by_symbol)
        long_result = report["directions"]["LONG"]

        assert long_result["status"] == "POLICY_READY"
        assert long_result["policy"]["delay_bars"] == 1
        assert long_result["policy"]["sample_count"] == 20
        assert report["alerts_delayed"] is False
        assert report["live_strategy_changed"] is False


    def test_daily_trainer_holds_immediate_entry_when_sample_is_small():
        signals = [{
            "signal_id": index + 1,
            "symbol": f"COIN{index}/USDT",
            "direction": "SHORT",
            "candle_open_ms": 1_800_000_000_000,
            "entry": 100.0,
            "stop": 101.0,
            "target1": 99.0,
            "risk_per_unit": 1.0,
            "outcome_horizon_bars": 12,
            "cost_pct": 0.1,
        } for index in range(5)]
        report = train_entry_timing(signals, {})

        assert report["directions"]["SHORT"]["status"] == "INSUFFICIENT_DATA"
        assert report["directions"]["SHORT"]["policy"] is None
