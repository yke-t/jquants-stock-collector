import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "analyze_signal_performance.py"
SPEC = importlib.util.spec_from_file_location("analyze_signal_performance", MODULE_PATH)
analysis = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = analysis
SPEC.loader.exec_module(analysis)


class SignalPerformanceAnalysisTest(unittest.TestCase):
    def test_read_only_connection_rejects_writes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.db"
            connection = sqlite3.connect(path)
            try:
                connection.execute("CREATE TABLE sample (id INTEGER)")
                connection.commit()
            finally:
                connection.close()

            connection = analysis.connect_read_only(path)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("INSERT INTO sample VALUES (1)")
            finally:
                connection.close()

    def test_explicit_split_factor_keeps_price_basis_continuous(self):
        prices = pd.DataFrame(
            [
                {"date": "2026-01-01", "code": "10000", "open": 100, "high": 102, "low": 98, "close": 100, "adjustmentfactor": None},
                {"date": "2026-01-02", "code": "10000", "open": 50, "high": 52, "low": 49, "close": 50, "adjustmentfactor": 0.5},
                {"date": "2026-01-03", "code": "10000", "open": 51, "high": 53, "low": 50, "close": 52, "adjustmentfactor": None},
            ]
        )

        result = analysis.add_share_basis_features(prices)

        self.assertEqual(result["basis_close"].round(2).tolist(), [50.0, 50.0, 52.0])
        self.assertFalse(result["unverified_gap"].any())

    def test_large_gap_without_factor_is_unverified(self):
        prices = pd.DataFrame(
            [
                {"date": "2026-01-01", "code": "10000", "open": 100, "high": 102, "low": 98, "close": 100, "adjustmentfactor": None},
                {"date": "2026-01-02", "code": "10000", "open": 40, "high": 42, "low": 39, "close": 40, "adjustmentfactor": None},
            ]
        )

        result = analysis.add_share_basis_features(prices)

        self.assertTrue(result.iloc[-1]["unverified_gap"])

    def test_signal_return_uses_explicit_factor(self):
        dates = pd.bdate_range("2026-01-01", periods=36)
        rows = []
        for index, current_date in enumerate(dates):
            split = 0.5 if index == 16 else None
            price = 100.0 if index < 16 else 55.0
            rows.append(
                {
                    "date": current_date,
                    "code": "10000",
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adjustmentfactor": split,
                }
            )
        signal_date = dates[14]
        signals = pd.DataFrame(
            [
                {
                    "signal_date": signal_date,
                    "code": "10000",
                    "name": "Sample",
                    "signal_price": 100,
                    "ma25_rate": -5,
                    "stop_loss": 90,
                    "take_profit": 110,
                    "verdict": "ENTRY",
                    "reason": "",
                    "news_hit": "",
                }
            ]
        )

        result = analysis.evaluate_signals(signals, pd.DataFrame(rows), eval_days=20).iloc[0]

        self.assertTrue(result["eval_complete"])
        self.assertEqual(result["share_basis_status"], "VERIFIED")
        self.assertAlmostEqual(result["return_pct"], 10.0)

    def test_non_overlapping_episode_keeps_first_signal(self):
        frame = pd.DataFrame(
            [
                {"code": "10000", "signal_date": pd.Timestamp("2026-01-01"), "eval_end_date": pd.Timestamp("2026-01-30")},
                {"code": "10000", "signal_date": pd.Timestamp("2026-01-15"), "eval_end_date": pd.Timestamp("2026-02-13")},
                {"code": "10000", "signal_date": pd.Timestamp("2026-02-02"), "eval_end_date": pd.Timestamp("2026-03-02")},
            ]
        )

        result = analysis.select_non_overlapping_episodes(frame)

        self.assertEqual(result["signal_date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-01-01", "2026-02-02"])

    def test_rsi_quintiles_produce_five_ordered_groups(self):
        frame = pd.DataFrame(
            {
                "code": [f"{index:05d}" for index in range(10)],
                "rsi": list(range(20, 30)),
                "return_pct": list(range(-5, 5)),
                "stop_hit": [False] * 10,
                "take_hit": [False] * 10,
            }
        )

        result = analysis.summarize_rsi_quintiles(frame)

        self.assertEqual(
            result["rsi_quintile"].astype(str).tolist(),
            ["Q1 (lowest)", "Q2", "Q3", "Q4", "Q5 (highest)"],
        )
        self.assertEqual(result["observations"].tolist(), [2, 2, 2, 2, 2])
        self.assertTrue(result["rsi_median"].is_monotonic_increasing)


if __name__ == "__main__":
    unittest.main()
