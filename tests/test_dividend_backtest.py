# -*- coding: utf-8 -*-
import gc
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dividend_backtest import load_backtest_data, next_trading_date_after, period_return, summarize_backtest


class DividendBacktestTest(unittest.TestCase):
    def test_next_trading_date_after_uses_following_session(self):
        dates = [
            pd.Timestamp("2026-01-30"),
            pd.Timestamp("2026-02-02"),
            pd.Timestamp("2026-02-03"),
        ]

        self.assertEqual(
            next_trading_date_after(dates, pd.Timestamp("2026-01-30")),
            pd.Timestamp("2026-02-02"),
        )
        self.assertIsNone(next_trading_date_after(dates, pd.Timestamp("2026-02-03")))

    def test_load_backtest_data_includes_indicator_lookback(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "test.db"
            with closing(sqlite3.connect(db_path)) as conn:
                conn.executescript("""
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    volume REAL
                );
                CREATE TABLE fundamentals (
                    code TEXT,
                    coname TEXT,
                    s33nm TEXT,
                    mktnm TEXT
                );
                CREATE TABLE dividend_financials (
                    code TEXT,
                    disclosure_date TEXT,
                    period TEXT
                );
                """)
                conn.executemany(
                    "INSERT INTO prices(date, code, close, volume) VALUES (?, ?, ?, ?)",
                    [
                        ("2025-12-15", "11110", 100.0, 10000),
                        ("2026-01-15", "11110", 110.0, 11000),
                    ],
                )
                conn.execute(
                    "INSERT INTO dividend_financials(code, disclosure_date, period) VALUES (?, ?, ?)",
                    ("11110", "2025-12-31", "2025Q4"),
                )
                conn.commit()

            data = load_backtest_data(
                db_path,
                "2026-01-01",
                indicator_lookback_days=30,
            )
            price_dates = data["prices"]["date"].dt.strftime("%Y-%m-%d").tolist()
            del data
            gc.collect()

        self.assertEqual(
            price_dates,
            ["2025-12-15", "2026-01-15"],
        )

    def test_period_return_uses_equal_weight_and_monthly_dividend(self):
        prices = pd.DataFrame([
            {"date": pd.Timestamp("2026-01-31"), "code": "11110", "close": 100.0},
            {"date": pd.Timestamp("2026-02-28"), "code": "11110", "close": 110.0},
            {"date": pd.Timestamp("2026-01-31"), "code": "22220", "close": 200.0},
            {"date": pd.Timestamp("2026-02-28"), "code": "22220", "close": 190.0},
        ])
        selected = pd.DataFrame([
            {"code": "11110", "dividend_yield": 6.0},
            {"code": "22220", "dividend_yield": 6.0},
        ])

        result = period_return(
            prices,
            selected,
            pd.Timestamp("2026-01-31"),
            pd.Timestamp("2026-02-28"),
            include_dividend=True,
        )

        self.assertEqual(result["holdings"], 2)
        self.assertAlmostEqual(result["price_return"], 0.025)
        self.assertAlmostEqual(result["dividend_return"], 0.005)
        self.assertAlmostEqual(result["return"], 0.03)

    def test_summarize_backtest_returns_kpis(self):
        results = pd.DataFrame([
            {"return": 0.10, "equity": 1.10, "drawdown": 0.0, "holdings": 10},
            {"return": -0.05, "equity": 1.045, "drawdown": -0.05, "holdings": 8},
        ])

        summary = summarize_backtest(results)

        self.assertEqual(summary["months"], 2)
        self.assertAlmostEqual(summary["total_return"], 0.045)
        self.assertAlmostEqual(summary["max_drawdown"], -0.05)
        self.assertAlmostEqual(summary["win_rate"], 0.5)
        self.assertAlmostEqual(summary["avg_holdings"], 9.0)


if __name__ == "__main__":
    unittest.main()
