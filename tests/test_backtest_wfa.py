import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.backtest_wfa import (
    ExecutionConfig,
    PortfolioSimulator,
    StrategyParams,
    build_walk_forward_folds,
    connect_read_only,
    load_price_history,
    prepare_price_history,
    run_walk_forward,
    summarize_portfolio,
)


def prepared_rows(
    dates,
    codes=("10000",),
    *,
    signal_date=None,
    overrides=None,
):
    overrides = overrides or {}
    rows = []
    for date in dates:
        for code in codes:
            values = {
                "basis_open": 100.0,
                "basis_high": 101.0,
                "basis_low": 99.0,
                "basis_close": 100.0,
            }
            values.update(overrides.get((pd.Timestamp(date), code), {}))
            is_signal = pd.Timestamp(date) == pd.Timestamp(signal_date)
            rows.append(
                {
                    "date": pd.Timestamp(date),
                    "code": code,
                    **values,
                    "indicator_ready": True,
                    "gc_trend": is_signal,
                    "is_bullish": True,
                    "ma_short": 105.0 if is_signal else 95.0,
                    "priority_score": 0.90 if is_signal else 1.05,
                }
            )
    return pd.DataFrame(rows)


class BacktestWfaTest(unittest.TestCase):
    def setUp(self):
        self.params = StrategyParams(
            dip_threshold=0.97,
            stop_loss=0.20,
            trailing_stop=0.20,
            market_threshold=0.0,
        )

    def test_read_only_connection_rejects_writes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.db"
            connection = sqlite3.connect(path)
            try:
                connection.execute("CREATE TABLE sample (id INTEGER)")
                connection.commit()
            finally:
                connection.close()

            connection = connect_read_only(path)
            try:
                with self.assertRaises(sqlite3.OperationalError):
                    connection.execute("INSERT INTO sample VALUES (1)")
            finally:
                connection.close()

    def test_loader_uses_database_scale_category_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.db"
            connection = sqlite3.connect(path)
            try:
                connection.execute(
                    "CREATE TABLE prices (date TEXT, code TEXT, open REAL, "
                    "high REAL, low REAL, close REAL, volume REAL, "
                    "adjustmentfactor REAL)"
                )
                connection.execute(
                    "CREATE TABLE fundamentals (code TEXT, scalecat TEXT)"
                )
                for code in ("10000", "20000"):
                    connection.execute(
                        "INSERT INTO prices VALUES "
                        "('2026-01-05', ?, 100, 101, 99, 100, 1000, 1)",
                        (code,),
                    )
                connection.execute(
                    "INSERT INTO fundamentals VALUES ('10000', 'TOPIX Small 1')"
                )
                connection.execute(
                    "INSERT INTO fundamentals VALUES ('20000', '-')"
                )
                connection.commit()
            finally:
                connection.close()

            result = load_price_history(
                path,
                start_date="2026-01-05",
                lookback_days=0,
            )

        self.assertEqual(result["code"].tolist(), ["10000"])

    def test_signal_executes_at_next_session_open_with_costs(self):
        dates = pd.bdate_range("2026-01-05", periods=4)
        prices = prepared_rows(
            dates,
            signal_date=dates[1],
            overrides={
                (dates[1], "10000"): {"basis_close": 90.0},
                (dates[2], "10000"): {
                    "basis_open": 110.0,
                    "basis_high": 112.0,
                    "basis_low": 109.0,
                    "basis_close": 111.0,
                },
            },
        )
        execution = ExecutionConfig(
            initial_capital=10_000,
            max_positions=1,
            lot_size=1,
            commission_bps=10,
            slippage_bps=5,
        )

        equity, trades = PortfolioSimulator(prices, execution).run(
            self.params,
            start_date=dates[0],
            end_date=dates[-1],
        )

        self.assertEqual(trades.iloc[0]["entry_date"], dates[2])
        self.assertEqual(trades.iloc[0]["signal_date"], dates[1])
        self.assertLess(trades.iloc[0]["signal_date"], trades.iloc[0]["entry_date"])
        self.assertAlmostEqual(trades.iloc[0]["entry_fill"], 110.055)
        self.assertEqual(trades.iloc[0]["reason"], "EndOfFold")
        self.assertGreater(trades.iloc[0]["entry_fee"], 0)
        self.assertGreater(trades.iloc[0]["exit_fee"], 0)
        self.assertTrue((equity["cash"] >= 0).all())

    def test_gap_stop_uses_open_instead_of_unreachable_stop_price(self):
        dates = pd.bdate_range("2026-01-05", periods=4)
        prices = prepared_rows(
            dates,
            signal_date=dates[0],
            overrides={
                (dates[2], "10000"): {
                    "basis_open": 75.0,
                    "basis_high": 78.0,
                    "basis_low": 70.0,
                    "basis_close": 76.0,
                }
            },
        )
        execution = ExecutionConfig(
            initial_capital=10_000,
            max_positions=1,
            lot_size=1,
            commission_bps=0,
            slippage_bps=0,
        )

        _, trades = PortfolioSimulator(prices, execution).run(
            self.params,
            start_date=dates[0],
            end_date=dates[-1],
        )

        self.assertEqual(trades.iloc[0]["reason"], "GapStop")
        self.assertEqual(trades.iloc[0]["exit_fill"], 75.0)

    def test_trailing_stop_uses_prior_high(self):
        dates = pd.bdate_range("2026-01-05", periods=4)
        prices = prepared_rows(
            dates,
            signal_date=dates[0],
            overrides={
                (dates[1], "10000"): {
                    "basis_open": 100.0,
                    "basis_high": 130.0,
                    "basis_low": 99.0,
                    "basis_close": 125.0,
                },
                (dates[2], "10000"): {
                    "basis_open": 120.0,
                    "basis_high": 125.0,
                    "basis_low": 100.0,
                    "basis_close": 110.0,
                },
            },
        )
        execution = ExecutionConfig(
            initial_capital=10_000,
            max_positions=1,
            lot_size=1,
            commission_bps=0,
            slippage_bps=0,
        )

        _, trades = PortfolioSimulator(prices, execution).run(
            self.params,
            start_date=dates[0],
            end_date=dates[-1],
        )

        self.assertEqual(trades.iloc[0]["reason"], "StopOrTrail")
        self.assertEqual(trades.iloc[0]["exit_fill"], 104.0)

    def test_cash_position_and_lot_constraints_hold(self):
        dates = pd.bdate_range("2026-01-05", periods=3)
        prices = prepared_rows(
            dates,
            codes=("10000", "20000", "30000"),
            signal_date=dates[0],
        )
        execution = ExecutionConfig(
            initial_capital=1_000,
            max_positions=2,
            lot_size=2,
            commission_bps=0,
            slippage_bps=0,
        )

        equity, trades = PortfolioSimulator(prices, execution).run(
            self.params,
            start_date=dates[0],
            end_date=dates[-1],
        )

        self.assertEqual(len(trades), 2)
        self.assertTrue((trades["qty"] % 2 == 0).all())
        self.assertLessEqual(equity["positions"].max(), 2)
        self.assertTrue((equity["cash"] >= 0).all())

    def test_weekend_end_date_liquidates_on_last_available_session(self):
        dates = pd.bdate_range("2026-01-05", periods=3)
        prices = prepared_rows(dates, signal_date=dates[0])
        execution = ExecutionConfig(
            initial_capital=10_000,
            max_positions=1,
            lot_size=1,
            commission_bps=0,
            slippage_bps=0,
        )

        _, trades = PortfolioSimulator(prices, execution).run(
            self.params,
            start_date=dates[0],
            end_date=dates[-1] + pd.Timedelta(days=3),
        )

        self.assertEqual(trades.iloc[0]["exit_date"], dates[-1])
        self.assertEqual(trades.iloc[0]["reason"], "EndOfFold")

    def test_unverified_code_is_excluded_instead_of_adjusted_by_guess(self):
        prices = pd.DataFrame(
            [
                {
                    "date": "2026-01-05",
                    "code": "10000",
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "adjustmentfactor": None,
                },
                {
                    "date": "2026-01-06",
                    "code": "10000",
                    "open": 40,
                    "high": 41,
                    "low": 39,
                    "close": 40,
                    "adjustmentfactor": None,
                },
                {
                    "date": "2026-01-05",
                    "code": "20000",
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "adjustmentfactor": None,
                },
            ]
        )

        prepared, quality = prepare_price_history(prices, ma_short=1, ma_long=2)

        self.assertEqual(set(prepared["code"]), {"20000"})
        self.assertEqual(quality["unverified_codes"], 1)
        self.assertEqual(quality["excluded_codes"], 1)

    def test_compound_summary_uses_equity_curve_and_drawdown(self):
        equity = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-01-01", "2025-07-01", "2026-01-01"]),
                "equity": [100.0, 110.0, 99.0],
                "drawdown": [0.0, 0.0, -0.10],
                "positions": [0, 1, 0],
            }
        )

        result = summarize_portfolio(equity, pd.DataFrame(), initial_capital=100)

        self.assertAlmostEqual(result["total_return"], -0.01)
        self.assertAlmostEqual(result["max_drawdown"], -0.10)
        self.assertAlmostEqual(result["final_equity"], 99.0)

    def test_walk_forward_folds_have_expanding_train_and_disjoint_test(self):
        dates = list(pd.bdate_range("2026-01-05", periods=12))
        folds = build_walk_forward_folds(dates, 3)

        self.assertEqual(len(folds), 3)
        for train, test in folds:
            self.assertLess(max(train), min(test))
            self.assertFalse(set(train) & set(test))
        self.assertLess(len(folds[0][0]), len(folds[-1][0]))

    def test_walk_forward_returns_only_disjoint_test_dates(self):
        dates = pd.bdate_range("2026-01-05", periods=12)
        prices = prepared_rows(
            dates,
            signal_date=dates[4],
        )
        execution = ExecutionConfig(
            initial_capital=10_000,
            max_positions=1,
            lot_size=1,
            commission_bps=0,
            slippage_bps=0,
        )

        result = run_walk_forward(
            prices,
            start_date=dates[0].date().isoformat(),
            end_date=dates[-1].date().isoformat(),
            n_splits=3,
            execution=execution,
            param_grid=(self.params,),
            minimum_train_trades=0,
        )

        folds = result["folds"]
        self.assertEqual(len(folds), 3)
        self.assertTrue(
            (
                pd.to_datetime(folds["train_end"])
                < pd.to_datetime(folds["test_start"])
            ).all()
        )
        self.assertFalse(result["equity"]["date"].duplicated().any())
        self.assertEqual(result["summary"]["folds"], 3)
        self.assertIn("cagr_passed", result["summary"]["targets"])
        self.assertIn("max_drawdown_passed", result["summary"]["targets"])
        self.assertTrue((folds["test_min_cash"] >= 0).all())
        self.assertTrue((folds["test_max_positions"] <= 1).all())


if __name__ == "__main__":
    unittest.main()
