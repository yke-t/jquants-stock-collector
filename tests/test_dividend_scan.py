# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dividend_scan import (
    annotate_share_basis,
    apply_dividend_news_risk,
    build_candidate_frame,
    classify_candidate,
)


def row(**overrides):
    base = {
        "close": 1000.0,
        "forecast_dividend_per_share": 40.0,
        "dividend_per_share": None,
        "forecast_eps": 100.0,
        "eps": None,
        "profit": 1_000_000.0,
        "equity": 5_000_000.0,
        "total_assets": 10_000_000.0,
        "avg_volume_20": 50_000.0,
        "ma75": 1000.0,
        "ma200": 1000.0,
    }
    base.update(overrides)
    return pd.Series(base)


class DividendScanClassificationTest(unittest.TestCase):
    def test_candidate_frame_excludes_share_price_above_10000(self):
        dates = pd.date_range("2025-01-01", periods=200, freq="B")
        prices = pd.DataFrame([
            {"date": date, "code": code, "close": close, "volume": 50_000}
            for code, close in [("11110", 10_000.0), ("22220", 10_001.0)]
            for date in dates
        ])

        result = build_candidate_frame(prices, pd.DataFrame())

        self.assertEqual(result["code"].tolist(), ["11110"])

    def test_classifies_quality_candidate_as_buy_zone(self):
        result = classify_candidate(row())

        self.assertEqual(result["verdict"], "BUY_ZONE")
        self.assertEqual(result["dividend_yield"], 4.0)
        self.assertEqual(result["payout_ratio"], 40.0)
        self.assertEqual(result["equity_ratio"], 50.0)

    def test_high_payout_candidate_is_watch(self):
        result = classify_candidate(row(forecast_dividend_per_share=80.0))

        self.assertEqual(result["verdict"], "WATCH")
        self.assertIn("payout high", result["reason"])

    def test_extreme_payout_candidate_is_avoid(self):
        result = classify_candidate(row(forecast_dividend_per_share=120.0))

        self.assertEqual(result["verdict"], "AVOID")
        self.assertIn("payout>100%", result["reason"])

    def test_missing_financials_are_data_missing(self):
        result = classify_candidate(row(forecast_dividend_per_share=None, dividend_per_share=None))

        self.assertEqual(result["verdict"], "DATA_MISSING")
        self.assertIn("missing dividend", result["reason"])

    def test_unverified_share_basis_is_data_warning(self):
        result = classify_candidate(row(
            share_basis_status="UNVERIFIED",
            share_basis_reason="price discontinuity without adjustment factor: 2026-03-23",
        ))

        self.assertEqual(result["verdict"], "DATA_WARNING")
        self.assertIsNone(result["dividend_yield"])
        self.assertIn("2026-03-23", result["reason"])

    def test_explicit_split_factor_normalizes_per_share_values(self):
        result = classify_candidate(row(
            close=1821.0,
            forecast_dividend_per_share=280.0,
            forecast_eps=340.47,
            share_basis_status="VERIFIED",
            share_basis_factor=0.25,
        ))

        self.assertAlmostEqual(result["dividend_yield"], 3.84, places=2)
        self.assertAlmostEqual(result["payout_ratio"], 82.2, places=1)

    def test_known_20030_gap_without_factor_is_not_guessed_over(self):
        prices = pd.DataFrame([
            {"date": "2026-03-19", "code": "20030", "close": 7160.0, "adjustmentfactor": None},
            {"date": "2026-03-23", "code": "20030", "close": 1633.39, "adjustmentfactor": None},
            {"date": "2026-08-21", "code": "20030", "close": 1821.0, "adjustmentfactor": None},
        ])
        candidates = pd.DataFrame([{
            "code": "20030",
            "disclosure_date": "2026-02-02",
        }])

        annotated = annotate_share_basis(
            prices,
            candidates,
            pd.Timestamp("2026-08-21"),
        )

        self.assertEqual(annotated.iloc[0]["share_basis_status"], "UNVERIFIED")
        self.assertEqual(annotated.iloc[0]["share_basis_factor"], 1.0)
        self.assertIn("2026-03-23", annotated.iloc[0]["share_basis_reason"])

    def test_known_20030_gap_with_explicit_factor_is_normalized(self):
        prices = pd.DataFrame([
            {"date": "2026-03-19", "code": "20030", "close": 7160.0, "adjustmentfactor": None},
            {"date": "2026-03-23", "code": "20030", "close": 1633.39, "adjustmentfactor": 0.25},
            {"date": "2026-08-21", "code": "20030", "close": 1821.0, "adjustmentfactor": None},
        ])
        candidates = pd.DataFrame([{
            "code": "20030",
            "disclosure_date": "2026-02-02",
        }])

        annotated = annotate_share_basis(
            prices,
            candidates,
            pd.Timestamp("2026-08-21"),
        )

        self.assertEqual(annotated.iloc[0]["share_basis_status"], "VERIFIED")
        self.assertEqual(annotated.iloc[0]["share_basis_factor"], 0.25)

    def test_known_19610_gap_without_factor_is_not_guessed_over(self):
        prices = pd.DataFrame([
            {"date": "2026-04-20", "code": "19610", "close": 7060.0, "adjustmentfactor": None},
            {"date": "2026-04-21", "code": "19610", "close": 2380.0, "adjustmentfactor": None},
        ])
        candidates = pd.DataFrame([{
            "code": "19610",
            "disclosure_date": "2026-02-13",
        }])

        annotated = annotate_share_basis(
            prices,
            candidates,
            pd.Timestamp("2026-08-21"),
        )

        self.assertEqual(annotated.iloc[0]["share_basis_status"], "UNVERIFIED")
        self.assertIn("2026-04-21", annotated.iloc[0]["share_basis_reason"])

    def test_price_above_ma75_is_watch(self):
        result = classify_candidate(row(close=1100.0, ma75=1000.0))

        self.assertEqual(result["verdict"], "WATCH")
        self.assertIn("price above MA75", result["reason"])

    def test_news_risk_demotes_buy_zone_to_watch(self):
        import src.news_analyzer as news_analyzer

        original = news_analyzer.batch_analyze_dividend_risk
        try:
            news_analyzer.batch_analyze_dividend_risk = lambda candidates: [
                {
                    "code": "12340",
                    "risk": "HIGH",
                    "reason": "DividendRisk:減配",
                    "news_hit": "減配ニュース",
                }
            ]
            df = pd.DataFrame([{
                "code": "12340",
                "name": "テスト会社",
                "verdict": "BUY_ZONE",
                "score": 10.0,
                "dividend_yield": 4.0,
                "reason": "quality dividend candidate",
            }])

            result = apply_dividend_news_risk(df)

            self.assertEqual(result.iloc[0]["verdict"], "WATCH")
            self.assertEqual(result.iloc[0]["news_risk"], "HIGH")
            self.assertIn("DividendRisk:減配", result.iloc[0]["reason"])
        finally:
            news_analyzer.batch_analyze_dividend_risk = original


if __name__ == "__main__":
    unittest.main()
