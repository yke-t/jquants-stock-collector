# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dividend_scan import apply_dividend_news_risk, build_candidate_frame, classify_candidate


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
