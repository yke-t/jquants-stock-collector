# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.financial_collector import normalize_financial_row


class FinancialCollectorNormalizeTest(unittest.TestCase):
    def test_normalizes_jquants_v2_short_fields(self):
        source = {
            "Code": "72030",
            "DiscDate": "2026-02-06",
            "CurFYEn": "2026-03-31",
            "CurPerType": "3Q",
            "FDivAnn": "95.0",
            "FEPS": "273.91",
            "NP": "3030891000000",
            "Eq": "39992539000000",
            "TA": "102344599000000",
        }

        row = normalize_financial_row(source)

        self.assertEqual(row["code"], "72030")
        self.assertEqual(row["disclosure_date"], "2026-02-06")
        self.assertEqual(row["period"], "3Q")
        self.assertEqual(row["forecast_dividend_per_share"], 95.0)
        self.assertEqual(row["forecast_eps"], 273.91)
        self.assertEqual(row["profit"], 3030891000000.0)
        self.assertEqual(row["equity"], 39992539000000.0)
        self.assertEqual(row["total_assets"], 102344599000000.0)

    def test_requires_code_and_disclosure_date(self):
        self.assertIsNone(normalize_financial_row({"Code": "72030"}))
        self.assertIsNone(normalize_financial_row({"DiscDate": "2026-02-06"}))


if __name__ == "__main__":
    unittest.main()
