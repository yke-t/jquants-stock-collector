import unittest

import pandas as pd

from src.price_basis import normalize_price_history


class PriceBasisTest(unittest.TestCase):
    def test_explicit_split_factor_keeps_ohlc_continuous(self):
        prices = pd.DataFrame(
            [
                {
                    "date": "2026-01-05",
                    "code": "10000",
                    "open": 100,
                    "high": 104,
                    "low": 98,
                    "close": 102,
                    "adjustmentfactor": None,
                },
                {
                    "date": "2026-01-06",
                    "code": "10000",
                    "open": 51,
                    "high": 53,
                    "low": 50,
                    "close": 51,
                    "adjustmentfactor": 0.5,
                },
            ]
        )

        result = normalize_price_history(prices)

        self.assertEqual(result["basis_open"].tolist(), [50.0, 51.0])
        self.assertEqual(result["basis_close"].tolist(), [51.0, 51.0])
        self.assertFalse(result["unverified_gap"].any())

    def test_large_gap_without_factor_is_not_inferred(self):
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
            ]
        )

        result = normalize_price_history(prices)

        self.assertTrue(result.iloc[-1]["unverified_gap"])
        self.assertEqual(result.iloc[-1]["basis_scale"], 1.0)

    def test_invalid_factor_and_invalid_identifiers_are_rejected_or_marked(self):
        prices = pd.DataFrame(
            [
                {
                    "date": "2026-01-05",
                    "code": "10000",
                    "open": 100,
                    "high": 101,
                    "low": 99,
                    "close": 100,
                    "adjustmentfactor": 0,
                }
            ]
        )
        result = normalize_price_history(prices)
        self.assertTrue(result.iloc[0]["invalid_adjustment_factor"])
        self.assertTrue(result.iloc[0]["unverified_gap"])

        invalid_code = prices.copy()
        invalid_code.loc[0, "code"] = None
        with self.assertRaises(ValueError):
            normalize_price_history(invalid_code)

    def test_duplicate_date_code_is_rejected(self):
        row = {
            "date": "2026-01-05",
            "code": "10000",
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "adjustmentfactor": None,
        }
        with self.assertRaises(ValueError):
            normalize_price_history(pd.DataFrame([row, row]))


if __name__ == "__main__":
    unittest.main()
