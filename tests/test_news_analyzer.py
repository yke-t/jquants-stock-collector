# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.news_analyzer import analyze_dividend_risk, is_company_relevant_hit, mask_api_keys


class NewsAnalyzerMaskTest(unittest.TestCase):
    def test_masks_google_cse_key_and_cx_in_url(self):
        message = (
            "HTTPSConnectionPool(host='www.googleapis.com'): "
            "https://www.googleapis.com/customsearch/v1?"
            "key=secret-api-key&cx=secret-cx-id&q=Toyota&num=3"
        )

        masked = mask_api_keys(message)

        self.assertNotIn("secret-api-key", masked)
        self.assertNotIn("secret-cx-id", masked)
        self.assertIn("key=***", masked)
        self.assertIn("cx=***", masked)
        self.assertIn("q=Toyota", masked)

    def test_masks_sensitive_params_case_insensitively(self):
        message = "https://example.test/search?KEY=abc123&CX=def456&q=x"

        masked = mask_api_keys(message)

        self.assertNotIn("abc123", masked)
        self.assertNotIn("def456", masked)
        self.assertIn("KEY=***", masked)
        self.assertIn("CX=***", masked)

    def test_leaves_non_sensitive_text_unchanged(self):
        message = "timeout while searching Toyota"

        self.assertEqual(mask_api_keys(message), message)


class DividendRiskAnalyzerTest(unittest.TestCase):
    def test_company_relevance_accepts_target_company(self):
        self.assertTrue(is_company_relevant_hit(
            "パーソルホールディングス",
            "パーソルHDが業績予想を下方修正",
            "",
        ))

    def test_company_relevance_rejects_unrelated_keyword_page(self):
        self.assertFalse(is_company_relevant_hit(
            "パーソルホールディングス",
            "JAL、赤字は回避 欧州線が好調",
            "",
        ))

    def test_high_risk_when_keyword_news_hits(self):
        import src.news_analyzer as news_analyzer

        original = news_analyzer.search_news_with_keywords
        try:
            news_analyzer.search_news_with_keywords = lambda name, keywords, max_results=3: [
                {"title": "テスト会社 減配を発表", "keyword": "減配", "link": "https://example.test"}
            ]

            result = analyze_dividend_risk("12340", "テスト会社")

            self.assertEqual(result["risk"], "HIGH")
            self.assertIn("DividendRisk:減配", result["reason"])
            self.assertIn("減配", result["news_hit"])
        finally:
            news_analyzer.search_news_with_keywords = original

    def test_low_risk_when_no_news_hits(self):
        import src.news_analyzer as news_analyzer

        original = news_analyzer.search_news_with_keywords
        try:
            news_analyzer.search_news_with_keywords = lambda name, keywords, max_results=3: []

            result = analyze_dividend_risk("12340", "テスト会社")

            self.assertEqual(result["risk"], "LOW")
            self.assertEqual(result["news_hit"], "")
        finally:
            news_analyzer.search_news_with_keywords = original


if __name__ == '__main__':
    unittest.main()
