# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.news_analyzer import mask_api_keys


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


if __name__ == '__main__':
    unittest.main()
