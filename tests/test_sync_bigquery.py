# -*- coding: utf-8 -*-
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from src import sync_bigquery


class SyncBigQueryExitCodeTest(unittest.TestCase):
    def test_daily_sync_returns_failure_when_database_is_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing = Path(temp_dir) / "missing.db"
            with patch.object(sync_bigquery, "DB_PATH", missing):
                self.assertEqual(sync_bigquery.run_daily_sync(), 1)

    def test_daily_sync_returns_failure_on_partial_sync(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "prices.db"
            db_path.touch()
            recent = pd.DataFrame([{
                "date": "2026-08-21",
                "code": "20030",
                "close": 1821.0,
            }])
            with (
                patch.object(sync_bigquery, "DB_PATH", db_path),
                patch.object(sync_bigquery, "get_recent_data", return_value=recent),
                patch.object(sync_bigquery, "sync_to_bigquery", return_value=0),
            ):
                self.assertEqual(sync_bigquery.run_daily_sync(), 1)


if __name__ == "__main__":
    unittest.main()
