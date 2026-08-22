# -*- coding: utf-8 -*-
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.collector import DataCollector


class DataCollectorFailureTest(unittest.TestCase):
    def test_partial_daily_save_fails_before_progress_update(self):
        class Client:
            def get_listed_info(self):
                return {"data": []}

            def get_daily_quotes(self, date=None):
                return {"data": [{
                    "Date": date,
                    "Code": "20030",
                    "O": 1800.0,
                    "H": 1830.0,
                    "L": 1790.0,
                    "C": 1821.0,
                    "Vo": 100000,
                }]}

        class PartialDatabase:
            def get_sync_progress(self, table_name):
                return None

            def save_daily_quotes(self, frame):
                return 0

            def update_sync_progress(self, table_name, date):
                raise AssertionError("partial save must not advance progress")

        collector = DataCollector(Client(), PartialDatabase())

        with self.assertRaisesRegex(RuntimeError, "Saved 0 of 1"):
            collector.run(
                start_date="2026-08-21",
                end_date="2026-08-21",
                resume=False,
            )


if __name__ == "__main__":
    unittest.main()
