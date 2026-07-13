# -*- coding: utf-8 -*-
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.export_drive_spreadsheet import latest_csv_by_prefix, read_csv_rows


class ExportDriveSpreadsheetTest(unittest.TestCase):
    def test_read_csv_rows_handles_utf8_sig(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.csv"
            path.write_text("\ufeffcode,name\n19640,中外炉工業\n", encoding="utf-8")

            rows = read_csv_rows(path)

            self.assertEqual(rows, [["code", "name"], ["19640", "中外炉工業"]])

    def test_latest_csv_by_prefix_uses_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old_path = root / "dividend_candidates_2026-06-22.csv"
            new_path = root / "dividend_candidates_2026-06-23.csv"
            old_path.write_text("old\n", encoding="utf-8")
            time.sleep(0.01)
            new_path.write_text("new\n", encoding="utf-8")

            self.assertEqual(latest_csv_by_prefix(root, "dividend_candidates"), new_path)


if __name__ == "__main__":
    unittest.main()
