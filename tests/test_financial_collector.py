# -*- coding: utf-8 -*-
import sys
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.financial_collector import (
    collect_financial_summary,
    collect_for_codes,
    load_codes_from_db,
    normalize_financial_row,
)


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

    def test_does_not_treat_total_annual_amount_as_per_share(self):
        row = normalize_financial_row({
            "Code": "20030",
            "DiscDate": "2025-05-07",
            "DivTotalAnn": "2554000000",
            "FDivTotalAnn": "2800000000",
        })

        self.assertIsNone(row["dividend_per_share"])
        self.assertIsNone(row["forecast_dividend_per_share"])


class FinancialCollectorSchedulingTest(unittest.TestCase):
    def test_stale_selection_prioritizes_missing_then_oldest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "financials.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("CREATE TABLE fundamentals (code TEXT)")
                connection.execute("""
                    CREATE TABLE dividend_financials (
                        code TEXT NOT NULL,
                        disclosure_date TEXT NOT NULL,
                        fiscal_year TEXT,
                        period TEXT,
                        dividend_per_share REAL,
                        forecast_dividend_per_share REAL,
                        eps REAL,
                        forecast_eps REAL,
                        profit REAL,
                        equity REAL,
                        total_assets REAL,
                        raw_json TEXT,
                        updated_at TEXT,
                        PRIMARY KEY (code, disclosure_date, period)
                    )
                """)
                connection.execute("""
                    CREATE TABLE sync_progress (
                        table_name TEXT PRIMARY KEY,
                        last_synced_date TEXT
                    )
                """)
                connection.executemany(
                    "INSERT INTO fundamentals VALUES (?)",
                    [("10010",), ("10020",), ("10030",), ("10040",)],
                )
                connection.executemany(
                    """INSERT INTO dividend_financials
                       (code, disclosure_date, period, updated_at)
                       VALUES (?, ?, ?, ?)""",
                    [
                        ("10020", "2026-01-01", "FY", "2026-08-01 00:00:00"),
                        ("10030", "2026-01-01", "FY", "2026-08-20 00:00:00"),
                        ("10040", "2026-01-01", "FY", ""),
                    ],
                )
                connection.execute(
                    "INSERT INTO sync_progress VALUES (?, ?)",
                    ("dividend_financials:10040", "2026-08-20 00:00:00"),
                )
                connection.commit()

            codes = load_codes_from_db(
                str(db_path),
                limit=3,
                stale_before="2026-08-15 00:00:00",
            )

        self.assertEqual(codes, ["10010", "10020"])

    def test_missing_only_remains_available_for_manual_use(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "financials.db"
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute("CREATE TABLE fundamentals (code TEXT)")
                connection.executemany(
                    "INSERT INTO fundamentals VALUES (?)",
                    [("10010",), ("10020",)],
                )
                connection.commit()

            codes = load_codes_from_db(
                str(db_path), limit=10, missing_only=True
            )

        self.assertEqual(codes, ["10010", "10020"])

    def test_rejects_conflicting_refresh_modes(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            load_codes_from_db(
                "unused.db",
                missing_only=True,
                stale_before="2026-08-15 00:00:00",
            )

    def test_successful_empty_response_advances_sync_progress(self):
        class EmptyClient:
            def get_financial_summary(self, code=None, date=None):
                return {"data": []}

        class RecordingDatabase:
            def __init__(self):
                self.progress = []

            def save_dividend_financials(self, rows):
                self.saved_rows = rows
                return len(rows)

            def update_sync_progress(self, table_name, synced_date):
                self.progress.append((table_name, synced_date))

        db = RecordingDatabase()

        saved = collect_for_codes(
            EmptyClient(), db, ["13050"], sleep_seconds=0
        )

        self.assertEqual(saved, 0)
        self.assertEqual(len(db.progress), 1)
        self.assertEqual(db.progress[0][0], "dividend_financials:13050")

    def test_partial_database_save_is_failure(self):
        class OneRowClient:
            def get_financial_summary(self, code=None, date=None):
                return {"data": [{
                    "Code": code,
                    "DiscDate": "2026-08-01",
                    "CurPerType": "1Q",
                }]}

        class FailedDatabase:
            def save_dividend_financials(self, rows):
                return 0

        with self.assertRaisesRegex(RuntimeError, "Saved 0 of 1"):
            collect_financial_summary(
                OneRowClient(), FailedDatabase(), code="10010"
            )

    def test_code_failure_returns_batch_failure(self):
        class FailedClient:
            def get_financial_summary(self, code=None, date=None):
                raise RuntimeError("API unavailable")

        class RecordingDatabase:
            def update_sync_progress(self, table_name, synced_date):
                raise AssertionError("failed fetch must not advance progress")

        with self.assertRaisesRegex(RuntimeError, "failed for 1 code"):
            collect_for_codes(
                FailedClient(), RecordingDatabase(), ["10010"], sleep_seconds=0
            )


if __name__ == "__main__":
    unittest.main()
