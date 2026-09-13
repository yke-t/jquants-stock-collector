from __future__ import annotations

import importlib.util
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "audit_data_coverage.py"
SPEC = importlib.util.spec_from_file_location("audit_data_coverage", MODULE_PATH)
coverage = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(coverage)


class DataCoverageAuditTests(unittest.TestCase):
    def create_database(
        self,
        path: Path,
        *,
        master_date: str = "2026-06-19",
        covered_target_codes: tuple[str, ...] = ("10000", "20000", "30000"),
        record_missing_attempt: bool = True,
    ) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(
                """
                CREATE TABLE fundamentals (
                    date TEXT,
                    code TEXT,
                    coname TEXT,
                    scalecat TEXT
                );
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    PRIMARY KEY (date, code)
                );
                CREATE INDEX idx_prices_code ON prices(code);
                CREATE TABLE dividend_financials (
                    code TEXT,
                    disclosure_date TEXT,
                    forecast_dividend_per_share REAL,
                    updated_at TEXT
                );
                CREATE TABLE sync_progress (
                    table_name TEXT PRIMARY KEY,
                    last_synced_date TEXT
                );
                """
            )
            fundamentals = [
                (master_date, "10000", "A", "TOPIX Small 1"),
                (master_date, "20000", "B", "TOPIX Small 2"),
                (master_date, "30000", "C", "TOPIX Mid400"),
                (master_date, "40000", "D", "-"),
            ]
            connection.executemany(
                "INSERT INTO fundamentals VALUES (?, ?, ?, ?)", fundamentals
            )
            connection.executemany(
                "INSERT INTO prices VALUES ('2026-09-10', ?, 100)",
                [(code,) for code in covered_target_codes],
            )
            connection.executemany(
                "INSERT INTO dividend_financials VALUES (?, ?, ?, ?)",
                [
                    ("10000", "2026-06-19", 10.0, "2026-09-11 18:00:00"),
                    ("20000", "2026-06-19", None, "2026-09-11 18:00:00"),
                    ("30000", "2026-06-19", 20.0, "2026-09-11 18:00:00"),
                ],
            )
            attempts = [
                (f"dividend_financials:{code}", "2026-09-11 18:00:00")
                for code in ("10000", "20000", "30000")
            ]
            if record_missing_attempt:
                attempts.append(
                    ("dividend_financials:40000", "2026-09-11 18:00:00")
                )
            connection.executemany(
                "INSERT INTO sync_progress VALUES (?, ?)", attempts
            )
            connection.commit()

    def test_healthy_delayed_free_plan_data_passes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            audit = coverage.build_audit(database, date(2026, 9, 14))

        self.assertEqual(audit["overall_status"], "pass")
        self.assertEqual(audit["master"]["age_days"], 87)
        self.assertEqual(audit["daily_prices"]["coverage_pct"], 100.0)
        self.assertEqual(
            audit["dividend_financials"]["disclosure_lag_days"], 87
        )
        self.assertEqual(
            audit["dividend_financials"]["unattempted_missing_codes"], 0
        )

    def test_stale_master_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database, master_date="2025-11-19")
            audit = coverage.build_audit(database, date(2026, 9, 14))

        self.assertEqual(audit["overall_status"], "fail")
        self.assertEqual(audit["master"]["status"], "fail")
        self.assertIn("snapshot age", audit["master"]["issues"][0])

    def test_low_target_price_coverage_fails_and_lists_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database, covered_target_codes=("10000", "20000"))
            audit = coverage.build_audit(database, date(2026, 9, 14))

        prices = audit["daily_prices"]
        self.assertEqual(prices["status"], "fail")
        self.assertEqual(prices["covered_codes"], 2)
        self.assertEqual(prices["missing_code_count"], 1)
        self.assertEqual(prices["missing_codes"][0]["code"], "30000")

    def test_newer_partial_date_does_not_replace_representative_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            with closing(sqlite3.connect(database)) as connection:
                connection.execute(
                    "INSERT INTO prices VALUES ('2026-09-11', '10000', 100)"
                )
                connection.commit()
            audit = coverage.build_audit(database, date(2026, 9, 14))

        prices = audit["daily_prices"]
        self.assertEqual(prices["status"], "pass")
        self.assertEqual(prices["representative_date"], "2026-09-10")
        self.assertEqual(prices["observed_latest_date"], "2026-09-11")
        self.assertEqual(prices["newer_partial_date_count"], 1)
        self.assertEqual(prices["coverage_pct"], 100.0)

    def test_unattempted_missing_financial_code_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database, record_missing_attempt=False)
            audit = coverage.build_audit(database, date(2026, 9, 14))

        dividend = audit["dividend_financials"]
        self.assertEqual(dividend["status"], "fail")
        self.assertEqual(dividend["unattempted_missing_codes"], 1)

    def test_audit_does_not_modify_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            before = (database.stat().st_size, database.stat().st_mtime_ns)
            coverage.build_audit(database, date(2026, 9, 14))
            after = (database.stat().st_size, database.stat().st_mtime_ns)

        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
