from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "refresh_listed_info.py"
SPEC = importlib.util.spec_from_file_location("refresh_listed_info", MODULE_PATH)
refresh = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(refresh)


class RefreshListedInfoTests(unittest.TestCase):
    def response(self) -> dict:
        return {
            "data": [
                {
                    "Date": "2026-06-22",
                    "Code": "10000",
                    "CoName": "A",
                    "ScaleCat": "TOPIX Small 1",
                    "Mkt": "0111",
                },
                {
                    "Date": "2026-06-22",
                    "Code": "30000",
                    "CoName": "C",
                    "ScaleCat": "TOPIX Mid400",
                    "Mkt": "0111",
                },
                {
                    "Date": "2026-06-22",
                    "Code": "40000",
                    "CoName": "D",
                    "ScaleCat": "TOPIX Small 2",
                    "Mkt": "0111",
                },
            ]
        }

    def create_database(self, path: Path) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(
                """
                CREATE TABLE fundamentals (
                    date TEXT,
                    code TEXT,
                    coname TEXT,
                    scalecat TEXT
                );
                INSERT INTO fundamentals VALUES
                    ('2025-11-19', '10000', 'A', 'TOPIX Small 2'),
                    ('2025-11-19', '20000', 'B', 'TOPIX Small 2');
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    PRIMARY KEY (date, code)
                );
                INSERT INTO prices VALUES
                    ('2026-09-10', '10000', 100),
                    ('2026-09-10', '30000', 100),
                    ('2026-09-10', '40000', 100);
                CREATE TABLE sentinel (value TEXT);
                INSERT INTO sentinel VALUES ('preserved');
                """
            )
            connection.commit()

    def create_backup_evidence(self, database: Path, path: Path) -> None:
        payload = {
            "created_at": "2099-01-01T00:00:00+09:00",
            "backup_verified": True,
            "source_database_modified": False,
            "source": {
                "path": str(database.resolve()),
                "size_bytes": database.stat().st_size,
                "quick_check": "ok",
            },
            "backup": {"path": str(path.with_suffix(".db"))},
            "restore_drill": {"verified": True},
        }
        path.write_text(json.dumps(payload), encoding="utf-8")

    def test_normalize_and_validate_response(self) -> None:
        columns, rows = refresh.normalize_rows(refresh.response_rows(self.response()))
        summary = refresh.validate_rows(
            rows,
            date(2026, 9, 14),
            minimum_rows=3,
            minimum_target_rows=3,
        )

        self.assertIn("code", columns)
        self.assertEqual(summary["rows"], 3)
        self.assertEqual(summary["unique_codes"], 3)
        self.assertEqual(summary["age_days"], 84)

    def test_duplicate_code_is_rejected(self) -> None:
        response = self.response()
        response["data"].append(dict(response["data"][0]))
        _, rows = refresh.normalize_rows(refresh.response_rows(response))

        with self.assertRaisesRegex(ValueError, "duplicate code"):
            refresh.validate_rows(
                rows,
                date(2026, 9, 14),
                minimum_rows=3,
                minimum_target_rows=3,
            )

    def test_stale_response_is_rejected(self) -> None:
        response = self.response()
        for row in response["data"]:
            row["Date"] = "2025-01-01"
        _, rows = refresh.normalize_rows(refresh.response_rows(response))

        with self.assertRaisesRegex(ValueError, "master age"):
            refresh.validate_rows(
                rows,
                date(2026, 9, 14),
                minimum_rows=3,
                minimum_target_rows=3,
            )

    def test_dry_run_reports_diff_without_modifying_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            before = (database.stat().st_size, database.stat().st_mtime_ns)
            result = refresh.refresh_master(
                database,
                self.response(),
                apply=False,
                backup_verification=None,
                as_of=date(2026, 9, 14),
                minimum_rows=3,
                minimum_target_rows=3,
            )
            after = (database.stat().st_size, database.stat().st_mtime_ns)

        self.assertFalse(result["applied"])
        self.assertEqual(result["comparison"]["added_codes"], ["30000", "40000"])
        self.assertEqual(result["comparison"]["removed_codes"], ["20000"])
        self.assertEqual(result["comparison"]["category_change_count"], 1)
        self.assertEqual(
            result["projected_daily_price_coverage"]["coverage_pct"], 100.0
        )
        self.assertEqual(after, before)

    def test_apply_requires_matching_verified_backup_and_preserves_other_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            database = directory / "data.db"
            evidence = directory / "backup.verification.json"
            self.create_database(database)
            self.create_backup_evidence(database, evidence)

            result = refresh.refresh_master(
                database,
                self.response(),
                apply=True,
                backup_verification=evidence,
                as_of=date(2026, 9, 14),
                minimum_rows=3,
                minimum_target_rows=3,
            )
            with closing(sqlite3.connect(database)) as connection:
                codes = [
                    row[0]
                    for row in connection.execute(
                        "SELECT code FROM fundamentals ORDER BY code"
                    )
                ]
                sentinel = connection.execute("SELECT value FROM sentinel").fetchone()[0]

        self.assertTrue(result["applied"])
        self.assertEqual(result["database_verification"]["quick_check"], "ok")
        self.assertEqual(codes, ["10000", "30000", "40000"])
        self.assertEqual(sentinel, "preserved")

    def test_apply_without_backup_is_rejected_before_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)

            with self.assertRaisesRegex(ValueError, "backup-verification"):
                refresh.refresh_master(
                    database,
                    self.response(),
                    apply=True,
                    backup_verification=None,
                    as_of=date(2026, 9, 14),
                    minimum_rows=3,
                    minimum_target_rows=3,
                )
            with closing(sqlite3.connect(database)) as connection:
                count = connection.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]

        self.assertEqual(count, 2)

    def test_apply_rejects_low_projected_price_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            with closing(sqlite3.connect(database)) as connection:
                connection.execute("DELETE FROM prices WHERE code != '10000'")
                connection.commit()

            with self.assertRaisesRegex(ValueError, "Projected target price coverage"):
                refresh.refresh_master(
                    database,
                    self.response(),
                    apply=True,
                    backup_verification=None,
                    as_of=date(2026, 9, 14),
                    minimum_rows=3,
                    minimum_target_rows=3,
                )

    def test_projection_uses_latest_representative_price_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            with closing(sqlite3.connect(database)) as connection:
                connection.execute(
                    "INSERT INTO prices VALUES ('2026-09-11', '10000', 101)"
                )
                connection.commit()
                _, incoming_rows = refresh.normalize_rows(
                    refresh.response_rows(self.response())
                )
                projection = refresh.projected_price_coverage(
                    connection,
                    incoming_rows,
                    minimum_coverage=0.95,
                )

        self.assertEqual(projection["representative_price_date"], "2026-09-10")
        self.assertEqual(projection["observed_latest_price_date"], "2026-09-11")
        self.assertEqual(projection["newer_partial_date_count"], 1)
        self.assertTrue(projection["meets_minimum"])


if __name__ == "__main__":
    unittest.main()
