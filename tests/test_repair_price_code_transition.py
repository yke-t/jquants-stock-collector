from __future__ import annotations

import importlib.util
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "repair_price_code_transition.py"
SPEC = importlib.util.spec_from_file_location("repair_price_code_transition", MODULE_PATH)
repair = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(repair)


class RepairPriceCodeTransitionTests(unittest.TestCase):
    def create_database(self, path: Path, *, conflict: bool = False) -> None:
        with closing(sqlite3.connect(path)) as connection:
            connection.executescript(
                """
                CREATE TABLE prices (
                    date TEXT,
                    code TEXT,
                    close REAL,
                    volume REAL,
                    PRIMARY KEY (date, code)
                );
                CREATE TABLE sentinel (value TEXT);
                INSERT INTO sentinel VALUES ('preserved');
                INSERT INTO prices VALUES ('2026-06-29', '44490', 995, 0);
                INSERT INTO prices VALUES ('2026-06-30', '44490', 995, 0);
                INSERT INTO prices VALUES ('2026-07-01', '44490', 1047, 106700);
                INSERT INTO prices VALUES ('2026-06-01', '590A0', 1, 1);
                """
            )
            if conflict:
                connection.execute(
                    "INSERT INTO prices VALUES ('2026-07-01', '590A0', 1047, 106700)"
                )
            connection.commit()

    def create_backup_evidence(self, database: Path, result: Path) -> None:
        created_at = datetime.now(timezone.utc) + timedelta(seconds=2)
        payload = {
            "created_at": created_at.isoformat(),
            "backup_verified": True,
            "source_database_modified": False,
            "source": {
                "path": str(database.resolve()),
                "size_bytes": database.stat().st_size,
                "quick_check": "ok",
            },
            "backup": {"path": str(database.with_suffix(".backup.db"))},
            "restore_drill": {"verified": True},
        }
        result.write_text(json.dumps(payload), encoding="utf-8")

    def test_dry_run_reports_rows_without_modifying_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            before = (database.stat().st_size, database.stat().st_mtime_ns)
            result = repair.repair_transition(
                database,
                "44490",
                "590A0",
                datetime(2026, 6, 29).date(),
                apply=False,
                backup_verification=None,
                expected_source_rows=None,
                expected_source_sha256=None,
            )
            after = (database.stat().st_size, database.stat().st_mtime_ns)

        self.assertFalse(result["applied"])
        self.assertEqual(result["inspection"]["source_rows"], 2)
        self.assertEqual(result["inspection"]["conflict_count"], 0)
        self.assertEqual(after, before)

    def test_apply_moves_only_rows_after_cutoff_and_preserves_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            database = directory / "data.db"
            evidence = directory / "backup.verification.json"
            self.create_database(database)
            self.create_backup_evidence(database, evidence)
            with closing(repair.connect_read_only(database)) as connection:
                inspection = repair.inspect_transition(
                    connection, "44490", "590A0", datetime(2026, 6, 29).date()
                )
            result = repair.repair_transition(
                database,
                "44490",
                "590A0",
                datetime(2026, 6, 29).date(),
                apply=True,
                backup_verification=evidence,
                expected_source_rows=inspection["source_rows"],
                expected_source_sha256=inspection["source_rows_sha256"],
            )
            with closing(sqlite3.connect(database)) as connection:
                source_dates = connection.execute(
                    "SELECT date FROM prices WHERE code = '44490' ORDER BY date"
                ).fetchall()
                target_dates = connection.execute(
                    "SELECT date FROM prices WHERE code = '590A0' ORDER BY date"
                ).fetchall()
                sentinel = connection.execute("SELECT value FROM sentinel").fetchone()[0]

        self.assertTrue(result["applied"])
        self.assertEqual(result["database_verification"]["moved_rows"], 2)
        self.assertEqual(source_dates, [("2026-06-29",)])
        self.assertEqual(
            target_dates,
            [("2026-06-01",), ("2026-06-30",), ("2026-07-01",)],
        )
        self.assertEqual(sentinel, "preserved")

    def test_apply_rejects_conflicting_target_dates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database, conflict=True)
            with closing(repair.connect_read_only(database)) as connection:
                inspection = repair.inspect_transition(
                    connection, "44490", "590A0", datetime(2026, 6, 29).date()
                )
            with self.assertRaisesRegex(ValueError, "already has rows"):
                repair.repair_transition(
                    database,
                    "44490",
                    "590A0",
                    datetime(2026, 6, 29).date(),
                    apply=True,
                    backup_verification=None,
                    expected_source_rows=inspection["source_rows"],
                    expected_source_sha256=inspection["source_rows_sha256"],
                )

    def test_apply_requires_approved_dry_run_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            database = Path(temp_dir) / "data.db"
            self.create_database(database)
            with self.assertRaisesRegex(ValueError, "required with --apply"):
                repair.repair_transition(
                    database,
                    "44490",
                    "590A0",
                    datetime(2026, 6, 29).date(),
                    apply=True,
                    backup_verification=None,
                    expected_source_rows=None,
                    expected_source_sha256=None,
                )


if __name__ == "__main__":
    unittest.main()
