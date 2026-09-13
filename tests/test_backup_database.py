# -*- coding: utf-8 -*-
import importlib.util
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "backup_database.py"
SPEC = importlib.util.spec_from_file_location("backup_database", MODULE_PATH)
backup_database = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(backup_database)


class DatabaseBackupTest(unittest.TestCase):
    def create_source(self, directory: str) -> Path:
        source = Path(directory) / "source.db"
        with closing(sqlite3.connect(source)) as connection:
            connection.execute(
                "CREATE TABLE prices (date TEXT, code TEXT, close REAL)"
            )
            connection.executemany(
                "INSERT INTO prices VALUES (?, ?, ?)",
                [
                    ("2026-09-10", "20030", 1000.0),
                    ("2026-09-10", "19610", 2000.0),
                ],
            )
            connection.execute("CREATE TABLE signals (code TEXT, verdict TEXT)")
            connection.execute("INSERT INTO signals VALUES ('20030', 'WATCH')")
            connection.commit()
        return source

    def test_backup_and_restore_drill_preserve_schema_and_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = self.create_source(temp_dir)
            backup = Path(temp_dir) / "backups" / "source.backup.db"
            source_bytes = source.read_bytes()

            result = backup_database.create_verified_backup(
                source,
                backup,
                restore_drill=True,
            )

            self.assertTrue(backup.exists())
            self.assertTrue(result["backup_verified"])
            self.assertTrue(result["restore_drill"]["verified"])
            self.assertTrue(
                result["restore_drill"]["temporary_database_removed"]
            )
            self.assertEqual(
                result["source"]["schema_sha256"],
                result["backup"]["schema_sha256"],
            )
            self.assertEqual(
                result["source"]["table_row_counts"],
                {"prices": 2, "signals": 1},
            )
            self.assertEqual(source.read_bytes(), source_bytes)

    def test_existing_backup_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = self.create_source(temp_dir)
            backup = Path(temp_dir) / "existing.db"
            backup.write_bytes(b"keep-me")

            with self.assertRaisesRegex(FileExistsError, "already exists"):
                backup_database.create_verified_backup(
                    source,
                    backup,
                    restore_drill=False,
                )

            self.assertEqual(backup.read_bytes(), b"keep-me")

    def test_source_cannot_be_its_own_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = self.create_source(temp_dir)

            with self.assertRaisesRegex(ValueError, "must differ"):
                backup_database.create_verified_backup(
                    source,
                    source,
                    restore_drill=False,
                )

    def test_failed_backup_verification_removes_new_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = self.create_source(temp_dir)
            backup = Path(temp_dir) / "unverified.db"

            with patch.object(
                backup_database,
                "summarize_database",
                side_effect=ValueError("verification failed"),
            ):
                with self.assertRaisesRegex(ValueError, "verification failed"):
                    backup_database.create_verified_backup(
                        source,
                        backup,
                        restore_drill=False,
                    )

            self.assertFalse(backup.exists())

    def test_equivalence_check_rejects_row_count_mismatch(self):
        reference = {
            "quick_check": "ok",
            "schema_sha256": "same",
            "table_row_counts": {"prices": 2},
            "application_id": 0,
            "user_version": 0,
        }
        candidate = {
            **reference,
            "table_row_counts": {"prices": 1},
        }

        with self.assertRaisesRegex(ValueError, "table_row_counts"):
            backup_database.assert_equivalent(reference, candidate, "Backup")

    def test_result_path_is_validated_before_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            source = self.create_source(temp_dir)
            backup = Path(temp_dir) / "new-backup.db"
            result = Path(temp_dir) / "existing.json"
            result.write_text("keep-me", encoding="utf-8")

            with self.assertRaisesRegex(FileExistsError, "already exists"):
                backup_database.validate_result_path(source, backup, result)

            self.assertFalse(backup.exists())
            self.assertEqual(result.read_text(encoding="utf-8"), "keep-me")


if __name__ == "__main__":
    unittest.main()
