from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
import uuid
from contextlib import closing
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "backup_retention.py"
SPEC = importlib.util.spec_from_file_location("backup_retention", MODULE_PATH)
backup_retention = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(backup_retention)
POWERSHELL = shutil.which("powershell")


class BackupRetentionTests(unittest.TestCase):
    def create_pair(
        self,
        directory: Path,
        stamp: str,
        *,
        database_bytes: int = 32,
        valid: bool = True,
    ) -> tuple[Path, Path]:
        database = directory / f"stock_data-{stamp}.db"
        result = directory / f"stock_data-{stamp}.verification.json"
        database.write_bytes(stamp.encode("ascii").ljust(database_bytes, b"x"))
        payload = {
            "backup_verified": valid,
            "source_database_modified": False,
            "existing_backup_overwritten": False,
            "backup": {
                "path": str(database.resolve()),
                "size_bytes": database.stat().st_size,
                "quick_check": "ok",
                "schema_sha256": "verified-schema",
                "table_row_counts": {"prices": 2},
            },
            "restore_drill": {
                "performed": True,
                "verified": True,
                "temporary_database_removed": True,
            },
        }
        result.write_text(json.dumps(payload), encoding="utf-8")
        return database, result

    def test_dry_run_keeps_newest_and_changes_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            oldest = self.create_pair(directory, "20260101-090000")
            middle = self.create_pair(directory, "20260108-090000")
            newest = self.create_pair(directory, "20260115-090000")

            plan = backup_retention.plan_retention(
                directory,
                retain_count=2,
                max_total_bytes=10_000,
            )

            self.assertEqual(
                [item["database_path"] for item in plan["prune"]],
                [oldest[0]],
            )
            self.assertEqual(
                [item["database_path"] for item in plan["keep"]],
                [middle[0], newest[0]],
            )
            self.assertTrue(all(path.exists() for path in (*oldest, *middle, *newest)))

    def test_apply_deletes_only_selected_verified_pair(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            oldest = self.create_pair(directory, "20260101-090000")
            newest = self.create_pair(directory, "20260108-090000")
            manual_database = directory / "stock_data-20260913-p7e.db"
            manual_result = directory / "stock_data-20260913-p7e.verification.json"
            manual_database.write_bytes(b"manual")
            manual_result.write_text("{}", encoding="utf-8")

            plan = backup_retention.plan_retention(
                directory,
                retain_count=1,
                max_total_bytes=10_000,
            )
            deleted = backup_retention.apply_retention(plan)

            self.assertEqual(set(deleted), {str(oldest[0]), str(oldest[1])})
            self.assertFalse(oldest[0].exists())
            self.assertFalse(oldest[1].exists())
            self.assertTrue(newest[0].exists())
            self.assertTrue(newest[1].exists())
            self.assertTrue(manual_database.exists())
            self.assertTrue(manual_result.exists())
            protected_paths = {item["path"] for item in plan["protected"]}
            self.assertIn(str(manual_database), protected_paths)
            self.assertIn(str(manual_result), protected_paths)

    def test_invalid_verification_record_is_protected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            invalid = self.create_pair(
                directory,
                "20260101-090000",
                valid=False,
            )
            valid = self.create_pair(directory, "20260108-090000")

            plan = backup_retention.plan_retention(
                directory,
                retain_count=1,
                max_total_bytes=10_000,
            )
            backup_retention.apply_retention(plan)

            self.assertTrue(invalid[0].exists())
            self.assertTrue(invalid[1].exists())
            self.assertTrue(valid[0].exists())
            self.assertEqual(plan["prune"], [])

    def test_size_limit_prunes_to_minimum_count(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            self.create_pair(directory, "20260101-090000", database_bytes=200)
            newest = self.create_pair(
                directory,
                "20260108-090000",
                database_bytes=200,
            )

            plan = backup_retention.plan_retention(
                directory,
                retain_count=2,
                minimum_count=1,
                max_total_bytes=newest[0].stat().st_size + newest[1].stat().st_size,
            )

            self.assertEqual(len(plan["keep"]), 1)
            self.assertEqual(plan["keep"][0]["database_path"], newest[0])
            self.assertTrue(plan["limit_satisfied"])

    def test_protected_file_can_make_limit_unsatisfied(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            self.create_pair(directory, "20260108-090000", database_bytes=100)
            protected = directory / "do-not-delete.bin"
            protected.write_bytes(b"x" * 500)

            plan = backup_retention.plan_retention(
                directory,
                retain_count=1,
                minimum_count=1,
                max_total_bytes=100,
            )

            self.assertFalse(plan["limit_satisfied"])
            self.assertEqual(plan["prune"], [])
            self.assertTrue(protected.exists())

    def test_repository_directory_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside the repository"):
            backup_retention.ensure_safe_directory(PROJECT_ROOT)


@unittest.skipUnless(os.name == "nt" and POWERSHELL, "requires Windows PowerShell")
class BackupRunnerTests(unittest.TestCase):
    def test_runner_creates_verified_pair_and_retention_log(self) -> None:
        with tempfile.TemporaryDirectory(prefix="jquants backup ") as temp_dir:
            work = Path(temp_dir)
            source = work / "source.db"
            backup_dir = work / "backups"
            with closing(sqlite3.connect(source)) as connection:
                connection.execute("CREATE TABLE prices (code TEXT)")
                connection.execute("INSERT INTO prices VALUES ('20030')")
                connection.commit()

            completed = subprocess.run(
                [
                    POWERSHELL,
                    "-NoProfile",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-File",
                    str(PROJECT_ROOT / "scripts" / "run_database_backup.ps1"),
                    "-RepositoryRoot",
                    str(PROJECT_ROOT),
                    "-SourcePath",
                    str(source),
                    "-BackupDirectory",
                    str(backup_dir),
                    "-RetentionCount",
                    "2",
                    "-MinimumCount",
                    "1",
                    "-MaxTotalBytes",
                    "10485760",
                    "-LockName",
                    f"Local\\JQuantsBackupTest-{uuid.uuid4()}",
                ],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(len(list(backup_dir.glob("stock_data-*.db"))), 1)
            self.assertEqual(
                len(list(backup_dir.glob("stock_data-*.verification.json"))),
                1,
            )
            log_text = (backup_dir / "backup_operation.log").read_text(
                encoding="utf-8-sig"
            )
            self.assertIn("[END] Verified backup and retention completed.", log_text)

    def test_scheduler_configuration_is_weekly_and_does_not_run_workflow(self) -> None:
        script = (
            PROJECT_ROOT / "scripts" / "configure_database_backup_task.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn('$taskName = "NISA-JQuant Database Backup"', script)
        self.assertIn("-DaysOfWeek Saturday", script)
        self.assertIn("-At 9am", script)
        self.assertIn("-StartWhenAvailable", script)
        self.assertIn("WorkflowsExecuted = $false", script)
        self.assertIn("-RetentionCount 8", script)
        self.assertIn("-MaxTotalBytes 21474836480", script)


if __name__ == "__main__":
    unittest.main()
