# -*- coding: utf-8 -*-
import importlib.util
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = PROJECT_ROOT / "scripts" / "audit_scheduled_operations.py"
SPEC = importlib.util.spec_from_file_location("audit_scheduled_operations", MODULE_PATH)
audit = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(audit)


class ScheduledOperationAuditTest(unittest.TestCase):
    def setUp(self):
        self.target = date(2026, 9, 4)
        self.workflow = audit.WORKFLOWS["daily"]

    def test_log_pass_uses_latest_target_date_section(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "daily_operation.log"
            path.write_text(
                "[START] Daily Routine: Thu 09/03/2026 17:00:00\n"
                "[ERROR] Daily Routine failed with exit code 1\n"
                "[START] Daily Routine: Fri 09/04/2026 17:00:00\n"
                "[END] Finished: Fri 09/04/2026 17:08:00\n",
                encoding="utf-8",
            )
            result = audit.audit_log(path, self.target, self.workflow)

        self.assertEqual(result["status"], "pass")
        self.assertIn("[END]", result["terminal"])

    def test_log_detects_failure_even_when_marker_is_joined_to_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "daily_operation.log"
            path.write_text(
                "[START] Daily Routine: Fri 09/04/2026 17:00:00\n"
                "candidate output[ERROR] Daily Routine failed with exit code 1\n",
                encoding="utf-8",
            )
            result = audit.audit_log(path, self.target, self.workflow)

        self.assertEqual(result["status"], "fail")

    def test_log_is_pending_before_target_run(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "daily_operation.log"
            path.write_text(
                "[START] Daily Routine: Thu 09/03/2026 17:00:00\n",
                encoding="utf-8",
            )
            result = audit.audit_log(path, self.target, self.workflow)

        self.assertEqual(result["status"], "pending")

    def test_task_is_pending_when_latest_run_precedes_target(self):
        result = audit.audit_task(
            {
                "LastRunTime": "2026-09-03T17:00:00+09:00",
                "LastTaskResult": 1,
            },
            self.target,
        )
        self.assertEqual(result["status"], "pending")

    def test_task_passes_only_with_zero_result_on_target_date(self):
        task = {
            "LastRunTime": "2026-09-04T17:00:00+09:00",
            "LastTaskResult": 0,
        }
        self.assertEqual(audit.audit_task(task, self.target)["status"], "pass")
        task["LastTaskResult"] = 1
        self.assertEqual(audit.audit_task(task, self.target)["status"], "fail")

    def test_workflow_and_overall_status(self):
        self.assertEqual(audit.workflow_status("pass", "pass"), "pass")
        self.assertEqual(audit.workflow_status("pending", "pending"), "pending")
        self.assertEqual(audit.workflow_status("pass", "fail"), "fail")
        self.assertEqual(audit.overall_status(["pass", "pass"]), "pass")
        self.assertEqual(audit.overall_status(["pass", "pending"]), "pending")
        self.assertEqual(audit.overall_status(["pass", "fail"]), "fail")
        self.assertEqual(
            audit.operational_status(["pass", "pass"], ["fresh", "fresh"]),
            "pass",
        )
        self.assertEqual(
            audit.operational_status(["pass", "pass"], ["fresh", "stale"]),
            "fail",
        )
        self.assertEqual(
            audit.operational_status(["pending", "pending"], ["stale", "stale"]),
            "pending",
        )

    def test_json_output_falls_back_to_ascii_for_legacy_windows_console(self):
        rendered = audit.format_json_for_stdout(
            {"overall_status": "inspection_error", "error": "アクセスは拒否されました"},
            encoding="cp1252",
        )

        self.assertIn(r"\u30a2", rendered)
        self.assertEqual(json.loads(rendered)["error"], "アクセスは拒否されました")

    def test_json_output_keeps_unicode_when_console_supports_it(self):
        rendered = audit.format_json_for_stdout(
            {"error": "アクセスは拒否されました"},
            encoding="utf-8",
        )

        self.assertIn("アクセスは拒否されました", rendered)

    def backup_plan(self, newest: datetime, *, limit_satisfied: bool = True):
        database_path = Path("C:/backup") / f"stock_data-{newest:%Y%m%d-%H%M%S}.db"
        result_path = database_path.with_name(
            f"{database_path.stem}.verification.json"
        )
        item = {
            "stamp": newest.replace(tzinfo=None),
            "database_path": database_path,
            "result_path": result_path,
            "size_bytes": 100,
        }
        return {
            "directory": Path("C:/backup"),
            "retain_count": 8,
            "minimum_count": 1,
            "max_total_bytes": 20 * 1024**3,
            "directory_total_bytes": 100,
            "projected_total_bytes": 100,
            "limit_satisfied": limit_satisfied,
            "managed": [item],
            "prune": [],
            "protected": [],
        }

    def test_backup_audit_accepts_fresh_live_validation_before_first_run(self):
        audited_at = datetime(2026, 9, 14, 20, tzinfo=timezone(timedelta(hours=9)))
        task = {
            "LastRunTime": "1999-11-30T00:00:00+09:00",
            "LastTaskResult": audit.BACKUP_NOT_RUN_RESULT,
            "NumberOfMissedRuns": 0,
        }
        with patch.object(
            audit.backup_retention,
            "plan_retention",
            return_value=self.backup_plan(datetime(2026, 9, 14, 0, 0, 34)),
        ):
            result = audit.audit_database_backup(task, Path("C:/backup"), audited_at)

        self.assertEqual(result["status"], "not_due")

    def test_backup_audit_matches_successful_task_to_verified_pair(self):
        zone = timezone(timedelta(hours=9))
        audited_at = datetime(2026, 9, 19, 20, tzinfo=zone)
        task = {
            "LastRunTime": "2026-09-19T09:00:00+09:00",
            "LastTaskResult": 0,
            "NumberOfMissedRuns": 0,
        }
        with patch.object(
            audit.backup_retention,
            "plan_retention",
            return_value=self.backup_plan(datetime(2026, 9, 19, 9, 0, 5)),
        ):
            result = audit.audit_database_backup(task, Path("C:/backup"), audited_at)

        self.assertEqual(result["status"], "pass")

    def test_backup_audit_fails_stale_not_run_task(self):
        zone = timezone(timedelta(hours=9))
        audited_at = datetime(2026, 9, 22, 20, tzinfo=zone)
        task = {
            "LastRunTime": "1999-11-30T00:00:00+09:00",
            "LastTaskResult": audit.BACKUP_NOT_RUN_RESULT,
            "NumberOfMissedRuns": 0,
        }
        with patch.object(
            audit.backup_retention,
            "plan_retention",
            return_value=self.backup_plan(datetime(2026, 9, 14, 0, 0, 34)),
        ):
            result = audit.audit_database_backup(task, Path("C:/backup"), audited_at)

        self.assertEqual(result["status"], "fail")
        self.assertIn("freshness", result["reason"])

    def test_backup_failure_overrides_operational_pass(self):
        self.assertEqual(
            audit.combine_operational_and_backup_status("pass", "fail"),
            "fail",
        )
        self.assertEqual(
            audit.combine_operational_and_backup_status("pass", "not_due"),
            "pass",
        )

    def test_data_coverage_failure_overrides_operational_pass(self):
        self.assertEqual(
            audit.combine_operational_and_backup_status("pass", "pass", "fail"),
            "fail",
        )
        self.assertEqual(
            audit.combine_operational_and_backup_status("pass", "pass", "pass"),
            "pass",
        )


if __name__ == "__main__":
    unittest.main()
