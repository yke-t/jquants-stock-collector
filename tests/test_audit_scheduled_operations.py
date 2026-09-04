# -*- coding: utf-8 -*-
import importlib.util
import tempfile
import unittest
from datetime import date
from pathlib import Path


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


if __name__ == "__main__":
    unittest.main()
