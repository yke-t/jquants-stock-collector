"""Read-only audit for the scheduled daily and dividend workflows on Windows."""

from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TASK_NAMES = ("NISA-JQuant Daily", "NISA-JQuant Dividend Daily")
WORKFLOWS = {
    "daily": {
        "task_name": "NISA-JQuant Daily",
        "log_name": "daily_operation.log",
        "start_marker": "[START] Daily Routine:",
        "success_marker": "[END] Finished:",
        "failure_marker": "[ERROR] Daily Routine failed",
    },
    "dividend": {
        "task_name": "NISA-JQuant Dividend Daily",
        "log_name": "dividend_operation.log",
        "start_marker": "[START] Dividend Routine:",
        "success_marker": "[END] Dividend Routine finished:",
        "failure_marker": "[ERROR] Dividend Routine failed",
    },
}


def query_scheduled_tasks() -> dict[str, dict[str, Any]]:
    """Return the two task states without starting either task."""
    powershell = r"""
$names = @('NISA-JQuant Daily', 'NISA-JQuant Dividend Daily')
$rows = foreach ($name in $names) {
    $task = Get-ScheduledTask -TaskName $name -ErrorAction Stop
    $info = Get-ScheduledTaskInfo -TaskName $name -ErrorAction Stop
    [pscustomobject]@{
        TaskName = $name
        State = [string]$task.State
        LastRunTime = $info.LastRunTime.ToString('o')
        LastTaskResult = $info.LastTaskResult
        NextRunTime = $info.NextRunTime.ToString('o')
        NumberOfMissedRuns = $info.NumberOfMissedRuns
        MultipleInstances = [string]$task.Settings.MultipleInstances
    }
}
$rows | ConvertTo-Json -Depth 4 -Compress
"""
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", powershell],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        message = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"Task Scheduler query failed: {message}")
    payload = json.loads(completed.stdout)
    rows = payload if isinstance(payload, list) else [payload]
    return {str(row["TaskName"]): row for row in rows}


def audit_task(task: dict[str, Any], target_date: date) -> dict[str, Any]:
    last_run_text = str(task.get("LastRunTime", ""))
    try:
        last_run_date = datetime.fromisoformat(last_run_text).date()
    except ValueError:
        return {"status": "fail", "reason": "invalid LastRunTime", **task}

    if last_run_date < target_date:
        status = "pending"
        reason = "target date has not run yet"
    elif last_run_date > target_date:
        status = "newer"
        reason = "a newer task run supersedes Task Scheduler history"
    elif int(task.get("LastTaskResult", -1)) == 0:
        status = "pass"
        reason = "Task Scheduler returned 0"
    else:
        status = "fail"
        reason = f"Task Scheduler returned {task.get('LastTaskResult')}"

    return {"status": status, "reason": reason, **task}


def audit_log(path: Path, target_date: date, workflow: dict[str, str]) -> dict[str, Any]:
    if not path.exists():
        return {"status": "fail", "reason": "log file is missing", "path": str(path)}

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    date_marker = target_date.strftime("%m/%d/%Y")
    start_indexes = [
        index
        for index, line in enumerate(lines)
        if workflow["start_marker"] in line and date_marker in line
    ]
    if not start_indexes:
        return {
            "status": "pending",
            "reason": "target date has no start marker",
            "path": str(path),
        }

    section = lines[start_indexes[-1] :]
    terminals: list[tuple[int, str, str]] = []
    for index, line in enumerate(section):
        if workflow["success_marker"] in line:
            terminals.append((index, "pass", line.strip()))
        if workflow["failure_marker"] in line:
            terminals.append((index, "fail", line.strip()))

    if not terminals:
        return {
            "status": "pending",
            "reason": "workflow started but has no terminal marker",
            "path": str(path),
            "start": section[0].strip(),
        }

    _, status, terminal = max(terminals, key=lambda item: item[0])
    return {
        "status": status,
        "reason": "terminal log marker found",
        "path": str(path),
        "start": section[0].strip(),
        "terminal": terminal,
    }


def audit_database(database_path: Path, target_date: date) -> dict[str, Any]:
    if not database_path.exists():
        return {"status": "fail", "reason": "database is missing", "path": str(database_path)}

    uri = database_path.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        price_max = connection.execute("SELECT MAX(date) FROM prices").fetchone()[0]
        signal_max = connection.execute("SELECT MAX(signal_date) FROM signals").fetchone()[0]
        sync_max = connection.execute(
            "SELECT MAX(last_synced_date) FROM sync_progress"
        ).fetchone()[0]
        dividend = connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT code), MAX(updated_at) "
            "FROM dividend_financials"
        ).fetchone()

    try:
        sync_date = date.fromisoformat(str(sync_max)[:10])
    except ValueError:
        sync_date = None

    return {
        "status": "fresh" if sync_date and sync_date >= target_date else "stale",
        "path": str(database_path),
        "price_max": price_max,
        "signal_max": signal_max,
        "sync_progress_max": sync_max,
        "dividend_financial_rows": dividend[0],
        "dividend_financial_codes": dividend[1],
        "dividend_financial_updated_at": dividend[2],
    }


def latest_artifact(reports_dir: Path, pattern: str, target_date: date) -> dict[str, Any]:
    candidates = sorted(
        reports_dir.glob(pattern),
        key=lambda candidate: candidate.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return {"status": "missing", "pattern": pattern}
    latest = candidates[0]
    modified = datetime.fromtimestamp(latest.stat().st_mtime)
    return {
        "status": "fresh" if modified.date() >= target_date else "stale",
        "path": str(latest),
        "bytes": latest.stat().st_size,
        "last_write_time": modified.astimezone().isoformat(),
    }


def workflow_status(task_status: str, log_status: str) -> str:
    if "fail" in (task_status, log_status):
        return "fail"
    if log_status == "pass" and task_status in ("pass", "newer"):
        return "pass"
    return "pending"


def overall_status(statuses: list[str]) -> str:
    if "fail" in statuses:
        return "fail"
    if statuses and all(status == "pass" for status in statuses):
        return "pass"
    return "pending"


def operational_status(workflow_statuses: list[str], evidence_statuses: list[str]) -> str:
    workflow_result = overall_status(workflow_statuses)
    if workflow_result != "pass":
        return workflow_result
    if all(status == "fresh" for status in evidence_statuses):
        return "pass"
    return "fail"


def build_audit(repository_root: Path, target_date: date) -> dict[str, Any]:
    tasks = query_scheduled_tasks()
    workflows: dict[str, Any] = {}
    for name, configuration in WORKFLOWS.items():
        task = audit_task(tasks[configuration["task_name"]], target_date)
        log = audit_log(
            repository_root / configuration["log_name"],
            target_date,
            configuration,
        )
        workflows[name] = {
            "status": workflow_status(task["status"], log["status"]),
            "task": task,
            "log": log,
        }

    report_dir = repository_root / "reports"
    workflow_statuses = [workflow["status"] for workflow in workflows.values()]
    database = audit_database(repository_root / "stock_data.db", target_date)
    artifacts = {
        "dividend_candidates": latest_artifact(
            report_dir,
            "dividend_candidates_*.csv",
            target_date,
        ),
        "dividend_backtest": latest_artifact(
            report_dir,
            "dividend_backtest_monthly*.csv",
            target_date,
        ),
    }
    evidence_statuses = [database["status"]] + [
        artifact["status"] for artifact in artifacts.values()
    ]
    return {
        "schema_version": "1.0",
        "audited_at": datetime.now().astimezone().isoformat(),
        "target_date": target_date.isoformat(),
        "overall_status": operational_status(workflow_statuses, evidence_statuses),
        "workflows": workflows,
        "database": database,
        "artifacts": artifacts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit scheduled workflows without starting them or modifying data."
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=date.today(),
        help="Scheduled run date to audit (YYYY-MM-DD; default: today)",
    )
    parser.add_argument(
        "--repository-root",
        type=Path,
        default=PROJECT_ROOT,
        help="Repository root containing logs, reports, and stock_data.db",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        audit = build_audit(args.repository_root.resolve(), args.date)
    except Exception as error:
        print(
            json.dumps(
                {
                    "overall_status": "inspection_error",
                    "error": f"{type(error).__name__}: {error}",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 3

    print(json.dumps(audit, ensure_ascii=False, indent=2))
    return {"pass": 0, "fail": 1, "pending": 2}[audit["overall_status"]]


if __name__ == "__main__":
    raise SystemExit(main())
