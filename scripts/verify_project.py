"""Offline-first verification entry point for local Codex work."""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import tomllib
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
UNIT_TEST_MODULES = [
    "tests.test_export_drive_spreadsheet",
    "tests.test_dividend_scan",
    "tests.test_dividend_backtest",
    "tests.test_financial_collector",
    "tests.test_news_analyzer",
    "tests.test_scan",
    "tests.test_evaluate",
    "tests.test_settings",
]
REQUIRED_TABLES = {
    "prices",
    "fundamentals",
    "sync_progress",
    "signals",
    "dividend_financials",
}


def run(command: list[str]) -> bool:
    print(f"[RUN] {' '.join(command)}", flush=True)
    completed = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    return completed.returncode == 0


def check_codex_guidance() -> bool:
    agents_path = PROJECT_ROOT / "AGENTS.md"
    if not agents_path.exists():
        print("[FAIL] AGENTS.md is missing")
        return False
    print("[OK] Codex repository guidance")
    return True

def check_codex_config() -> bool:
    config_path = PROJECT_ROOT / ".codex" / "config.toml"
    try:
        with config_path.open("rb") as handle:
            config = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        print(f"[FAIL] Invalid Codex config: {exc}")
        return False

    if config.get("sandbox_mode") != "workspace-write":
        print("[FAIL] Expected sandbox_mode=workspace-write")
        return False
    if config.get("sandbox_workspace_write", {}).get("network_access") is not False:
        print("[FAIL] Expected project command networking to default to disabled")
        return False
    print("[OK] Codex project config")
    return True



def check_local_files() -> bool:
    required = [
        PROJECT_ROOT / "AGENTS.md",
        PROJECT_ROOT / ".env.example",
        PROJECT_ROOT / "requirements.lock.txt",
    ]
    missing = [path.name for path in required if not path.exists()]
    if missing:
        print(f"[FAIL] Missing migration files: {', '.join(missing)}")
        return False
    print("[OK] Migration files present")
    return True


def check_database() -> bool:
    sys.path.insert(0, str(PROJECT_ROOT))
    from src.settings import DATABASE_PATH

    if not DATABASE_PATH.exists():
        print(f"[FAIL] Database not found: {DATABASE_PATH}")
        return False

    uri = DATABASE_PATH.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = REQUIRED_TABLES - tables
        if missing:
            print(f"[FAIL] Missing DB tables: {', '.join(sorted(missing))}")
            return False

        price_max = connection.execute("SELECT MAX(date) FROM prices").fetchone()[0]
        dividend_summary = connection.execute(
            "SELECT COUNT(*), COUNT(DISTINCT code), MAX(disclosure_date) "
            "FROM dividend_financials"
        ).fetchone()

    size_gib = DATABASE_PATH.stat().st_size / (1024**3)
    print(f"[OK] DB path={DATABASE_PATH} size={size_gib:.2f} GiB price_max={price_max}")
    print(
        "[INFO] dividend_financials "
        f"rows={dividend_summary[0]} codes={dividend_summary[1]} "
        f"latest_disclosure={dividend_summary[2]}"
    )
    print("[WARN] Operational readiness still requires split/share-basis regression checks")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the local Codex project setup")
    parser.add_argument(
        "--with-db",
        action="store_true",
        help="Also inspect the local SQLite database in read-only mode",
    )
    args = parser.parse_args()

    if sys.version_info[:2] != (3, 11):
        print(f"[FAIL] Expected Python 3.11, found {sys.version.split()[0]}")
        return 1

    checks = [
        check_codex_guidance(),
        check_local_files(),
        check_codex_config(),
        run([sys.executable, "-m", "pip", "check"]),
        run([sys.executable, "-m", "unittest", *UNIT_TEST_MODULES]),
    ]
    if args.with_db:
        checks.append(check_database())

    if all(checks):
        print("[PASS] Local Codex project verification completed")
        return 0
    print("[FAIL] Local Codex project verification failed")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
