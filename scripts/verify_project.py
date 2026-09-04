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
    "tests.test_collector",
    "tests.test_export_drive_spreadsheet",
    "tests.test_dividend_scan",
    "tests.test_dividend_backtest",
    "tests.test_financial_collector",
    "tests.test_news_analyzer",
    "tests.test_scan",
    "tests.test_evaluate",
    "tests.test_settings",
    "tests.test_update_yfinance",
    "tests.test_split_factor_backfill",
    "tests.test_sync_bigquery",
    "tests.test_run_with_lock",
    "tests.test_rotate_jquants_api_key",
    "tests.test_notifier",
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
    import pandas as pd

    from src.dividend_scan import annotate_share_basis, classify_candidate
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

        regression_codes = ("20030", "19610")
        prices = pd.read_sql(
            """SELECT date, code, close, adjustmentfactor
               FROM prices
               WHERE code IN (?, ?)
               ORDER BY code, date""",
            connection,
            params=regression_codes,
            parse_dates=["date"],
        )
        financials = pd.read_sql(
            """SELECT df.*
               FROM dividend_financials df
               JOIN (
                   SELECT code, MAX(disclosure_date) AS disclosure_date
                   FROM dividend_financials
                   WHERE code IN (?, ?)
                   GROUP BY code
               ) latest
                 ON df.code = latest.code
                AND df.disclosure_date = latest.disclosure_date
               ORDER BY df.code, df.period""",
            connection,
            params=regression_codes,
            parse_dates=["disclosure_date"],
        )

    size_gib = DATABASE_PATH.stat().st_size / (1024**3)
    print(f"[OK] DB path={DATABASE_PATH} size={size_gib:.2f} GiB price_max={price_max}")
    print(
        "[INFO] dividend_financials "
        f"rows={dividend_summary[0]} codes={dividend_summary[1]} "
        f"latest_disclosure={dividend_summary[2]}"
    )
    latest_prices = (
        prices.sort_values(["code", "date"])
        .drop_duplicates("code", keep="last")[["date", "code", "close"]]
    )
    latest_financials = (
        financials.sort_values(["code", "disclosure_date", "period"])
        .drop_duplicates("code", keep="last")
    )
    candidates = latest_prices.merge(latest_financials, on="code", how="inner")
    if set(candidates["code"]) != set(regression_codes):
        print("[FAIL] Missing split regression data for 20030 or 19610")
        return False

    candidates = annotate_share_basis(prices, candidates, prices["date"].max())
    split_events = {
        "20030": (pd.Timestamp("2026-03-30"), 0.25),
        "19610": (pd.Timestamp("2026-04-28"), 1.0 / 3.0),
    }
    for _, candidate in candidates.iterrows():
        result = classify_candidate(candidate)
        code = candidate["code"]
        status = candidate["share_basis_status"]
        factor = candidate["share_basis_factor"]
        if status == "UNVERIFIED":
            if result["verdict"] != "DATA_WARNING" or result["dividend_yield"] is not None:
                print(f"[FAIL] {code} unverified share basis was not blocked")
                return False
        else:
            event_date, event_factor = split_events[code]
            disclosure_date = pd.Timestamp(candidate["disclosure_date"])
            expected_factor = event_factor if disclosure_date < event_date else 1.0
            if abs(factor - expected_factor) > 1e-9:
                print(
                    f"[FAIL] {code} factor={factor:g}, "
                    f"expected={expected_factor:g} for disclosure={disclosure_date.date()}"
                )
                return False
            if result["dividend_yield"] is None:
                print(f"[FAIL] {code} verified share basis has no dividend yield")
                return False
        print(
            f"[OK] split regression code={code} status={status} "
            f"disclosure={pd.Timestamp(candidate['disclosure_date']).date()} "
            f"factor={factor:g} verdict={result['verdict']} "
            f"yield={result['dividend_yield']} reason={candidate['share_basis_reason']}"
        )

    if any(candidates["share_basis_status"] == "UNVERIFIED"):
        print("[WARN] Explicit adjustment-factor backfill is still required")
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
