"""Safely refresh the listed-issue master from J-Quants V2.

The default mode fetches and validates the live response but does not modify
SQLite. ``--apply`` requires a verified backup created after the current
database snapshot and swaps the table in one SQLite transaction.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from contextlib import closing
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.client import JQuantsClient
from src.settings import DATABASE_PATH


REQUIRED_COLUMNS = {"date", "code", "coname", "scalecat"}
TARGET_SCALECATS = (
    "TOPIX Small 1",
    "TOPIX Small 2",
    "TOPIX Mid400",
)
DEFAULT_MINIMUM_ROWS = 3000
DEFAULT_MINIMUM_TARGET_ROWS = 1000
DEFAULT_MAX_MASTER_AGE_DAYS = 120
DEFAULT_MIN_PROJECTED_PRICE_COVERAGE = 0.95
IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
STAGING_TABLE = "fundamentals_refresh_p8"
RETIRED_TABLE = "fundamentals_retired_p8"


def quote_identifier(identifier: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Unsafe SQLite identifier: {identifier!r}")
    return f'"{identifier}"'


def response_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response.get("data")
    if not isinstance(rows, list):
        raise ValueError("J-Quants master response does not contain a data list")
    return rows


def normalize_rows(rows: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    if not rows:
        raise ValueError("J-Quants master response is empty")

    columns: list[str] = []
    seen_columns: set[str] = set()
    normalized: list[dict[str, Any]] = []
    for source_row in rows:
        if not isinstance(source_row, dict):
            raise ValueError("J-Quants master response contains a non-object row")
        lowered: dict[str, Any] = {}
        for source_name, value in source_row.items():
            name = str(source_name).lower()
            quote_identifier(name)
            if name in lowered:
                raise ValueError(f"Duplicate case-insensitive field: {name}")
            lowered[name] = value
            if name not in seen_columns:
                seen_columns.add(name)
                columns.append(name)
        normalized.append(lowered)

    missing = REQUIRED_COLUMNS - seen_columns
    if missing:
        raise ValueError(f"J-Quants master fields are missing: {', '.join(sorted(missing))}")
    for row in normalized:
        for column in columns:
            row.setdefault(column, None)
        row["code"] = str(row.get("code") or "").strip()
        row["date"] = str(row.get("date") or "").strip()
        row["scalecat"] = str(row.get("scalecat") or "").strip()
    return columns, normalized


def validate_rows(
    rows: list[dict[str, Any]],
    as_of: date,
    minimum_rows: int = DEFAULT_MINIMUM_ROWS,
    minimum_target_rows: int = DEFAULT_MINIMUM_TARGET_ROWS,
    max_age_days: int = DEFAULT_MAX_MASTER_AGE_DAYS,
) -> dict[str, Any]:
    codes = [row["code"] for row in rows]
    blank_codes = sum(not code for code in codes)
    duplicate_codes = len(codes) - len(set(codes))
    dates: list[date] = []
    for row in rows:
        try:
            dates.append(date.fromisoformat(row["date"][:10]))
        except ValueError as error:
            raise ValueError(f"Invalid J-Quants master date: {row['date']!r}") from error
    target_rows = sum(row["scalecat"] in TARGET_SCALECATS for row in rows)
    max_date = max(dates)
    min_date = min(dates)
    age_days = (as_of - max_date).days

    issues: list[str] = []
    if len(rows) < minimum_rows:
        issues.append(f"row count {len(rows)} is below {minimum_rows}")
    if blank_codes:
        issues.append(f"{blank_codes} row(s) have a blank code")
    if duplicate_codes:
        issues.append(f"{duplicate_codes} duplicate code row(s) were returned")
    if target_rows < minimum_target_rows:
        issues.append(f"target row count {target_rows} is below {minimum_target_rows}")
    if age_days > max_age_days:
        issues.append(f"master age {age_days} days exceeds {max_age_days} days")
    if age_days < -7:
        issues.append("master response is unexpectedly future-dated")
    if issues:
        raise ValueError("; ".join(issues))

    return {
        "rows": len(rows),
        "unique_codes": len(set(codes)),
        "target_rows": target_rows,
        "min_date": min_date.isoformat(),
        "max_date": max_date.isoformat(),
        "age_days": age_days,
    }


def current_master(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(fundamentals)")
    }
    if not {"code", "scalecat"}.issubset(columns):
        raise ValueError("Current fundamentals table lacks code or scalecat")
    date_expression = "date" if "date" in columns else "NULL"
    return {
        str(row[0]): {"scalecat": row[1], "date": row[2]}
        for row in connection.execute(
            f"SELECT code, scalecat, {date_expression} FROM fundamentals"
        )
    }


def compare_master(
    existing: dict[str, dict[str, Any]],
    incoming_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    incoming = {row["code"]: row for row in incoming_rows}
    existing_codes = set(existing)
    incoming_codes = set(incoming)
    category_changes = [
        {
            "code": code,
            "before": existing[code].get("scalecat"),
            "after": incoming[code].get("scalecat"),
        }
        for code in sorted(existing_codes & incoming_codes)
        if existing[code].get("scalecat") != incoming[code].get("scalecat")
    ]
    return {
        "existing_codes": len(existing_codes),
        "incoming_codes": len(incoming_codes),
        "added_count": len(incoming_codes - existing_codes),
        "removed_count": len(existing_codes - incoming_codes),
        "category_change_count": len(category_changes),
        "added_codes": sorted(incoming_codes - existing_codes),
        "removed_codes": sorted(existing_codes - incoming_codes),
        "category_changes": category_changes,
    }


def projected_price_coverage(
    connection: sqlite3.Connection,
    incoming_rows: list[dict[str, Any]],
    minimum_coverage: float = DEFAULT_MIN_PROJECTED_PRICE_COVERAGE,
) -> dict[str, Any]:
    latest_date = connection.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    latest_codes = {
        str(row[0])
        for row in connection.execute(
            "SELECT code FROM prices WHERE date = ?", (latest_date,)
        )
    }
    target_codes = {
        row["code"] for row in incoming_rows if row["scalecat"] in TARGET_SCALECATS
    }
    covered_codes = target_codes & latest_codes
    missing_codes = sorted(target_codes - latest_codes)
    coverage_pct = (
        round(len(covered_codes) / len(target_codes) * 100, 4)
        if target_codes
        else None
    )
    return {
        "latest_price_date": latest_date,
        "target_codes": len(target_codes),
        "covered_codes": len(covered_codes),
        "missing_code_count": len(missing_codes),
        "missing_codes": missing_codes,
        "coverage_pct": coverage_pct,
        "minimum_coverage_pct": minimum_coverage * 100,
        "meets_minimum": bool(
            coverage_pct is not None and coverage_pct >= minimum_coverage * 100
        ),
    }


def validate_backup_evidence(database: Path, result_path: Path) -> dict[str, Any]:
    if not result_path.is_file():
        raise FileNotFoundError(f"Backup verification not found: {result_path}")
    payload = json.loads(result_path.read_text(encoding="utf-8-sig"))
    source = payload.get("source", {})
    restore = payload.get("restore_drill", {})
    if payload.get("backup_verified") is not True:
        raise ValueError("Backup evidence is not verified")
    if payload.get("source_database_modified") is not False:
        raise ValueError("Backup evidence does not protect an unchanged source")
    if restore.get("verified") is not True:
        raise ValueError("Backup evidence has no verified restore drill")
    if Path(str(source.get("path", ""))).resolve() != database.resolve():
        raise ValueError("Backup evidence belongs to a different source database")
    if int(source.get("size_bytes", -1)) != database.stat().st_size:
        raise ValueError("Database size differs from the backed-up source")
    if source.get("quick_check") != "ok":
        raise ValueError("Backed-up source did not pass SQLite quick_check")
    created_at = datetime.fromisoformat(str(payload.get("created_at", "")))
    database_modified_at = datetime.fromtimestamp(
        database.stat().st_mtime,
        tz=created_at.tzinfo,
    )
    if created_at < database_modified_at:
        raise ValueError("Backup evidence predates the current database")
    return {
        "result_path": str(result_path.resolve()),
        "created_at": created_at.isoformat(),
        "backup_path": str(payload.get("backup", {}).get("path", "")),
        "source_size_bytes": int(source["size_bytes"]),
    }


def replace_master_atomic(
    database: Path,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    quoted_columns = [quote_identifier(column) for column in columns]
    definitions = [
        f"{column} TEXT{' PRIMARY KEY' if name == 'code' else ''}"
        for name, column in zip(columns, quoted_columns)
    ]
    insert_sql = (
        f"INSERT INTO {quote_identifier(STAGING_TABLE)} "
        f"({', '.join(quoted_columns)}) VALUES "
        f"({', '.join('?' for _ in columns)})"
    )
    values = [tuple(row[column] for column in columns) for row in rows]

    with closing(sqlite3.connect(database)) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(STAGING_TABLE)}")
            connection.execute(f"DROP TABLE IF EXISTS {quote_identifier(RETIRED_TABLE)}")
            connection.execute(
                f"CREATE TABLE {quote_identifier(STAGING_TABLE)} "
                f"({', '.join(definitions)})"
            )
            connection.executemany(insert_sql, values)
            staged = connection.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT code) "
                f"FROM {quote_identifier(STAGING_TABLE)}"
            ).fetchone()
            if staged != (len(rows), len(rows)):
                raise ValueError("Staged fundamentals row validation failed")
            connection.execute(
                f"ALTER TABLE fundamentals RENAME TO {quote_identifier(RETIRED_TABLE)}"
            )
            connection.execute(
                f"ALTER TABLE {quote_identifier(STAGING_TABLE)} RENAME TO fundamentals"
            )
            connection.execute(f"DROP TABLE {quote_identifier(RETIRED_TABLE)}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    with closing(sqlite3.connect(f"{database.resolve().as_uri()}?mode=ro", uri=True)) as check:
        quick_check = [str(row[0]) for row in check.execute("PRAGMA quick_check")]
        row = check.execute(
            "SELECT COUNT(*), COUNT(DISTINCT code), MIN(date), MAX(date) "
            "FROM fundamentals"
        ).fetchone()
    if quick_check != ["ok"] or row[0] != len(rows) or row[1] != len(rows):
        raise ValueError("Post-swap fundamentals verification failed")
    return {
        "quick_check": "ok",
        "rows": int(row[0]),
        "unique_codes": int(row[1]),
        "min_date": row[2],
        "max_date": row[3],
    }


def refresh_master(
    database: Path,
    response: dict[str, Any],
    *,
    apply: bool,
    backup_verification: Path | None,
    as_of: date,
    minimum_rows: int = DEFAULT_MINIMUM_ROWS,
    minimum_target_rows: int = DEFAULT_MINIMUM_TARGET_ROWS,
    max_age_days: int = DEFAULT_MAX_MASTER_AGE_DAYS,
    min_projected_price_coverage: float = DEFAULT_MIN_PROJECTED_PRICE_COVERAGE,
) -> dict[str, Any]:
    database = database.resolve()
    if not database.is_file():
        raise FileNotFoundError(f"Database not found: {database}")
    columns, rows = normalize_rows(response_rows(response))
    source_summary = validate_rows(
        rows,
        as_of,
        minimum_rows=minimum_rows,
        minimum_target_rows=minimum_target_rows,
        max_age_days=max_age_days,
    )
    with closing(sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True)) as connection:
        comparison = compare_master(current_master(connection), rows)
        projection = projected_price_coverage(
            connection,
            rows,
            minimum_coverage=min_projected_price_coverage,
        )

    backup_summary = None
    database_verification = None
    if apply:
        if not projection["meets_minimum"]:
            raise ValueError(
                "Projected target price coverage is below the required minimum"
            )
        if backup_verification is None:
            raise ValueError("--backup-verification is required with --apply")
        backup_summary = validate_backup_evidence(database, backup_verification.resolve())
        database_verification = replace_master_atomic(database, columns, rows)

    return {
        "schema_version": "1.0",
        "status": "passed",
        "applied": apply,
        "database": str(database),
        "source": source_summary,
        "comparison": comparison,
        "projected_daily_price_coverage": projection,
        "backup_verification": backup_summary,
        "database_verification": database_verification,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DATABASE_PATH)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-verification", type=Path)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument("--minimum-rows", type=int, default=DEFAULT_MINIMUM_ROWS)
    parser.add_argument(
        "--minimum-target-rows",
        type=int,
        default=DEFAULT_MINIMUM_TARGET_ROWS,
    )
    parser.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_MASTER_AGE_DAYS)
    parser.add_argument(
        "--min-projected-price-coverage",
        type=float,
        default=DEFAULT_MIN_PROJECTED_PRICE_COVERAGE,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.minimum_rows <= 0 or args.minimum_target_rows <= 0:
            raise ValueError("minimum row thresholds must be positive")
        if args.max_age_days < 0:
            raise ValueError("--max-age-days must be nonnegative")
        if not 0 < args.min_projected_price_coverage <= 1:
            raise ValueError("--min-projected-price-coverage must be in (0, 1]")
        response = JQuantsClient().get_listed_info()
        result = refresh_master(
            args.database,
            response,
            apply=args.apply,
            backup_verification=args.backup_verification,
            as_of=args.as_of,
            minimum_rows=args.minimum_rows,
            minimum_target_rows=args.minimum_target_rows,
            max_age_days=args.max_age_days,
            min_projected_price_coverage=args.min_projected_price_coverage,
        )
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
