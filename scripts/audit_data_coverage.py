"""Read-only coverage and freshness audit for the local market database."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any, Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = PROJECT_ROOT / "stock_data.db"
TARGET_SCALECATS = (
    "TOPIX Small 1",
    "TOPIX Small 2",
    "TOPIX Mid400",
)
DIVIDEND_SYNC_KEY_PREFIX = "dividend_financials:"

DEFAULT_MAX_MASTER_AGE_DAYS = 120
DEFAULT_MAX_PRICE_AGE_DAYS = 7
DEFAULT_MIN_TARGET_PRICE_COVERAGE = 0.95
DEFAULT_EXPECTED_DISCLOSURE_DELAY_DAYS = 84
DEFAULT_DISCLOSURE_DELAY_GRACE_DAYS = 21
DEFAULT_MAX_DIVIDEND_UPDATE_AGE_DAYS = 7


def connect_read_only(database: Path) -> sqlite3.Connection:
    resolved = database.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(f"Database not found: {resolved}")
    connection = sqlite3.connect(f"{resolved.as_uri()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def percentage(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator * 100, 4)


def parse_stored_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def date_age_days(value: Any, as_of: date) -> int | None:
    parsed = parse_stored_date(value)
    if parsed is None:
        return None
    return (as_of - parsed).days


def _placeholders(values: Iterable[Any]) -> str:
    return ",".join("?" for _ in values)


def audit_master(
    connection: sqlite3.Connection,
    as_of: date,
    max_age_days: int = DEFAULT_MAX_MASTER_AGE_DAYS,
) -> dict[str, Any]:
    row = connection.execute(
        """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT code) AS codes,
               COUNT(DISTINCT date) AS snapshot_dates,
               SUM(CASE WHEN code IS NULL OR TRIM(code) = '' THEN 1 ELSE 0 END)
                   AS missing_codes,
               MIN(date) AS min_date,
               MAX(date) AS max_date
        FROM fundamentals
        """
    ).fetchone()
    categories = [
        dict(item)
        for item in connection.execute(
            """
            SELECT COALESCE(scalecat, 'NULL') AS scalecat, COUNT(*) AS codes
            FROM fundamentals
            GROUP BY scalecat
            ORDER BY codes DESC, scalecat
            """
        )
    ]
    max_date = row["max_date"]
    age_days = date_age_days(max_date, as_of)
    duplicate_codes = int(row["rows"] or 0) - int(row["codes"] or 0)
    issues: list[str] = []
    if not row["rows"]:
        issues.append("fundamentals is empty")
    if duplicate_codes:
        issues.append(f"fundamentals has {duplicate_codes} duplicate code row(s)")
    if row["missing_codes"]:
        issues.append(f"fundamentals has {row['missing_codes']} missing code row(s)")
    if age_days is None:
        issues.append("fundamentals max date is unavailable")
    elif age_days > max_age_days:
        issues.append(
            f"fundamentals snapshot age {age_days} days exceeds {max_age_days} days"
        )
    elif age_days < -7:
        issues.append("fundamentals snapshot is unexpectedly future-dated")

    return {
        "status": "fail" if issues else "pass",
        "issues": issues,
        "rows": int(row["rows"] or 0),
        "codes": int(row["codes"] or 0),
        "snapshot_dates": int(row["snapshot_dates"] or 0),
        "missing_codes": int(row["missing_codes"] or 0),
        "duplicate_codes": duplicate_codes,
        "min_date": row["min_date"],
        "max_date": max_date,
        "age_days": age_days,
        "max_age_days": max_age_days,
        "category_counts": categories,
    }


def audit_daily_price_coverage(
    connection: sqlite3.Connection,
    as_of: date,
    target_scalecats: tuple[str, ...] = TARGET_SCALECATS,
    min_coverage: float = DEFAULT_MIN_TARGET_PRICE_COVERAGE,
    max_age_days: int = DEFAULT_MAX_PRICE_AGE_DAYS,
) -> dict[str, Any]:
    latest_date = connection.execute("SELECT MAX(date) FROM prices").fetchone()[0]
    latest_age_days = date_age_days(latest_date, as_of)
    placeholders = _placeholders(target_scalecats)
    target_codes = int(
        connection.execute(
            f"SELECT COUNT(DISTINCT code) FROM fundamentals "
            f"WHERE scalecat IN ({placeholders})",
            target_scalecats,
        ).fetchone()[0]
    )
    covered_codes = int(
        connection.execute(
            f"""
            SELECT COUNT(*)
            FROM fundamentals f
            WHERE f.scalecat IN ({placeholders})
              AND EXISTS (
                  SELECT 1 FROM prices p
                  WHERE p.code = f.code AND p.date = ?
              )
            """,
            (*target_scalecats, latest_date),
        ).fetchone()[0]
    )
    missing_rows = connection.execute(
        f"""
        SELECT f.code, f.scalecat
        FROM fundamentals f
        WHERE f.scalecat IN ({placeholders})
          AND NOT EXISTS (
              SELECT 1 FROM prices p
              WHERE p.code = f.code AND p.date = ?
          )
        ORDER BY f.code
        """,
        (*target_scalecats, latest_date),
    ).fetchall()
    missing_codes: list[dict[str, Any]] = []
    for row in missing_rows:
        last_price_date = connection.execute(
            "SELECT MAX(date) FROM prices WHERE code = ?", (row["code"],)
        ).fetchone()[0]
        missing_codes.append(
            {
                "code": row["code"],
                "scalecat": row["scalecat"],
                "last_price_date": last_price_date,
                "lag_from_latest_days": (
                    date_age_days(last_price_date, parse_stored_date(latest_date))
                    if latest_date and last_price_date
                    else None
                ),
            }
        )

    coverage_pct = percentage(covered_codes, target_codes)
    issues: list[str] = []
    if latest_age_days is None:
        issues.append("latest price date is unavailable")
    elif latest_age_days > max_age_days:
        issues.append(
            f"latest price age {latest_age_days} days exceeds {max_age_days} days"
        )
    elif latest_age_days < 0:
        issues.append("latest price is future-dated")
    if coverage_pct is None:
        issues.append("configured target universe is empty")
    elif coverage_pct < min_coverage * 100:
        issues.append(
            f"target price coverage {coverage_pct:.2f}% is below "
            f"{min_coverage * 100:.2f}%"
        )

    return {
        "status": "fail" if issues else "pass",
        "issues": issues,
        "target_scalecats": list(target_scalecats),
        "latest_date": latest_date,
        "latest_age_days": latest_age_days,
        "max_age_days": max_age_days,
        "target_codes": target_codes,
        "covered_codes": covered_codes,
        "missing_code_count": len(missing_codes),
        "coverage_pct": coverage_pct,
        "minimum_coverage_pct": min_coverage * 100,
        "missing_codes": missing_codes,
    }


def audit_dividend_financials(
    connection: sqlite3.Connection,
    as_of: date,
    expected_delay_days: int = DEFAULT_EXPECTED_DISCLOSURE_DELAY_DAYS,
    delay_grace_days: int = DEFAULT_DISCLOSURE_DELAY_GRACE_DAYS,
    max_update_age_days: int = DEFAULT_MAX_DIVIDEND_UPDATE_AGE_DAYS,
) -> dict[str, Any]:
    fundamentals_codes = int(
        connection.execute("SELECT COUNT(DISTINCT code) FROM fundamentals").fetchone()[0]
    )
    row = connection.execute(
        """
        SELECT COUNT(*) AS rows,
               COUNT(DISTINCT code) AS codes,
               COUNT(DISTINCT CASE
                   WHEN forecast_dividend_per_share IS NOT NULL THEN code
               END) AS forecast_codes,
               MIN(disclosure_date) AS min_disclosure_date,
               MAX(disclosure_date) AS max_disclosure_date,
               MAX(updated_at) AS max_updated_at
        FROM dividend_financials
        """
    ).fetchone()
    missing_codes = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals f
            WHERE NOT EXISTS (
                SELECT 1 FROM dividend_financials d WHERE d.code = f.code
            )
            """
        ).fetchone()[0]
    )
    attempted_missing_codes = int(
        connection.execute(
            """
            SELECT COUNT(*)
            FROM fundamentals f
            WHERE NOT EXISTS (
                SELECT 1 FROM dividend_financials d WHERE d.code = f.code
            )
              AND EXISTS (
                  SELECT 1 FROM sync_progress sp
                  WHERE sp.table_name = ? || f.code
              )
            """,
            (DIVIDEND_SYNC_KEY_PREFIX,),
        ).fetchone()[0]
    )
    attempts = connection.execute(
        """
        SELECT COUNT(*) AS codes,
               MIN(last_synced_date) AS min_attempted_at,
               MAX(last_synced_date) AS max_attempted_at
        FROM sync_progress
        WHERE table_name LIKE ?
        """,
        (f"{DIVIDEND_SYNC_KEY_PREFIX}%",),
    ).fetchone()

    disclosure_lag_days = date_age_days(row["max_disclosure_date"], as_of)
    update_age_days = date_age_days(row["max_updated_at"], as_of)
    unattempted_missing_codes = missing_codes - attempted_missing_codes
    maximum_disclosure_lag = expected_delay_days + delay_grace_days
    issues: list[str] = []
    if not row["rows"]:
        issues.append("dividend_financials is empty")
    if update_age_days is None:
        issues.append("dividend refresh timestamp is unavailable")
    elif update_age_days > max_update_age_days:
        issues.append(
            f"dividend refresh age {update_age_days} days exceeds "
            f"{max_update_age_days} days"
        )
    elif update_age_days < 0:
        issues.append("dividend refresh timestamp is future-dated")
    if disclosure_lag_days is None:
        issues.append("latest dividend disclosure date is unavailable")
    elif disclosure_lag_days > maximum_disclosure_lag:
        issues.append(
            f"dividend disclosure lag {disclosure_lag_days} days exceeds "
            f"the expected delay plus grace ({maximum_disclosure_lag} days)"
        )
    if unattempted_missing_codes > 0:
        issues.append(
            f"{unattempted_missing_codes} code(s) without financial rows have "
            "no recorded collection attempt"
        )

    codes = int(row["codes"] or 0)
    forecast_codes = int(row["forecast_codes"] or 0)
    return {
        "status": "fail" if issues else "pass",
        "issues": issues,
        "rows": int(row["rows"] or 0),
        "codes": codes,
        "fundamentals_codes": fundamentals_codes,
        "coverage_pct": percentage(codes, fundamentals_codes),
        "forecast_codes": forecast_codes,
        "forecast_coverage_pct": percentage(forecast_codes, fundamentals_codes),
        "missing_codes": missing_codes,
        "attempted_missing_codes": attempted_missing_codes,
        "unattempted_missing_codes": unattempted_missing_codes,
        "collection_attempt_codes": int(attempts["codes"] or 0),
        "min_attempted_at": attempts["min_attempted_at"],
        "max_attempted_at": attempts["max_attempted_at"],
        "min_disclosure_date": row["min_disclosure_date"],
        "max_disclosure_date": row["max_disclosure_date"],
        "disclosure_lag_days": disclosure_lag_days,
        "expected_disclosure_delay_days": expected_delay_days,
        "disclosure_delay_grace_days": delay_grace_days,
        "max_updated_at": row["max_updated_at"],
        "update_age_days": update_age_days,
        "max_update_age_days": max_update_age_days,
    }


def build_audit(
    database: Path,
    as_of: date,
    *,
    max_master_age_days: int = DEFAULT_MAX_MASTER_AGE_DAYS,
    max_price_age_days: int = DEFAULT_MAX_PRICE_AGE_DAYS,
    min_target_price_coverage: float = DEFAULT_MIN_TARGET_PRICE_COVERAGE,
    expected_disclosure_delay_days: int = DEFAULT_EXPECTED_DISCLOSURE_DELAY_DAYS,
    disclosure_delay_grace_days: int = DEFAULT_DISCLOSURE_DELAY_GRACE_DAYS,
    max_dividend_update_age_days: int = DEFAULT_MAX_DIVIDEND_UPDATE_AGE_DAYS,
) -> dict[str, Any]:
    with closing(connect_read_only(database)) as connection:
        master = audit_master(connection, as_of, max_master_age_days)
        daily_prices = audit_daily_price_coverage(
            connection,
            as_of,
            min_coverage=min_target_price_coverage,
            max_age_days=max_price_age_days,
        )
        dividend = audit_dividend_financials(
            connection,
            as_of,
            expected_delay_days=expected_disclosure_delay_days,
            delay_grace_days=disclosure_delay_grace_days,
            max_update_age_days=max_dividend_update_age_days,
        )

    statuses = [master["status"], daily_prices["status"], dividend["status"]]
    return {
        "schema_version": "1.0",
        "as_of": as_of.isoformat(),
        "database": str(database.resolve()),
        "overall_status": "fail" if "fail" in statuses else "pass",
        "master": master,
        "daily_prices": daily_prices,
        "dividend_financials": dividend,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--as-of", type=date.fromisoformat, default=date.today())
    parser.add_argument(
        "--max-master-age-days",
        type=int,
        default=DEFAULT_MAX_MASTER_AGE_DAYS,
    )
    parser.add_argument(
        "--max-price-age-days",
        type=int,
        default=DEFAULT_MAX_PRICE_AGE_DAYS,
    )
    parser.add_argument(
        "--min-target-price-coverage",
        type=float,
        default=DEFAULT_MIN_TARGET_PRICE_COVERAGE,
    )
    parser.add_argument(
        "--expected-disclosure-delay-days",
        type=int,
        default=DEFAULT_EXPECTED_DISCLOSURE_DELAY_DAYS,
    )
    parser.add_argument(
        "--disclosure-delay-grace-days",
        type=int,
        default=DEFAULT_DISCLOSURE_DELAY_GRACE_DAYS,
    )
    parser.add_argument(
        "--max-dividend-update-age-days",
        type=int,
        default=DEFAULT_MAX_DIVIDEND_UPDATE_AGE_DAYS,
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    nonnegative = (
        args.max_master_age_days,
        args.max_price_age_days,
        args.expected_disclosure_delay_days,
        args.disclosure_delay_grace_days,
        args.max_dividend_update_age_days,
    )
    if any(value < 0 for value in nonnegative):
        raise ValueError("age and delay thresholds must be nonnegative")
    if not 0 < args.min_target_price_coverage <= 1:
        raise ValueError("--min-target-price-coverage must be in (0, 1]")


def main() -> int:
    args = parse_args()
    try:
        validate_args(args)
        audit = build_audit(
            args.database,
            args.as_of,
            max_master_age_days=args.max_master_age_days,
            max_price_age_days=args.max_price_age_days,
            min_target_price_coverage=args.min_target_price_coverage,
            expected_disclosure_delay_days=args.expected_disclosure_delay_days,
            disclosure_delay_grace_days=args.disclosure_delay_grace_days,
            max_dividend_update_age_days=args.max_dividend_update_age_days,
        )
    except Exception as error:
        print(
            json.dumps(
                {
                    "overall_status": "inspection_error",
                    "error": f"{type(error).__name__}: {error}",
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return 3

    print(json.dumps(audit, ensure_ascii=True, indent=2))
    return 0 if audit["overall_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
