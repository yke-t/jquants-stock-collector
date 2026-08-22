"""
Collect financial summary data for the long-term dividend strategy.

This module stores a normalized subset of J-Quants /fins/summary data in
dividend_financials while preserving each source row as raw_json.
"""

import argparse
import json
import sys
import sqlite3
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.client import JQuantsClient
from src.database import StockDatabase
from src.settings import DATABASE_PATH


FIELD_ALIASES = {
    "code": ["Code", "code", "LocalCode"],
    "disclosure_date": [
        "DiscDate",
        "DisclosedDate",
        "DisclosureDate",
        "disclosure_date",
        "Date",
        "date",
    ],
    "fiscal_year": [
        "CurFYEn",
        "CurrentFiscalYearEndDate",
        "FiscalYear",
        "fiscal_year",
        "CurrentPeriodEndDate",
    ],
    "period": [
        "CurPerType",
        "DocType",
        "TypeOfCurrentPeriod",
        "TypeOfDocument",
        "period",
    ],
    "dividend_per_share": [
        "DivAnn",
        "DivFY",
        "ResultDividendPerShareAnnual",
        "DividendPerShare",
        "dividend_per_share",
    ],
    "forecast_dividend_per_share": [
        "FDivAnn",
        "FDivFY",
        "NxFDivAnn",
        "ForecastDividendPerShareAnnual",
        "ForecastDividendPerShare",
        "forecast_dividend_per_share",
    ],
    "eps": [
        "EPS",
        "NCEPS",
        "EarningsPerShare",
        "ResultEarningsPerShare",
        "eps",
    ],
    "forecast_eps": [
        "FEPS",
        "NxFEPS",
        "ForecastEarningsPerShare",
        "ForecastEPS",
        "forecast_eps",
    ],
    "profit": [
        "NP",
        "NCNP",
        "FNP",
        "FNCNP",
        "Profit",
        "ProfitAttributableToOwnersOfParent",
        "NetIncome",
        "profit",
    ],
    "equity": [
        "Eq",
        "NCEq",
        "Equity",
        "NetAssets",
        "equity",
    ],
    "total_assets": [
        "TA",
        "NCTA",
        "TotalAssets",
        "Assets",
        "total_assets",
    ],
}
DIVIDEND_SYNC_KEY_PREFIX = "dividend_financials:"


def dividend_sync_key(code: str) -> str:
    return f"{DIVIDEND_SYNC_KEY_PREFIX}{code}"


def first_present(row: Dict[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in row and row[name] not in ("", None, "-"):
            return row[name]
    return None


def to_float(value: Any) -> Optional[float]:
    if value in ("", None, "-"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def normalize_financial_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    code = first_present(row, FIELD_ALIASES["code"])
    disclosure_date = first_present(row, FIELD_ALIASES["disclosure_date"])
    if not code or not disclosure_date:
        return None

    normalized = {
        "code": str(code),
        "disclosure_date": str(disclosure_date),
        "fiscal_year": first_present(row, FIELD_ALIASES["fiscal_year"]),
        "period": first_present(row, FIELD_ALIASES["period"]),
        "dividend_per_share": to_float(first_present(row, FIELD_ALIASES["dividend_per_share"])),
        "forecast_dividend_per_share": to_float(first_present(row, FIELD_ALIASES["forecast_dividend_per_share"])),
        "eps": to_float(first_present(row, FIELD_ALIASES["eps"])),
        "forecast_eps": to_float(first_present(row, FIELD_ALIASES["forecast_eps"])),
        "profit": to_float(first_present(row, FIELD_ALIASES["profit"])),
        "equity": to_float(first_present(row, FIELD_ALIASES["equity"])),
        "total_assets": to_float(first_present(row, FIELD_ALIASES["total_assets"])),
        "raw_json": json.dumps(row, ensure_ascii=False, sort_keys=True),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    return normalized


def response_rows(response: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("data", "financials", "statements", "summary"):
        rows = response.get(key)
        if isinstance(rows, list):
            return rows
    return []


def collect_financial_summary(
    client: JQuantsClient,
    db: StockDatabase,
    code: Optional[str] = None,
    date: Optional[str] = None,
) -> int:
    response = client.get_financial_summary(code=code, date=date)
    rows = response_rows(response)
    normalized_rows: List[Dict[str, Any]] = []

    for row in rows:
        normalized = normalize_financial_row(row)
        if normalized:
            normalized_rows.append(normalized)

    saved = db.save_dividend_financials(normalized_rows)
    if saved != len(normalized_rows):
        raise RuntimeError(
            f"Saved {saved} of {len(normalized_rows)} normalized financial row(s)"
        )
    return saved


def load_codes_from_db(
    db_path: str,
    limit: Optional[int] = None,
    missing_only: bool = False,
    stale_before: Optional[str] = None,
) -> List[str]:
    if missing_only and stale_before:
        raise ValueError("missing_only and stale_before cannot be combined")

    params: List[Any] = []
    if missing_only or stale_before:
        query = """
        WITH code_refresh AS (
            SELECT
                f.code,
                MAX(NULLIF(df.updated_at, '')) AS last_updated,
                MAX(NULLIF(sp.last_synced_date, '')) AS last_attempted
            FROM fundamentals f
            LEFT JOIN dividend_financials df ON df.code = f.code
            LEFT JOIN sync_progress sp
              ON sp.table_name = ? || f.code
            WHERE f.code IS NOT NULL
            GROUP BY f.code
        ), refresh_order AS (
            SELECT
                code,
                last_updated,
                CASE
                    WHEN last_attempted IS NULL THEN last_updated
                    WHEN last_updated IS NULL THEN last_attempted
                    WHEN last_attempted >= last_updated THEN last_attempted
                    ELSE last_updated
                END AS last_refreshed
            FROM code_refresh
        )
        SELECT code, last_refreshed
        FROM refresh_order
        """
        params.append(DIVIDEND_SYNC_KEY_PREFIX)
        if missing_only:
            query += " WHERE last_updated IS NULL"
        else:
            query += """
            WHERE last_refreshed IS NULL OR last_refreshed < ?
            """
            params.append(str(stale_before))
        query += """
        ORDER BY
            CASE WHEN last_refreshed IS NULL THEN 0 ELSE 1 END,
            last_refreshed,
            code
        """
    else:
        query = """
        SELECT DISTINCT f.code
        FROM fundamentals f
        WHERE f.code IS NOT NULL
        ORDER BY f.code
        """
    if limit:
        query += f" LIMIT {int(limit)}"

    StockDatabase(db_path)
    with closing(sqlite3.connect(db_path)) as conn:
        rows = conn.execute(query, params).fetchall()
    return [str(row[0]) for row in rows]


def collect_for_codes(
    client: JQuantsClient,
    db: StockDatabase,
    codes: List[str],
    sleep_seconds: float = 0.2,
) -> int:
    total_saved = 0
    failures = []
    for idx, code in enumerate(codes, start=1):
        try:
            saved = collect_financial_summary(client, db, code=code)
            total_saved += saved
            db.update_sync_progress(
                dividend_sync_key(code),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            print(f"[{idx}/{len(codes)}] {code}: saved {saved}")
        except Exception as e:
            print(f"[WARN] {code}: {e}")
            failures.append(code)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    if failures:
        raise RuntimeError(
            f"Financial collection failed for {len(failures)} code(s): "
            f"{','.join(failures[:10])}"
        )
    return total_saved


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    load_dotenv()

    parser = argparse.ArgumentParser(description="Collect J-Quants financial summary for dividend scans")
    parser.add_argument("--db", default=str(DATABASE_PATH), help="Database file path")
    parser.add_argument("--code", default=None, help="Optional stock code")
    parser.add_argument("--date", default=None, help="Optional disclosure date (YYYY-MM-DD)")
    parser.add_argument("--all-codes", action="store_true", help="Collect by iterating codes from fundamentals")
    parser.add_argument("--limit", type=int, default=None, help="Limit codes when using --all-codes")
    parser.add_argument("--missing-only", action="store_true", help="Collect only codes missing from dividend_financials")
    parser.add_argument(
        "--stale-days",
        type=int,
        default=None,
        help="Collect missing codes and codes not refreshed within this many days",
    )
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between code requests")
    parser.add_argument("--show-fields", action="store_true", help="Print response keys and first row fields")
    args = parser.parse_args()

    if args.missing_only and args.stale_days is not None:
        parser.error("--missing-only and --stale-days cannot be combined")
    if args.stale_days is not None and args.stale_days < 0:
        parser.error("--stale-days must be zero or greater")

    client = JQuantsClient()
    db = StockDatabase(args.db)

    if args.show_fields:
        response = client.get_financial_summary(code=args.code, date=args.date)
        rows = response_rows(response)
        print(f"[DEBUG] response keys: {sorted(response.keys())}")
        print(f"[DEBUG] row count: {len(rows)}")
        if rows:
            print(f"[DEBUG] first row fields: {sorted(rows[0].keys())}")
        return 0

    if args.all_codes:
        stale_before = None
        if args.stale_days is not None:
            stale_before = (
                datetime.now() - timedelta(days=args.stale_days)
            ).strftime("%Y-%m-%d %H:%M:%S")
        codes = load_codes_from_db(
            args.db,
            limit=args.limit,
            missing_only=args.missing_only,
            stale_before=stale_before,
        )
        mode = (
            "missing-only" if args.missing_only
            else f"stale-before={stale_before}" if stale_before
            else "all-codes"
        )
        print(f"[INFO] Selected {len(codes)} code(s): {mode}")
        saved = collect_for_codes(client, db, codes, sleep_seconds=args.sleep)
    elif args.code or args.date:
        saved = collect_financial_summary(client, db, code=args.code, date=args.date)
    else:
        raise SystemExit("Specify --code, --date, or --all-codes. Use --limit with --all-codes for a trial run.")

    print(f"[DONE] Saved {saved} dividend financial rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
