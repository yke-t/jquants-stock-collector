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
from datetime import datetime
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
        "DivTotalAnn",
        "DivAnn",
        "DivFY",
        "ResultDividendPerShareAnnual",
        "DividendPerShare",
        "dividend_per_share",
    ],
    "forecast_dividend_per_share": [
        "FDivTotalAnn",
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

    return db.save_dividend_financials(normalized_rows)


def load_codes_from_db(
    db_path: str,
    limit: Optional[int] = None,
    missing_only: bool = False,
) -> List[str]:
    query = """
    SELECT DISTINCT f.code
    FROM fundamentals f
    WHERE f.code IS NOT NULL
    """
    if missing_only:
        query += """
        AND NOT EXISTS (
            SELECT 1
            FROM dividend_financials df
            WHERE df.code = f.code
        )
        """
    query += " ORDER BY f.code"
    if limit:
        query += f" LIMIT {int(limit)}"

    StockDatabase(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(query).fetchall()
    return [str(row[0]) for row in rows]


def collect_for_codes(
    client: JQuantsClient,
    db: StockDatabase,
    codes: List[str],
    sleep_seconds: float = 0.2,
) -> int:
    total_saved = 0
    for idx, code in enumerate(codes, start=1):
        try:
            saved = collect_financial_summary(client, db, code=code)
            total_saved += saved
            print(f"[{idx}/{len(codes)}] {code}: saved {saved}")
        except Exception as e:
            print(f"[WARN] {code}: {e}")
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return total_saved


def main():
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
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between code requests")
    parser.add_argument("--show-fields", action="store_true", help="Print response keys and first row fields")
    args = parser.parse_args()

    client = JQuantsClient()
    db = StockDatabase(args.db)

    if args.show_fields:
        response = client.get_financial_summary(code=args.code, date=args.date)
        rows = response_rows(response)
        print(f"[DEBUG] response keys: {sorted(response.keys())}")
        print(f"[DEBUG] row count: {len(rows)}")
        if rows:
            print(f"[DEBUG] first row fields: {sorted(rows[0].keys())}")
        return

    if args.all_codes:
        codes = load_codes_from_db(args.db, limit=args.limit, missing_only=args.missing_only)
        saved = collect_for_codes(client, db, codes, sleep_seconds=args.sleep)
    elif args.code or args.date:
        saved = collect_financial_summary(client, db, code=args.code, date=args.date)
    else:
        raise SystemExit("Specify --code, --date, or --all-codes. Use --limit with --all-codes for a trial run.")

    print(f"[DONE] Saved {saved} dividend financial rows")


if __name__ == "__main__":
    main()
