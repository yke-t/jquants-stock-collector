"""
Export local CSV reports as native Google Sheets files on Google Drive.

The script uses the same service-account credentials as src.notifier.
Set GOOGLE_DRIVE_FOLDER_ID to create files in a specific Drive folder.
"""

import argparse
import csv
import gspread
import os
import sys
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.notifier import (
    SPREADSHEET_KEY,
    call_google_api_with_retry,
    get_sheets_client,
)
from src.settings import (
    GOOGLE_DRIVE_EXPORT_SPREADSHEET_KEY,
    GOOGLE_DRIVE_FOLDER_ID,
    REPORTS_DIR as PROJECT_REPORTS_DIR,
)


REPORTS_DIR = PROJECT_REPORTS_DIR
EXPORT_SPREADSHEET_KEY = GOOGLE_DRIVE_EXPORT_SPREADSHEET_KEY or SPREADSHEET_KEY


def read_csv_rows(csv_path: Path) -> List[List[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


def latest_csv_by_prefix(reports_dir: Path, prefix: str) -> Optional[Path]:
    candidates = sorted(
        reports_dir.glob(f"{prefix}*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def default_title(csv_path: Path) -> str:
    return csv_path.stem


def worksheet_title(csv_path: Path) -> str:
    return csv_path.stem[:100]


def write_rows_to_worksheet(worksheet, rows: List[List[str]]) -> None:
    call_google_api_with_retry(worksheet.clear, "Clear export worksheet")
    call_google_api_with_retry(
        lambda: worksheet.resize(rows=len(rows), cols=max(len(row) for row in rows)),
        "Resize export worksheet",
    )
    call_google_api_with_retry(
        lambda: worksheet.update(range_name="A1", values=rows),
        "Update export worksheet",
    )


def export_csv_to_spreadsheet(
    csv_path: Path,
    title: Optional[str] = None,
    folder_id: Optional[str] = None,
) -> str:
    rows = read_csv_rows(csv_path)
    if not rows:
        raise ValueError(f"CSV is empty: {csv_path}")

    client = get_sheets_client()
    if not client:
        raise RuntimeError("Google Sheets client is not available")

    spreadsheet = client.create(title or default_title(csv_path), folder_id=folder_id)
    worksheet = spreadsheet.sheet1
    call_google_api_with_retry(
        lambda: worksheet.update_title("data"),
        "Rename export worksheet",
    )
    write_rows_to_worksheet(worksheet, rows)
    return spreadsheet.url


def export_csv_to_existing_spreadsheet(
    csv_path: Path,
    spreadsheet_key: str = EXPORT_SPREADSHEET_KEY,
    title: Optional[str] = None,
) -> str:
    rows = read_csv_rows(csv_path)
    if not rows:
        raise ValueError(f"CSV is empty: {csv_path}")

    client = get_sheets_client()
    if not client:
        raise RuntimeError("Google Sheets client is not available")

    spreadsheet = call_google_api_with_retry(
        lambda: client.open_by_key(spreadsheet_key),
        "Open export spreadsheet",
    )
    sheet_title = title or worksheet_title(csv_path)
    try:
        worksheet = call_google_api_with_retry(
            lambda: spreadsheet.worksheet(sheet_title),
            "Find export worksheet",
        )
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_title, rows=max(len(rows), 1), cols=max(len(row) for row in rows))

    write_rows_to_worksheet(worksheet, rows)
    return spreadsheet.url


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    load_dotenv()

    parser = argparse.ArgumentParser(description="Export a local CSV as a Google Sheets file")
    parser.add_argument("--csv", dest="csv_path", default=None, help="CSV path to export")
    parser.add_argument("--latest-prefix", default=None, help="Use latest reports/<prefix>*.csv")
    parser.add_argument("--title", default=None, help="Spreadsheet title")
    parser.add_argument("--as-tab", action="store_true", help="Write CSV to a tab in an existing spreadsheet")
    parser.add_argument(
        "--spreadsheet-key",
        default=EXPORT_SPREADSHEET_KEY,
        help="Existing spreadsheet key for --as-tab",
    )
    parser.add_argument(
        "--folder-id",
        default=GOOGLE_DRIVE_FOLDER_ID,
        help="Optional Google Drive folder ID",
    )
    args = parser.parse_args()

    if args.csv_path:
        csv_path = Path(args.csv_path)
    elif args.latest_prefix:
        csv_path = latest_csv_by_prefix(REPORTS_DIR, args.latest_prefix)
        if not csv_path:
            raise SystemExit(f"No CSV found for prefix: {args.latest_prefix}")
    else:
        raise SystemExit("Specify --csv or --latest-prefix")

    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    if args.as_tab:
        url = export_csv_to_existing_spreadsheet(
            csv_path,
            spreadsheet_key=args.spreadsheet_key,
            title=args.title,
        )
        print(f"[DRIVE] Updated spreadsheet tab: {url}")
    else:
        url = export_csv_to_spreadsheet(csv_path, title=args.title, folder_id=args.folder_id)
        print(f"[DRIVE] Created Google Sheet: {url}")


if __name__ == "__main__":
    main()
