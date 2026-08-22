"""Shared project configuration loaded from the repository-level .env file."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def resolve_project_path(value: str | os.PathLike[str]) -> Path:
    """Resolve relative configuration paths from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else PROJECT_ROOT / path


DATABASE_PATH = resolve_project_path(os.getenv("DATABASE_PATH", "stock_data.db"))
REPORTS_DIR = resolve_project_path(os.getenv("REPORTS_DIR", "reports"))
CHARTS_DIR = resolve_project_path(os.getenv("CHARTS_DIR", "charts"))
GOOGLE_SERVICE_ACCOUNT_FILE = resolve_project_path(
    os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "secret_key.json")
)

JQUANTS_API_KEY = os.getenv("JQUANTS_API_KEY") or os.getenv("JQUANTS_REFRESH_TOKEN")
GOOGLE_SHEETS_SPREADSHEET_KEY = os.getenv("GOOGLE_SHEETS_SPREADSHEET_KEY", "")
GOOGLE_DRIVE_EXPORT_SPREADSHEET_KEY = os.getenv(
    "GOOGLE_DRIVE_EXPORT_SPREADSHEET_KEY",
    GOOGLE_SHEETS_SPREADSHEET_KEY,
)
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID", "")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "nisa-jquant")
BQ_DATASET = os.getenv("BQ_DATASET", "stock_data")
BQ_TABLE_PRICES = os.getenv("BQ_TABLE_PRICES", "prices")
BQ_TABLE_FUNDAMENTALS = os.getenv("BQ_TABLE_FUNDAMENTALS", "fundamentals")
