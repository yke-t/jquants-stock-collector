# src/notifier.py
"""
Google Sheets Notifier Module

シグナルデータをGoogle Sheetsに書き込む通知モジュール。
サービスアカウント認証を使用し、指定されたスプレッドシートのシートを更新します。
シートが存在しない場合は自動作成し、ヘッダーを設定します。
"""
import gspread
import os
import time
import requests
from google.oauth2.service_account import Credentials
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any, Callable, TypeVar
import logging
from dotenv import load_dotenv

load_dotenv()

from src.settings import (
    GOOGLE_SERVICE_ACCOUNT_FILE,
    GOOGLE_SHEETS_SPREADSHEET_KEY,
)

# --- Configuration ---
# 認証キーはプロジェクトルートに配置することを想定
SECRET_KEY_PATH = GOOGLE_SERVICE_ACCOUNT_FILE
SPREADSHEET_KEY = GOOGLE_SHEETS_SPREADSHEET_KEY

def get_sheet_name() -> str:
    """日付入りのシート名を生成する（例: Signals_20260120）"""
    return f"Signals_{datetime.now().strftime('%Y%m%d')}"


def get_dividend_sheet_name() -> str:
    """日付入りの配当候補シート名を生成する。"""
    return f"Dividend_{datetime.now().strftime('%Y%m%d')}"

# API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TRANSIENT_GOOGLE_STATUS_CODES = {429, 500, 502, 503, 504}
GOOGLE_API_MAX_ATTEMPTS = 5
GOOGLE_API_INITIAL_DELAY_SECONDS = 2.0
T = TypeVar("T")


def google_api_status_code(error: Exception) -> Optional[int]:
    """Extract an HTTP status code from gspread/requests style errors."""
    response = getattr(error, "response", None)
    value = getattr(response, "status_code", None)
    if value is None:
        value = getattr(error, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def is_transient_google_error(error: Exception) -> bool:
    status_code = google_api_status_code(error)
    if status_code in TRANSIENT_GOOGLE_STATUS_CODES:
        return True
    return isinstance(
        error,
        (requests.exceptions.ConnectionError, requests.exceptions.Timeout),
    )


def call_google_api_with_retry(
    operation: Callable[[], T],
    operation_name: str,
    *,
    max_attempts: int = GOOGLE_API_MAX_ATTEMPTS,
    initial_delay_seconds: float = GOOGLE_API_INITIAL_DELAY_SECONDS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> T:
    """Retry only transient Google API failures with bounded backoff."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except Exception as error:
            if attempt >= max_attempts or not is_transient_google_error(error):
                raise
            delay = initial_delay_seconds * (2 ** (attempt - 1))
            status_code = google_api_status_code(error)
            reason = f"HTTP {status_code}" if status_code is not None else type(error).__name__
            logger.warning(
                "[NOTIFIER] %s failed with %s; retry %d/%d in %.1f seconds.",
                operation_name,
                reason,
                attempt + 1,
                max_attempts,
                delay,
            )
            sleep_fn(delay)

    raise AssertionError("unreachable")

def get_sheets_client() -> Optional[gspread.Client]:
    """Google Sheets APIクライアントを認証・取得する"""
    if not SECRET_KEY_PATH.exists():
        logger.error(f"[NOTIFIER] Secret key not found at: {SECRET_KEY_PATH}")
        logger.error("[NOTIFIER] Please ensure 'secret_key.json' is placed in the project root.")
        return None
    
    try:
        creds = Credentials.from_service_account_file(str(SECRET_KEY_PATH), scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"[NOTIFIER] Authentication failed: {e}")
        return None

def update_signal_sheet(signal_data: List[Dict[str, Any]], spreadsheet_key: str = SPREADSHEET_KEY) -> bool:
    """
    シグナルリストをスプレッドシートに上書き保存する。
    
    Args:
        signal_data: 書き込むデータのリスト。各辞書は以下のキーを持つことを期待:
                     ['code', 'name', 'current_price', 'ma25_rate', 'stop_loss']
        spreadsheet_key: 対象のスプレッドシートID
        
    Returns:
        bool: 更新成功ならTrue
    """
    if spreadsheet_key == "YOUR_SPREADSHEET_ID_HERE" or not spreadsheet_key:
        logger.warning("[NOTIFIER] Spreadsheet Key is not configured.")
        return False

    client = get_sheets_client()
    if not client:
        return False

    try:
        # スプレッドシートを開く
        sh = call_google_api_with_retry(
            lambda: client.open_by_key(spreadsheet_key),
            "Open signal spreadsheet",
        )
        
        # シートの取得または作成（日付入りシート名）
        sheet_name = get_sheet_name()
        try:
            worksheet = call_google_api_with_retry(
                lambda: sh.worksheet(sheet_name),
                "Find signal worksheet",
            )
        except gspread.WorksheetNotFound:
            logger.info(f"[NOTIFIER] Sheet '{sheet_name}' not found. Creating new sheet...")
            worksheet = sh.add_worksheet(title=sheet_name, rows=100, cols=10)

        # ヘッダー定義（判定結果を追加）
        header = ['更新日時', '銘柄コード', '銘柄名', '現在値', 'MA25乖離率(%)', '損切りライン', '利確目標(MA25)', '判定結果', '判定理由', 'News Hit']
        
        # データ行の生成
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        rows = []
        
        for item in signal_data:
            rows.append([
                current_time,
                str(item.get('code', '')),
                str(item.get('name', '')),
                item.get('current_price', 0),
                item.get('ma25_rate', 0.0),
                item.get('stop_loss', 0),
                item.get('take_profit', 0),  # 利確目標（MA25）
                str(item.get('verdict', 'N/A')),  # 判定結果
                str(item.get('reason', '')),  # 判定理由
                str(item.get('news_hit', '') or '')  # News Hit
            ])
            
        # 既存データをクリアして書き込み
        call_google_api_with_retry(worksheet.clear, "Clear signal worksheet")
        
        if rows:
            # ヘッダー + データ
            call_google_api_with_retry(
                lambda: worksheet.update(range_name='A1', values=[header] + rows),
                "Update signal worksheet",
            )
            logger.info(f"[NOTIFIER] Successfully updated sheet with {len(rows)} signals.")
        else:
            # データが無い場合もヘッダーだけは残す
            call_google_api_with_retry(
                lambda: worksheet.update(
                    range_name='A1',
                    values=[header, ["(No signals today)"]],
                ),
                "Update empty signal worksheet",
            )
            logger.info("[NOTIFIER] No signals to report. Sheet cleared.")

        return True

    except Exception as e:
        logger.error(f"[NOTIFIER] Failed to update Google Sheets: {e}")
        return False


def update_dividend_candidate_sheet(
    candidates: List[Dict[str, Any]],
    spreadsheet_key: str = SPREADSHEET_KEY,
) -> bool:
    """長期配当候補を専用シートに上書き保存する。"""
    if spreadsheet_key == "YOUR_SPREADSHEET_ID_HERE" or not spreadsheet_key:
        logger.warning("[NOTIFIER] Spreadsheet Key is not configured.")
        return False

    client = get_sheets_client()
    if not client:
        return False

    try:
        sh = call_google_api_with_retry(
            lambda: client.open_by_key(spreadsheet_key),
            "Open dividend spreadsheet",
        )
        sheet_name = get_dividend_sheet_name()
        try:
            worksheet = call_google_api_with_retry(
                lambda: sh.worksheet(sheet_name),
                "Find dividend worksheet",
            )
        except gspread.WorksheetNotFound:
            logger.info(f"[NOTIFIER] Sheet '{sheet_name}' not found. Creating new sheet...")
            worksheet = sh.add_worksheet(title=sheet_name, rows=200, cols=20)

        header = [
            '更新日時', '日付', '銘柄コード', '銘柄名', '現在値',
            '配当利回り(%)', '配当性向(%)', '自己資本比率(%)',
            '20日平均出来高', 'MA75', 'MA200', '判定結果', 'スコア',
            '判定理由', 'ニュースリスク', 'News Hit', '開示日',
            '期間', '業種', '市場'
        ]

        current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        rows = []
        for item in candidates:
            rows.append([
                current_time,
                str(item.get('date', '')),
                str(item.get('code', '')),
                str(item.get('name', '')),
                item.get('close', ''),
                item.get('dividend_yield', ''),
                item.get('payout_ratio', ''),
                item.get('equity_ratio', ''),
                item.get('avg_volume_20', ''),
                item.get('ma75', ''),
                item.get('ma200', ''),
                str(item.get('verdict', '')),
                item.get('score', ''),
                str(item.get('reason', '')),
                str(item.get('news_risk', '')),
                str(item.get('news_hit', '')),
                str(item.get('disclosure_date', '')),
                str(item.get('period', '')),
                str(item.get('s33nm', '')),
                str(item.get('mktnm', '')),
            ])

        call_google_api_with_retry(worksheet.clear, "Clear dividend worksheet")
        if rows:
            call_google_api_with_retry(
                lambda: worksheet.update(range_name='A1', values=[header] + rows),
                "Update dividend worksheet",
            )
            logger.info(f"[NOTIFIER] Successfully updated dividend sheet with {len(rows)} rows.")
        else:
            call_google_api_with_retry(
                lambda: worksheet.update(
                    range_name='A1',
                    values=[header, ["(No dividend candidates)"]],
                ),
                "Update empty dividend worksheet",
            )
            logger.info("[NOTIFIER] No dividend candidates to report.")

        return True

    except Exception as e:
        logger.error(f"[NOTIFIER] Failed to update dividend Google Sheet: {e}")
        return False

if __name__ == "__main__":
    # Test execution
    print("Testing notifier...")
    test_data = [
        {'code': '7203', 'name': 'Toyota', 'current_price': 2000, 'ma25_rate': -5.2, 'stop_loss': 1900}
    ]
    update_signal_sheet(test_data)
