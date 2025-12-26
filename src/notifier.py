"""
Google Sheets Notifier Module

シグナルデータをGoogle Sheetsに書き込む通知モジュール。
サービスアカウント認証を使用。

Requirements:
    pip install gspread google-auth
"""
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import logging

# --- Config ---
# サービスアカウントキーのパス（プロジェクトルートに配置）
SECRET_KEY_PATH = Path(__file__).parent.parent / "secret_key.json"

# スプレッドシートの設定（後で書き換え可能）
SPREADSHEET_KEY = "YOUR_SPREADSHEET_KEY_HERE"  # ← スプレッドシートIDを設定
SHEET_NAME = "Signals"  # シート名

# Google Sheets APIのスコープ
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_sheets_client() -> Optional[gspread.Client]:
    """
    Google Sheets APIクライアントを取得する。
    
    Returns:
        gspread.Client: 認証済みクライアント
        None: 認証失敗時
    """
    if not SECRET_KEY_PATH.exists():
        logger.error(f"[NOTIFIER] Secret key not found: {SECRET_KEY_PATH}")
        logger.info("[NOTIFIER] Please place your service account key as 'secret_key.json' in project root.")
        return None
    
    try:
        credentials = Credentials.from_service_account_file(
            str(SECRET_KEY_PATH),
            scopes=SCOPES
        )
        client = gspread.authorize(credentials)
        logger.info("[NOTIFIER] Google Sheets authenticated successfully.")
        return client
    except Exception as e:
        logger.error(f"[NOTIFIER] Authentication failed: {e}")
        return None


def update_signal_sheet(signal_data: List[Dict]) -> bool:
    """
    シグナルデータをGoogle Sheetsに書き込む。
    
    Args:
        signal_data: シグナル情報のリスト
            各要素は以下のキーを持つ辞書:
            - code: 銘柄コード
            - name: 銘柄名
            - current_price: 現在値
            - ma25_rate: MA25乖離率(%)
            - stop_loss: 損切りライン
    
    Returns:
        bool: 成功時True、失敗時False
    """
    # スプレッドシートKeyが未設定の場合
    if SPREADSHEET_KEY == "YOUR_SPREADSHEET_KEY_HERE":
        logger.warning("[NOTIFIER] SPREADSHEET_KEY is not configured. Skipping Sheets update.")
        return False
    
    # クライアント取得
    client = get_sheets_client()
    if client is None:
        return False
    
    try:
        # スプレッドシートを開く
        spreadsheet = client.open_by_key(SPREADSHEET_KEY)
        
        # シートを取得（存在しない場合は作成）
        try:
            worksheet = spreadsheet.worksheet(SHEET_NAME)
            logger.info(f"[NOTIFIER] Sheet '{SHEET_NAME}' found.")
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"[NOTIFIER] Sheet '{SHEET_NAME}' not found. Creating new sheet...")
            worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=100, cols=10)
            logger.info(f"[NOTIFIER] Sheet '{SHEET_NAME}' created.")
        
        # 既存データをクリア
        worksheet.clear()
        logger.info("[NOTIFIER] Sheet cleared.")
        
        # ヘッダー行
        headers = [
            "更新日時",
            "銘柄コード",
            "銘柄名",
            "現在値",
            "MA25乖離率(%)",
            "損切りライン"
        ]
        
        # データ行を構築
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M')
        rows = [headers]  # ヘッダーを最初に追加
        
        for signal in signal_data:
            row = [
                update_time,
                str(signal.get('code', '')),
                str(signal.get('name', '')),
                signal.get('current_price', 0),
                round(signal.get('ma25_rate', 0), 2),
                signal.get('stop_loss', 0)
            ]
            rows.append(row)
        
        # 一括更新（A1からデータを書き込み）
        worksheet.update(range_name='A1', values=rows)
        logger.info(f"[NOTIFIER] Updated {len(signal_data)} signals to Google Sheets.")
        
        return True
        
    except gspread.exceptions.SpreadsheetNotFound:
        logger.error(f"[NOTIFIER] Spreadsheet not found: {SPREADSHEET_KEY}")
        logger.info("[NOTIFIER] Please check the SPREADSHEET_KEY and share the sheet with the service account.")
        return False
    except Exception as e:
        logger.error(f"[NOTIFIER] Failed to update sheet: {e}")
        return False


# --- テスト用 ---
if __name__ == "__main__":
    # テストデータ
    test_data = [
        {
            'code': '1234',
            'name': 'テスト株式会社',
            'current_price': 1000,
            'ma25_rate': -3.5,
            'stop_loss': 950
        },
        {
            'code': '5678',
            'name': 'サンプル工業',
            'current_price': 2500,
            'ma25_rate': -2.1,
            'stop_loss': 2375
        }
    ]
    
    print("[TEST] Running notifier test...")
    result = update_signal_sheet(test_data)
    print(f"[TEST] Result: {'Success' if result else 'Failed'}")
