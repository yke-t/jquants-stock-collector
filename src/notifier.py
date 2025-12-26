# src/notifier.py
"""
Google Sheets Notifier Module

シグナルデータをGoogle Sheetsに書き込む通知モジュール。
サービスアカウント認証を使用し、指定されたスプレッドシートのシートを更新します。
シートが存在しない場合は自動作成し、ヘッダーを設定します。
"""
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Any
import logging

# --- Configuration ---
# 認証キーはプロジェクトルートに配置することを想定
SECRET_KEY_PATH = Path(__file__).parent.parent / "secret_key.json"

# デフォルト設定（呼び出し元で上書き可能だが、基本はここを修正）
# 注意: 共有設定したスプレッドシートのIDをここに設定してください
SPREADSHEET_KEY = "1Hejm_UXA3xvn5rEXUhMkpHPtSjM2-foq-t1Su96gGYo"
SHEET_NAME = "Signals"

# API Scopes
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Logging
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

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
        sh = client.open_by_key(spreadsheet_key)
        
        # シートの取得または作成
        try:
            worksheet = sh.worksheet(SHEET_NAME)
        except gspread.WorksheetNotFound:
            logger.info(f"[NOTIFIER] Sheet '{SHEET_NAME}' not found. Creating new sheet...")
            worksheet = sh.add_worksheet(title=SHEET_NAME, rows=100, cols=10)

        # ヘッダー定義（利確目標を追加）
        header = ['更新日時', '銘柄コード', '銘柄名', '現在値', 'MA25乖離率(%)', '損切りライン', '利確目標(MA25)']
        
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
                item.get('take_profit', 0)  # 利確目標（MA25）
            ])
            
        # 既存データをクリアして書き込み
        worksheet.clear()
        
        if rows:
            # ヘッダー + データ
            worksheet.update(range_name='A1', values=[header] + rows)
            logger.info(f"[NOTIFIER] Successfully updated sheet with {len(rows)} signals.")
        else:
            # データが無い場合もヘッダーだけは残す
            worksheet.update(range_name='A1', values=[header, ["(No signals today)"]])
            logger.info("[NOTIFIER] No signals to report. Sheet cleared.")

        return True

    except Exception as e:
        logger.error(f"[NOTIFIER] Failed to update Google Sheets: {e}")
        return False

if __name__ == "__main__":
    # Test execution
    print("Testing notifier...")
    test_data = [
        {'code': '7203', 'name': 'Toyota', 'current_price': 2000, 'ma25_rate': -5.2, 'stop_loss': 1900}
    ]
    update_signal_sheet(test_data)
