# -*- coding: utf-8 -*-
"""
Read signals from Google Sheets and test news_analyzer
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

import gspread
from google.oauth2.service_account import Credentials
from src.news_analyzer import batch_analyze, get_nikkei_change

# --- Config ---
SECRET_KEY_PATH = Path(__file__).parent.parent / "secret_key.json"
SPREADSHEET_KEY = "1Hejm_UXA3xvn5rEXUhMkpHPtSjM2-foq-t1Su96gGYo"
SHEET_NAME = "Signals_20260105"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def main():
    print("="*60)
    print("Testing News Analyzer with Real Signals")
    print("="*60)
    
    # 1. Google Sheets認証
    print("[INFO] Connecting to Google Sheets...")
    creds = Credentials.from_service_account_file(str(SECRET_KEY_PATH), scopes=SCOPES)
    client = gspread.authorize(creds)
    
    # 2. シートからデータ取得
    try:
        sh = client.open_by_key(SPREADSHEET_KEY)
        worksheet = sh.worksheet(SHEET_NAME)
        records = worksheet.get_all_records()
        print(f"[INFO] Loaded {len(records)} signals from {SHEET_NAME}")
    except Exception as e:
        print(f"[ERROR] Failed to load sheet: {e}")
        return
    
    if not records:
        print("[WARN] No signals found.")
        return
    
    # 3. シグナルデータを変換
    signals = []
    for row in records[:10]:  # 最大10銘柄
        code = str(row.get('銘柄コード', row.get('code', '')))
        name = str(row.get('銘柄名', row.get('name', '')))
        ma25_rate = row.get('MA25乖離率(%)', row.get('ma25_rate', 0))
        
        if code and name:
            signals.append({
                'code': code,
                'name': name,
                'dip_pct': float(ma25_rate) if ma25_rate else -3.0
            })
    
    print(f"[INFO] Testing {len(signals)} signals:")
    for s in signals:
        print(f"  - {s['code']}: {s['name']} ({s['dip_pct']:+.1f}%)")
    
    # 4. ニュース分析実行
    print("\n[NEWS] Running analysis...")
    results = batch_analyze(signals)
    
    # 5. 結果表示
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    
    for r in results:
        verdict = r.get('verdict', 'N/A')
        reason = r.get('reason', '')
        news = r.get('news_hit', '')
        
        # 色分け表示
        if verdict == 'ENTRY':
            marker = '[O]'
        elif verdict == 'WATCH':
            marker = '[?]'
        else:
            marker = '[X]'
        
        print(f"{marker} {r['code']}: {verdict}")
        print(f"    Reason: {reason}")
        if news:
            print(f"    News: {news}")
    
    # 6. サマリー
    entry_count = sum(1 for r in results if r.get('verdict') == 'ENTRY')
    watch_count = sum(1 for r in results if r.get('verdict') == 'WATCH')
    reject_count = sum(1 for r in results if r.get('verdict') == 'REJECT')
    
    print("\n" + "="*60)
    print(f"SUMMARY: ENTRY={entry_count}, WATCH={watch_count}, REJECT={reject_count}")
    print("="*60)

if __name__ == "__main__":
    main()
