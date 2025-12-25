"""
Phase 3: Daily Signal Scanner
毎日のデータ更新後に実行し、翌日のエントリー候補と市場環境を出力する。
"""
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# --- Config (Golden Configuration) ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
DIP_THRESHOLD = 0.97       # 押し目
MARKET_BULLISH_THRESHOLD = 0.40  # 市場環境フィルター

# フィルタリング設定
USE_SCALECAT_FILTER = True
SCALECAT_TARGETS = ['TOPIX Small 1', 'TOPIX Small 2', 'TOPIX Mid400']


def analyze_market():
    print("="*60)
    print(f"Snow Money Signal Scanner - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)

    # 1. DB接続 & データ読み込み（直近100日分で十分）
    db_path = DB_PATH
    if not db_path.exists():
        print(f"[ERROR] Database not found: {db_path}")
        return
        
    conn = sqlite3.connect(db_path)
    
    print("[INFO] Loading recent data...")
    
    # 日付取得のため、まずカレンダーを確認
    date_query = "SELECT DISTINCT date FROM prices ORDER BY date DESC LIMIT 100"
    dates = pd.read_sql(date_query, conn)['date']
    
    if dates.empty:
        print("[ERROR] No price data found.")
        conn.close()
        return
        
    start_date = dates.min()
    
    # データ取得 (fundamentalsのカラム名をDBに合わせて調整)
    if USE_SCALECAT_FILTER:
        scalecat_list = "', '".join(SCALECAT_TARGETS)
        query = f"""
        SELECT p.date, p.code, p.close, f.scalecat, f.coname as company_name
        FROM prices p
        LEFT JOIN fundamentals f ON p.code = f.code
        WHERE p.date >= '{start_date}'
        AND f.scalecat IN ('{scalecat_list}')
        ORDER BY p.date ASC
        """
    else:
        query = f"SELECT date, code, close FROM prices WHERE date >= '{start_date}' ORDER BY date ASC"
        
    df = pd.read_sql(query, conn, parse_dates=['date'])
    conn.close()
    
    if df.empty:
        print("[ERROR] No data found.")
        return

    latest_date = df['date'].max()
    print(f"[INFO] Latest Data: {latest_date.date()}")
    print(f"[INFO] Tracked Stocks: {df['code'].nunique()}")

    # 2. 指標計算
    print("[INFO] Calculating indicators...")
    df['ma_short'] = df.groupby('code')['close'].transform(lambda x: x.rolling(25).mean())
    df['ma_long'] = df.groupby('code')['close'].transform(lambda x: x.rolling(75).mean())
    
    # トレンド判定
    df['is_bullish'] = df['close'] > df['ma_long']
    df['gc_trend'] = df['ma_short'] > df['ma_long']
    
    # 3. 市場環境判定 (Market Regime)
    latest_df = df[df['date'] == latest_date].copy()
    
    n_stocks = len(latest_df)
    n_bullish = latest_df['is_bullish'].sum()
    market_sentiment = n_bullish / n_stocks if n_stocks > 0 else 0
    
    print(f"\n{'='*60}")
    print("MARKET REGIME ANALYSIS")
    print(f"{'='*60}")
    print(f"Tracked Stocks: {n_stocks}")
    print(f"Stocks > MA75:  {n_bullish} ({market_sentiment:.1%})")
    print(f"Threshold:      {MARKET_BULLISH_THRESHOLD:.0%}")
    print(f"{'='*60}")
    
    is_safe = market_sentiment >= MARKET_BULLISH_THRESHOLD
    
    if is_safe:
        print("[OK] CONDITION: GREEN (GO)")
        print("     Market is healthy. Hunting for dips.")
    else:
        print("[NG] CONDITION: RED (NO ENTRY)")
        print("     Market is weak. Cash is King. Do NOT buy new positions.")
        print(f"{'='*60}")
        return

    # 4. シグナル抽出
    latest_df['dip_ratio'] = latest_df['close'] / latest_df['ma_short']
    
    candidates = latest_df[
        (latest_df['gc_trend']) & 
        (latest_df['close'] < latest_df['ma_short'] * DIP_THRESHOLD)
    ].copy()
    
    print(f"\n{'='*60}")
    print("BUY CANDIDATES")
    print(f"{'='*60}")
    
    if candidates.empty:
        print("No candidates found today.")
    else:
        candidates = candidates.sort_values('dip_ratio')
        
        print(f"Found {len(candidates)} candidates:")
        print("-" * 60)
        print(f"{'Code':<8} {'Close':>12} {'MA25':>12} {'Dip':>10}")
        print("-" * 60)
        
        for _, row in candidates.head(20).iterrows():
            dip_pct = (row['dip_ratio'] - 1) * 100
            print(f"{row['code']:<8} {row['close']:>12,.0f} {row['ma_short']:>12,.0f} {dip_pct:>9.1f}%")
            
    print(f"{'='*60}")


if __name__ == "__main__":
    analyze_market()
