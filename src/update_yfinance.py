"""
yfinance日次データ更新スクリプト

J-Quants解約後、yfinanceを使用して日次株価データを更新する。
既存のDBに追記する形で動作。
"""
import argparse
import sqlite3
import pandas as pd
import yfinance as yf
from pathlib import Path
from datetime import datetime, timedelta
from tqdm import tqdm
import time

from src.settings import DATABASE_PATH

# --- Config ---
DB_PATH = DATABASE_PATH
BATCH_SIZE = 20  # 一度に取得する銘柄数（レートリミット対策）
WAIT_BETWEEN_BATCHES = 3  # バッチ間の待機秒数

# Known security-code transitions. Rows after the cutoff belong to the successor
# security and must never be stored under the predecessor J-Quants code.
PRICE_CODE_CUTOFFS = {
    "44490": "2026-06-29",  # giftee -> giftee Group (590A0)
}


def get_target_codes(db_path: Path) -> list:
    """fundamentalsテーブルから対象銘柄コードを取得"""
    conn = sqlite3.connect(db_path)
    query = """
    SELECT DISTINCT code FROM fundamentals
    WHERE scalecat IN ('TOPIX Small 1', 'TOPIX Small 2', 'TOPIX Mid400')
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df['code'].tolist()


def convert_to_yfinance_ticker(code: str) -> str:
    """
    J-Quantsの銘柄コード（5桁）をyfinance形式（4桁.T）に変換
    例: 72030 -> 7203.T, 6758 -> 6758.T
    """
    code_str = str(code)
    # 5桁で末尾が0の場合、4桁に変換
    if len(code_str) == 5 and code_str.endswith('0'):
        return f"{code_str[:-1]}.T"
    # それ以外はそのまま
    return f"{code_str}.T"


def fetch_single_stock(ticker: str, code: str, start_date: str, end_date: str) -> list:
    """単一銘柄のデータを取得"""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(
            start=start_date,
            end=end_date,
            auto_adjust=False,
            actions=True,
        )
        
        if hist.empty:
            return []
        
        results = []
        for date_idx, row in hist.iterrows():
            price_date = date_idx.strftime('%Y-%m-%d')
            cutoff = PRICE_CODE_CUTOFFS.get(str(code))
            if cutoff is not None and price_date > cutoff:
                continue
            split_ratio = float(row.get('Stock Splits', 0) or 0)
            adjustment_factor = (1.0 / split_ratio) if split_ratio > 0 else None
            results.append({
                'date': price_date,
                'code': code,
                'open': float(row.get('Open', 0)),
                'high': float(row.get('High', 0)),
                'low': float(row.get('Low', 0)),
                'close': float(row.get('Close', 0)),
                'volume': int(row.get('Volume', 0)),
                'adjustmentfactor': adjustment_factor,
            })
        return results
    except Exception as exc:
        raise RuntimeError(f"yfinance fetch failed for {code}: {exc}") from exc


def fetch_yfinance_data(codes: list, start_date: str, end_date: str) -> pd.DataFrame:
    """
    yfinanceから株価データを取得（1銘柄ずつ安全に取得）
    
    Args:
        codes: 銘柄コードリスト（J-Quantsの5桁形式）
        start_date: 開始日 (YYYY-MM-DD)
        end_date: 終了日 (YYYY-MM-DD)
    
    Returns:
        DataFrame with columns: date, code, open, high, low, close, volume
    """
    all_data = []
    success_count = 0
    
    for i, code in enumerate(tqdm(codes, desc="Fetching")):
        ticker = convert_to_yfinance_ticker(code)
        results = fetch_single_stock(ticker, code, start_date, end_date)
        
        if results:
            all_data.extend(results)
            success_count += 1
        
        # レートリミット対策
        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(WAIT_BETWEEN_BATCHES)
    
    print(f"[INFO] Successfully fetched {success_count}/{len(codes)} stocks")
    return pd.DataFrame(all_data)


def update_database(df: pd.DataFrame, db_path: Path) -> int:
    """
    DBにデータを追加（UPSERT）
    
    Returns:
        追加されたレコード数
    """
    if df.empty:
        return 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    count = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO prices
                    (date, code, open, high, low, close, volume, adjustmentfactor)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, code) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    adjustmentfactor = COALESCE(
                        excluded.adjustmentfactor,
                        prices.adjustmentfactor
                    )
            """, (
                row['date'],
                row['code'],
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume'],
                row.get('adjustmentfactor'),
            ))
            count += 1
        except Exception:
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    return count


def normalize_requested_codes(codes: list[str]) -> list[str]:
    """Normalize an explicit catch-up list while preserving input order."""
    normalized = []
    seen = set()
    for code in codes:
        value = str(code).strip().upper()
        if not value:
            raise ValueError("Stock code must not be blank")
        if value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def run_daily_update(
    requested_codes: list[str] | None = None,
    lookback_days: int = 7,
) -> int:
    """日次更新を実行"""
    print("="*60)
    print(f"yfinance Daily Update - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)
    
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found: {DB_PATH}")
        return 1
    
    # 1. 対象銘柄を取得
    print("[INFO] Loading target codes from DB...")
    if requested_codes is None:
        codes = get_target_codes(DB_PATH)
        selection = "configured strategy universe"
    else:
        codes = normalize_requested_codes(requested_codes)
        selection = "explicit catch-up list"
    print(f"[INFO] Target stocks: {len(codes)} ({selection})")
    
    if not codes:
        print("[ERROR] No target codes found.")
        return 1
    
    # 2. 取得期間を設定（直近5営業日分を更新）
    end_date = datetime.now()
    start_date = end_date - timedelta(days=lookback_days)
    
    print(f"[INFO] Fetching: {start_date.strftime('%Y-%m-%d')} -> {end_date.strftime('%Y-%m-%d')}")
    
    # 3. yfinanceからデータ取得
    df = fetch_yfinance_data(
        codes,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    print(f"[INFO] Fetched {len(df)} records")
    
    # 4. DBに保存
    if not df.empty:
        count = update_database(df, DB_PATH)
        print(f"[INFO] Updated {count} records in database")
        if count != len(df):
            print(f"[ERROR] Updated {count} of {len(df)} fetched records")
            return 1
    else:
        print("[ERROR] No data fetched")
        return 1
    
    print("="*60)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch recent prices from yfinance and update SQLite."
    )
    parser.add_argument(
        "--code",
        action="append",
        dest="codes",
        help="Fetch only this J-Quants code; repeat for multiple codes",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Calendar-day lookback for the yfinance request (default: 7)",
    )
    args = parser.parse_args()
    if args.lookback_days <= 0:
        parser.error("--lookback-days must be positive")
    return args


if __name__ == "__main__":
    cli_args = parse_args()
    raise SystemExit(
        run_daily_update(cli_args.codes, lookback_days=cli_args.lookback_days)
    )
