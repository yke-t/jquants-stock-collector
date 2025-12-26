"""
BigQuery Daily Sync Script

stock_data.dbの直近データをBigQueryに差分同期する。
全データを上書きするのではなく、直近N日分のみ追加/更新する。
"""
import sqlite3
import pandas as pd
from google.cloud import bigquery
from pathlib import Path
from datetime import datetime, timedelta

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"
GCP_PROJECT_ID = "nisa-jquant"
BQ_DATASET = "stock_data"
BQ_TABLE_PRICES = "prices"
SYNC_DAYS = 7  # 直近7日分を同期


def get_recent_data(db_path: Path, days: int) -> pd.DataFrame:
    """SQLiteから直近N日分のデータを取得"""
    conn = sqlite3.connect(db_path)
    
    # 直近の日付を取得
    date_query = f"""
    SELECT DISTINCT date FROM prices
    ORDER BY date DESC
    LIMIT {days}
    """
    dates = pd.read_sql(date_query, conn)['date'].tolist()
    
    if not dates:
        conn.close()
        return pd.DataFrame()
    
    # 該当日のデータを取得
    dates_str = "', '".join(dates)
    query = f"""
    SELECT * FROM prices
    WHERE date IN ('{dates_str}')
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    return df


def sync_to_bigquery(df: pd.DataFrame) -> int:
    """BigQueryに差分データをMERGE（UPSERT）"""
    if df.empty:
        return 0
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_PRICES}"
    
    # 一時テーブルにアップロード
    temp_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}._temp_prices_sync"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()  # 完了を待つ
    
    # MERGEクエリで差分更新
    merge_query = f"""
    MERGE `{table_id}` AS target
    USING `{temp_table_id}` AS source
    ON target.date = source.date AND target.code = source.code
    WHEN MATCHED THEN
        UPDATE SET
            open = source.open,
            high = source.high,
            low = source.low,
            close = source.close,
            volume = source.volume,
            turnover = source.turnover,
            adjustmentfactor = source.adjustmentfactor,
            adjustmentopen = source.adjustmentopen,
            adjustmenthigh = source.adjustmenthigh,
            adjustmentlow = source.adjustmentlow,
            adjustmentclose = source.adjustmentclose,
            adjustmentvolume = source.adjustmentvolume
    WHEN NOT MATCHED THEN
        INSERT (date, code, open, high, low, close, volume, turnover,
                adjustmentfactor, adjustmentopen, adjustmenthigh, 
                adjustmentlow, adjustmentclose, adjustmentvolume)
        VALUES (source.date, source.code, source.open, source.high, 
                source.low, source.close, source.volume, source.turnover,
                source.adjustmentfactor, source.adjustmentopen, source.adjustmenthigh,
                source.adjustmentlow, source.adjustmentclose, source.adjustmentvolume)
    """
    
    query_job = client.query(merge_query)
    result = query_job.result()
    
    # 一時テーブルを削除
    client.delete_table(temp_table_id, not_found_ok=True)
    
    return len(df)


def run_daily_sync():
    """日次同期を実行"""
    print("="*60)
    print(f"BigQuery Daily Sync - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Project: {GCP_PROJECT_ID}")
    print(f"Sync Period: Last {SYNC_DAYS} days")
    print("="*60)
    
    if not DB_PATH.exists():
        print(f"[ERROR] Database not found: {DB_PATH}")
        return
    
    # 1. 直近データを取得
    print(f"[INFO] Loading recent {SYNC_DAYS} days from SQLite...")
    df = get_recent_data(DB_PATH, SYNC_DAYS)
    
    if df.empty:
        print("[WARN] No recent data found.")
        return
    
    # 日付範囲を表示
    dates = sorted(df['date'].unique())
    print(f"[INFO] Date range: {dates[0]} to {dates[-1]}")
    print(f"[INFO] Records to sync: {len(df)}")
    
    # 2. BigQueryに同期
    print("[INFO] Syncing to BigQuery (MERGE)...")
    count = sync_to_bigquery(df)
    print(f"[INFO] Synced {count} records")
    
    print("="*60)
    print("[SUCCESS] Daily sync completed!")
    print("="*60)


if __name__ == "__main__":
    run_daily_sync()
