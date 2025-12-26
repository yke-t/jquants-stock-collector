"""
BigQuery Export Script

stock_data.dbのデータをBigQueryにエクスポートするためのスクリプト。
1. SQLiteからDataFrameに読み込み
2. BigQueryに直接アップロード

前提条件:
- pip install google-cloud-bigquery pandas-gbq
- gcloud auth application-default login（認証済み）
- GCPプロジェクトでBigQuery APIを有効化
"""
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- Config ---
DB_PATH = Path(__file__).parent.parent / "stock_data.db"

# BigQuery設定（ユーザーが設定）
GCP_PROJECT_ID = "nisa-jquant"  # GCPプロジェクトID
BQ_DATASET = "stock_data"           # データセット名
BQ_TABLE_PRICES = "prices"          # 株価テーブル
BQ_TABLE_FUNDAMENTALS = "fundamentals"  # 銘柄マスタテーブル


def export_to_csv():
    """
    SQLiteからCSVにエクスポート（BigQueryへの手動アップロード用）
    """
    print("="*60)
    print(f"Export to CSV - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*60)
    
    conn = sqlite3.connect(DB_PATH)
    
    # prices テーブル
    print("[INFO] Exporting prices table...")
    df_prices = pd.read_sql("SELECT * FROM prices", conn)
    csv_prices = DB_PATH.parent / "export_prices.csv"
    df_prices.to_csv(csv_prices, index=False)
    print(f"[INFO] Exported {len(df_prices)} records to {csv_prices}")
    
    # fundamentals テーブル
    print("[INFO] Exporting fundamentals table...")
    df_fundamentals = pd.read_sql("SELECT * FROM fundamentals", conn)
    csv_fundamentals = DB_PATH.parent / "export_fundamentals.csv"
    df_fundamentals.to_csv(csv_fundamentals, index=False)
    print(f"[INFO] Exported {len(df_fundamentals)} records to {csv_fundamentals}")
    
    conn.close()
    
    print("="*60)
    print("[NEXT STEPS]")
    print("1. Upload CSVs to Cloud Storage:")
    print(f"   gsutil cp {csv_prices} gs://your-bucket/")
    print(f"   gsutil cp {csv_fundamentals} gs://your-bucket/")
    print("2. Load to BigQuery:")
    print(f"   bq load --autodetect {BQ_DATASET}.{BQ_TABLE_PRICES} gs://your-bucket/export_prices.csv")
    print(f"   bq load --autodetect {BQ_DATASET}.{BQ_TABLE_FUNDAMENTALS} gs://your-bucket/export_fundamentals.csv")
    print("="*60)


def export_to_bigquery():
    """
    SQLiteからBigQueryに直接エクスポート（pandas-gbq使用）
    """
    try:
        import pandas_gbq
    except ImportError:
        print("[ERROR] pandas-gbq not installed. Run: pip install pandas-gbq")
        return
    
    if GCP_PROJECT_ID == "your-project-id":
        print("[ERROR] Please set GCP_PROJECT_ID in this script.")
        return
    
    print("="*60)
    print(f"Export to BigQuery - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Project: {GCP_PROJECT_ID}")
    print(f"Dataset: {BQ_DATASET}")
    print("="*60)
    
    conn = sqlite3.connect(DB_PATH)
    
    # prices テーブル
    print("[INFO] Uploading prices table to BigQuery...")
    df_prices = pd.read_sql("SELECT * FROM prices", conn)
    
    # カラム型を明示的に指定
    df_prices['date'] = pd.to_datetime(df_prices['date']).dt.strftime('%Y-%m-%d')
    
    table_id = f"{BQ_DATASET}.{BQ_TABLE_PRICES}"
    pandas_gbq.to_gbq(
        df_prices,
        destination_table=table_id,
        project_id=GCP_PROJECT_ID,
        if_exists='replace',  # 既存テーブルを置換
        progress_bar=True
    )
    print(f"[INFO] Uploaded {len(df_prices)} records to {table_id}")
    
    # fundamentals テーブル
    print("[INFO] Uploading fundamentals table to BigQuery...")
    df_fundamentals = pd.read_sql("SELECT * FROM fundamentals", conn)
    
    table_id = f"{BQ_DATASET}.{BQ_TABLE_FUNDAMENTALS}"
    pandas_gbq.to_gbq(
        df_fundamentals,
        destination_table=table_id,
        project_id=GCP_PROJECT_ID,
        if_exists='replace',
        progress_bar=True
    )
    print(f"[INFO] Uploaded {len(df_fundamentals)} records to {table_id}")
    
    conn.close()
    
    print("="*60)
    print("[SUCCESS] Data exported to BigQuery!")
    print(f"[INFO] Query at: https://console.cloud.google.com/bigquery?project={GCP_PROJECT_ID}")
    print("="*60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--csv":
        # CSV出力モード
        export_to_csv()
    else:
        # BigQuery直接アップロードモード
        export_to_bigquery()
