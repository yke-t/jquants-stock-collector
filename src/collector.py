"""
データ収集ロジック (V2 API対応)

JQuantsClientとStockDatabaseを組み合わせて、
大量の株価・財務データを効率的に収集する。
中断再開機能とtqdmによる進捗表示を含む。
"""

import pandas as pd
from datetime import datetime, timedelta
import time
from tqdm import tqdm


class DataCollector:
    """データ収集クラス (V2 API対応)"""
    
    def __init__(self, client, db):
        """
        コレクターを初期化
        
        Args:
            client: JQuantsClient インスタンス
            db: StockDatabase インスタンス
        """
        self.client = client
        self.db = db

    def run(self, start_date, end_date=None, resume=True):
        """
        指定日から現在までの全銘柄データを日次で取得して保存する
        
        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD, 省略時は今日)
            resume: Trueの場合、前回の続きから再開
        """
        
        # 1. 銘柄一覧（マスタ情報）を取得
        print("[INFO] Fetching listed companies...")
        try:
            res = self.client.get_listed_info()
            companies = res.get("data", [])
            if companies:
                df_info = pd.DataFrame(companies)
                # V2カラム名を小文字に統一
                df_info.columns = [c.lower() for c in df_info.columns]
                self.db.save_fundamentals(df_info)
                print(f"[INFO] Saved {len(df_info)} companies to fundamentals table")
        except Exception as e:
            print(f"[WARN] Failed to fetch listed info (skipping): {e}")

        # 2. 日付範囲の設定
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        
        # 中断再開: 前回の同期位置を取得
        if resume:
            last_synced = self.db.get_sync_progress("prices")
            if last_synced:
                resume_dt = datetime.strptime(last_synced, "%Y-%m-%d") + timedelta(days=1)
                if resume_dt > start_dt:
                    print(f"[INFO] Resuming from {resume_dt.strftime('%Y-%m-%d')}")
                    start_dt = resume_dt
        
        if start_dt > end_dt:
            print("[INFO] Already up to date!")
            return
        
        days_num = (end_dt - start_dt).days + 1
        
        print("=" * 50)
        print(f"[INFO] Fetching prices: {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")
        print(f"[INFO] Total days: {days_num}")
        print("=" * 50)

        # 3. 1日ずつループして取得
        total_records = 0
        for i in tqdm(range(days_num), desc="Fetching prices"):
            current_dt = start_dt + timedelta(days=i)
            
            # 土日はスキップ (5=土, 6=日)
            if current_dt.weekday() >= 5:
                continue

            date_str = current_dt.strftime("%Y-%m-%d")
            
            try:
                # V2 API: 日足データを取得
                res = self.client.get_daily_quotes(date=date_str)
                quotes = res.get("data", [])
                
                if not quotes:
                    # 祝日などでデータがない場合
                    continue

                # DataFrame化
                df = pd.DataFrame(quotes)
                
                # V2カラム名をDB用にマッピング
                column_mapping = {
                    "Date": "date",
                    "Code": "code",
                    "O": "open",
                    "H": "high",
                    "L": "low",
                    "C": "close",
                    "Vo": "volume",
                    "Va": "turnover",
                    "AdjFactor": "adjustmentfactor",
                    "AdjO": "adjustmentopen",
                    "AdjH": "adjustmenthigh",
                    "AdjL": "adjustmentlow",
                    "AdjC": "adjustmentclose",
                    "AdjVo": "adjustmentvolume",
                }
                df = df.rename(columns=column_mapping)
                
                # 必要なカラムのみ抽出
                available_cols = [c for c in column_mapping.values() if c in df.columns]
                df = df[available_cols]
                
                # データベースに保存
                count = self.db.save_daily_quotes(df)
                total_records += count
                
                # 進捗を更新
                self.db.update_sync_progress("prices", date_str)
                
                # レートリミット対策（1秒待機）
                time.sleep(1.0)
                
            except Exception as e:
                print(f"\n[ERROR] Failed to fetch {date_str}: {e}")
                print("[INFO] Progress saved. Run again to resume.")
                raise

        print("\n" + "=" * 50)
        print(f"[DONE] Fetched {total_records} price records")
        print(f"[DONE] Total records in DB: {self.db.get_price_count()}")
        print("=" * 50)