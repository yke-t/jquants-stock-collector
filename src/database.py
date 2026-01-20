"""
SQLiteデータベース操作クラス

株価データと財務情報をSQLiteデータベースに保存する。
"""

import sqlite3
import os
from pathlib import Path


class StockDatabase:
    """SQLiteデータベース操作クラス"""
    
    def __init__(self, db_path="stock_data.db"):
        """
        データベースを初期化
        
        Args:
            db_path: SQLiteデータベースファイルのパス
        """
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """データベースとテーブルの初期化"""
        # ディレクトリが必要な場合のみ作成
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 株価テーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                date TEXT,
                code TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL,
                adjustmentfactor REAL,
                adjustmentopen REAL,
                adjustmenthigh REAL,
                adjustmentlow REAL,
                adjustmentclose REAL,
                adjustmentvolume REAL,
                PRIMARY KEY (date, code)
            )
        """)
        
        # 財務情報テーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                code TEXT,
                companyname TEXT,
                sector17code TEXT,
                sector17codename TEXT,
                sector33code TEXT,
                sector33codename TEXT,
                marketcode TEXT,
                marketcodename TEXT,
                updated_at TEXT,
                PRIMARY KEY (code)
            )
        """)
        
        # 同期進捗テーブル
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_progress (
                table_name TEXT PRIMARY KEY,
                last_synced_date TEXT
            )
        """)
        
        # インデックス
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_prices_code ON prices(code)")
        
        # シグナル履歴テーブル（評価機能用）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                signal_date TEXT,
                code TEXT,
                name TEXT,
                signal_price REAL,
                ma25_rate REAL,
                stop_loss REAL,
                take_profit REAL,
                verdict TEXT,
                reason TEXT,
                news_hit TEXT,
                PRIMARY KEY (signal_date, code)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signals_verdict ON signals(verdict)")
        
        conn.commit()
        conn.close()

    def save_daily_quotes(self, df):
        """日足データを保存（UPSERT）"""
        if df is None or df.empty:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        try:
            # INSERT OR REPLACE for upsert
            df.to_sql('prices', conn, if_exists='append', index=False)
            return len(df)
        except sqlite3.IntegrityError:
            # 重複キーの場合は無視
            return 0
        except Exception as e:
            print(f"[DB] Error saving prices: {e}")
            return 0
        finally:
            conn.close()

    def save_fundamentals(self, df):
        """財務情報を保存"""
        if df is None or df.empty:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        try:
            # 既存データを置き換え
            df.to_sql('fundamentals', conn, if_exists='replace', index=False)
            return len(df)
        except Exception as e:
            print(f"[DB] Error saving fundamentals: {e}")
            return 0
        finally:
            conn.close()

    def get_sync_progress(self, table_name):
        """同期進捗を取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT last_synced_date FROM sync_progress WHERE table_name = ?",
            (table_name,)
        )
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def update_sync_progress(self, table_name, synced_date):
        """同期進捗を更新"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO sync_progress (table_name, last_synced_date) VALUES (?, ?)",
            (table_name, synced_date)
        )
        conn.commit()
        conn.close()

    def get_price_count(self):
        """株価レコード数を取得"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prices")
        count = cursor.fetchone()[0]
        conn.close()
        return count

    def save_signals(self, signal_list, signal_date):
        """
        シグナルリストをDBに保存（UPSERT）
        
        Args:
            signal_list: シグナル辞書のリスト
            signal_date: シグナル発生日（YYYY-MM-DD形式）
        
        Returns:
            int: 保存したレコード数
        """
        if not signal_list:
            return 0
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved = 0
        for sig in signal_list:
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO signals 
                    (signal_date, code, name, signal_price, ma25_rate, stop_loss, take_profit, verdict, reason, news_hit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    signal_date,
                    str(sig.get('code', '')),
                    str(sig.get('name', '')),
                    sig.get('current_price', 0),
                    sig.get('ma25_rate', 0.0),
                    sig.get('stop_loss', 0),
                    sig.get('take_profit', 0),
                    str(sig.get('verdict', 'N/A')),
                    str(sig.get('reason', '')),
                    str(sig.get('news_hit', '') or '')
                ))
                saved += 1
            except Exception as e:
                print(f"[DB] Error saving signal {sig.get('code')}: {e}")
        
        conn.commit()
        conn.close()
        return saved

    def get_signals(self, start_date=None, end_date=None, verdict=None):
        """
        シグナル履歴を取得
        
        Args:
            start_date: 開始日（YYYY-MM-DD）
            end_date: 終了日（YYYY-MM-DD）
            verdict: フィルタする判定結果（ENTRY/WATCH/REJECT）
        
        Returns:
            list: シグナル辞書のリスト
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = "SELECT * FROM signals WHERE 1=1"
        params = []
        
        if start_date:
            query += " AND signal_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND signal_date <= ?"
            params.append(end_date)
        if verdict:
            query += " AND verdict = ?"
            params.append(verdict)
        
        query += " ORDER BY signal_date DESC, code ASC"
        
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(zip(columns, row)) for row in rows]