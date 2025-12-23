# J-Quants 日本株データ収集スクリプト 実装計画

## 概要
J-Quants API (Premium Plan) を使用して、過去10年分（2014年〜現在）の日本株データをSQLiteデータベースに保存するPythonスクリプトを作成します。

## 技術選定の理由（Zero-Toil観点）

| 選択 | 理由 |
|------|------|
| **jquants-api-client** | 公式ライブラリ。認証・ページネーション・レートリミットを内部で処理。自前でrequests直書きは車輪の再発明（＝Toil）。 |
| **SQLite** | ローカル環境で動作、セットアップ不要。バックテスト用途に最適。 |
| **tqdm** | 進捗表示。中断時の再開位置特定に有用。 |

---

## 提案するディレクトリ構成

```
C:\Users\yke\Projects\jquants-stock-collector\
├── main.py              # エントリーポイント
├── src/
│   ├── __init__.py
│   ├── client.py        # JQuantsClient クラス
│   ├── database.py      # StockDatabase クラス
│   └── collector.py     # DataCollector クラス
├── requirements.txt
├── .env.example
└── README.md
```

---

## 提案するコード構成

### [NEW] [requirements.txt](file:///C:/Users/yke/Projects/jquants-stock-collector/requirements.txt)

必要なPythonパッケージを定義。

```text
jquants-api-client>=2.0.0
pandas>=2.0.0
python-dotenv>=1.0.0
tqdm>=4.65.0
```

---

### [NEW] [client.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/client.py)

`jquants-api-client` ライブラリをラップし、認証と銘柄一覧取得を提供するクラス。

**主要メソッド:**
- `__init__(mail_address, password)` - 認証情報でクライアントを初期化
- `get_listed_stocks()` → `pd.DataFrame` - 全上場銘柄コード一覧を取得
- `get_price_range(start_dt, end_dt)` → `pd.DataFrame` - 期間指定で株価取得
- `get_statements_range(start_dt, end_dt)` → `pd.DataFrame` - 期間指定で財務情報取得

```python
import jquantsapi
from datetime import datetime
from dateutil import tz
import pandas as pd

class JQuantsClient:
    """J-Quants APIクライアントラッパー"""
    
    def __init__(self, mail_address: str, password: str):
        self.client = jquantsapi.Client(
            mail_address=mail_address,
            password=password
        )
        self.tz = tz.gettz("Asia/Tokyo")
    
    def get_listed_stocks(self) -> pd.DataFrame:
        """全上場銘柄一覧を取得"""
        return self.client.get_list()
    
    def get_price_range(self, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
        """期間指定で株価データを取得"""
        return self.client.get_price_range(
            start_dt=start_dt.replace(tzinfo=self.tz),
            end_dt=end_dt.replace(tzinfo=self.tz)
        )
```

---

### [NEW] [database.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/database.py)

SQLiteデータベースへのCRUD操作を担当。

**テーブル設計:**
```sql
CREATE TABLE IF NOT EXISTS prices (
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    turnover REAL,
    PRIMARY KEY (date, code)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    market_cap REAL,
    sector TEXT,
    PRIMARY KEY (date, code)
);

CREATE TABLE IF NOT EXISTS sync_progress (
    table_name TEXT PRIMARY KEY,
    last_synced_date TEXT
);
```

> [!NOTE]
> `sync_progress`テーブルにより、中断後の再開が可能になります。

---

### [NEW] [collector.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/collector.py)

データ収集のメインロジック。tqdmで進捗表示。

**処理フロー:**
1. `sync_progress`テーブルから最終同期日を取得
2. 最終同期日〜現在までをチャンク分割（月ごと）
3. 各チャンクを`jquants-api-client`で取得
4. SQLiteにUPSERT
5. 進捗を更新

---

### [NEW] [main.py](file:///C:/Users/yke/Projects/jquants-stock-collector/main.py)

CLIエントリーポイント。

```bash
# 使用例
python main.py --start 2014-01-01 --end 2024-12-23
```

---

## User Review Required

> [!IMPORTANT]
> **時価総額データについて**  
> J-Quants APIには `market_cap` という直接的なフィールドが存在しない可能性があります。
> 
> **選択肢:**
> 1. `株価 × 発行済株式数` で計算（発行済株式数は `get_listed_info` から取得可能か要確認）
> 2. `get_fins_fs_details` の財務情報から取得可能か確認
> 
> **確認事項:** Premium Planで利用可能なフィールドを実際にAPIを叩いて確認する必要があります。まずは認証と銘柄一覧取得から実装し、時価総額の取得方法は後続タスクで確定させる方針でよろしいでしょうか？

---

## Verification Plan

### 自動テスト
このプロジェクトは新規作成のため、既存のテストはありません。以下の検証を実施予定です：

1. **ユニットテスト（手動実行）**
   ```bash
   cd C:\Users\yke\Projects\jquants-stock-collector
   python -c "from src.client import JQuantsClient; print('Import OK')"
   ```

2. **認証テスト**
   ```bash
   # .envにJQUANTS_MAIL_ADDRESS, JQUANTS_PASSWORD設定後
   python -c "from src.client import JQuantsClient; import os; from dotenv import load_dotenv; load_dotenv(); c = JQuantsClient(os.getenv('JQUANTS_MAIL_ADDRESS'), os.getenv('JQUANTS_PASSWORD')); print(c.get_listed_stocks().head())"
   ```

### 手動検証
1. スクリプト実行後、SQLiteデータベースファイルが生成されていることを確認
2. `sqlite3 stock_data.db "SELECT COUNT(*) FROM prices;"` でレコード数を確認
