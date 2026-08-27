# J-Quants 日本株データ収集スクリプト 実装計画

## 概要
J-Quants API (Premium Plan) を使用して、過去10年分（2014年〜現在）の日本株データをSQLiteデータベースに保存するPythonスクリプトを作成します。

## 技術選定の理由（Zero-Toil観点）

| 選択 | 理由 |
|------|------|
| **requests + J-Quants V2** | V2 APIキーを`x-api-key`で送り、利用エンドポイントと保存フィールドを明示的に管理。 |
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
requests>=2.31.0
pandas>=2.0.0
python-dotenv>=1.0.0
tqdm>=4.65.0
```

---

### [NEW] [client.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/client.py)

J-Quants V2 APIをラップし、APIキー認証と銘柄一覧取得を提供するクラス。

**主要メソッド:**
- `__init__()` - `.env`の`JQUANTS_API_KEY`でクライアントを初期化
- `get_listed_info()` - 上場銘柄情報を取得
- `get_daily_quotes()` - 日足株価を取得
- `get_financial_summary()` - 財務サマリーを取得

```python
import requests

from src.settings import JQUANTS_API_KEY

class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self):
        if not JQUANTS_API_KEY:
            raise ValueError("JQUANTS_API_KEY environment variable is required.")
        self.headers = {"x-api-key": JQUANTS_API_KEY}

    def get_listed_info(self):
        response = requests.get(
            f"{self.BASE_URL}/equities/master",
            headers=self.headers,
        )
        response.raise_for_status()
        return response.json()
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
3. 各チャンクをJ-Quants V2 APIで取得
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
   # .envにJQUANTS_API_KEY設定後（外部APIへの読み取りリクエスト）
   python -c "from src.client import JQuantsClient; print(JQuantsClient().get_listed_info().keys())"
   ```

### 手動検証
1. スクリプト実行後、SQLiteデータベースファイルが生成されていることを確認
2. `sqlite3 stock_data.db "SELECT COUNT(*) FROM prices;"` でレコード数を確認
