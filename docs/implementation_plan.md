# J-Quants 日本株データ収集スクリプト 実装計画

> [!NOTE]
> これは初期実装時の設計記録です。現在のセットアップと運用手順は
> `README.md`、`AGENTS.md`、`docs/CODEX_MIGRATION.md`を正とします。
> 以下の構成例やコード断片は、現行実装の完全な仕様ではありません。

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

### [NEW] `requirements.txt` / `requirements.lock.txt`

必要なPythonパッケージを`requirements.txt`で範囲指定し、検証済みの
Python 3.11環境を`requirements.lock.txt`で固定します。通常のセットアップには
`python -m pip install -r requirements.lock.txt`を使用します。

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

## 時価総額フィルターの現状

> [!IMPORTANT]
> **現状:** WFAバックテストの時価総額フィルターは無効です。再導入する場合は、
> J-Quants V2の原フィールド、発行済株式数の基準日、株式分割調整を確認し、
> `株価 × 発行済株式数`が同一株数基準であることを回帰テストしてください。

---

## Verification Plan

### オフライン検証

```powershell
python scripts\verify_project.py
python -m pip check
```

### DBを読む検証

```powershell
python scripts\verify_project.py --with-db
```

この検証はDBを読み取りますが、外部APIやGoogleサービスへは書き込みません。
外部連携の運用可否は、実コマンドの終了状態、関連DB行、生成物を別途確認します。
