# jquants-stock-collector アーキテクチャ

## プロジェクト概要

J-Quants APIを使用して日本株データを収集・分析し、NISA向け小型株トレンドフォロー戦略のシグナルを生成するシステム。

---

## ディレクトリ構造

```
jquants-stock-collector/
├── main.py                  # エントリポイント（J-Quantsデータ収集）
├── run_daily.bat            # 日次自動実行バッチ
├── requirements.txt         # 依存ライブラリ
├── stock_data.db            # SQLiteデータベース（約1.3GB）
├── daily_operation.log      # 運用ログ
├── secret_key.json          # GCPサービスアカウントキー（git対象外）
├── .env                     # 環境変数（git対象外）
├── src/
│   ├── client.py            # J-Quants APIクライアント
│   ├── collector.py         # データ収集ロジック
│   ├── database.py          # SQLiteデータベース操作
│   ├── scan.py              # 日次シグナルスキャナー
│   ├── news_analyzer.py     # 暴落銘柄自動調査（Phase 5）
│   ├── notifier.py          # Google Sheets通知
│   ├── update_yfinance.py   # yfinance日次データ更新（J-Quants代替）
│   ├── sync_bigquery.py     # BigQuery差分同期
│   ├── export_bigquery.py   # BigQuery全量エクスポート
│   ├── backtest.py          # バックテストエンジン（WFA版）
│   └── backtest_portfolio.py # ポートフォリオバックテスト
├── notebooks/
│   └── bigquery_analysis_template.md  # Colab分析テンプレート
└── docs/
    └── ARCHITECTURE.md
```

---

## データフロー

```mermaid
graph TD
    subgraph データ取得
        A1[J-Quants API V2] -->|HTTP GET| B[client.py]
        A2[yfinance] -->|代替| B2[update_yfinance.py]
    end
    
    B --> C[collector.py]
    B2 --> D
    C -->|INSERT| D[(stock_data.db)]
    
    D --> E[scan.py]
    E -->|市場判定+シグナル| F{GREEN?}
    F -->|Yes| G[news_analyzer.py]
    G -->|ニュース検索| H{REJECT?}
    H -->|No| I[notifier.py]
    I -->|gspread| J[Google Sheets]
    H -->|Yes| K[REJECT: 悪材料検出]
    F -->|No| L[終了: Cash is King]
    
    D -->|MERGE| M[sync_bigquery.py]
    M --> N[(BigQuery)]
    N --> O[Colab分析]
```

---

## 日次運用フロー

```mermaid
sequenceDiagram
    participant Scheduler as タスクスケジューラ
    participant Bat as run_daily.bat
    participant Data as データ取得
    participant Scan as scan.py
    participant News as news_analyzer.py
    participant Sheets as Google Sheets
    participant BQ as BigQuery
    
    Scheduler->>Bat: 毎日 17:00 起動
    Bat->>Data: STEP1: J-Quants or yfinance
    Data-->>Bat: データ更新完了
    Bat->>Scan: STEP2: シグナルスキャン
    Scan->>News: ニュース分析
    News-->>Scan: ENTRY/WATCH/REJECT判定
    Scan->>Sheets: シグナル書き込み
    Bat->>BQ: STEP3: 差分同期（前日分）
    Bat-->>Scheduler: ログ出力完了
```

---

## モジュール詳細

### データ取得系

| モジュール | 説明 | データソース |
|-----------|------|-------------|
| `main.py` + `collector.py` | J-Quantsからデータ収集 | J-Quants API V2 |
| `update_yfinance.py` | J-Quants代替データ取得 | Yahoo Finance |

### シグナル生成系

| モジュール | 説明 | 出力先 |
|-----------|------|--------|
| `scan.py` | 市場環境判定+シグナル抽出 | コンソール |
| `news_analyzer.py` | Google CSEでニュース検索+地合い比較 | scan.pyへ判定結果返却 |
| `notifier.py` | シグナルをスプレッドシートに書き込み | Google Sheets |

### BigQuery連携系

| モジュール | 説明 | 処理 |
|-----------|------|------|
| `export_bigquery.py` | 全量エクスポート | SQLite → BigQuery (REPLACE) |
| `sync_bigquery.py` | 差分同期（日次） | 前日分のみ MERGE |

---

## Phase 5: 暴落銘柄自動調査

### 判定ロジック

```mermaid
graph TD
    A[シグナル銘柄] --> B[Phase 1: Event Filter]
    B -->|Killer Keywords検出| C[REJECT]
    B -->|ヒットなし| D[Phase 2: Context Filter]
    D -->|連れ安| E[ENTRY]
    D -->|固有下落| F[WATCH]
```

### Killer Keywords
`下方修正`, `減配`, `不祥事`, `ストップ安`, `課徴金`, `業績悪化`, `赤字転落`, `倒産`, `粉飾`

### 出力カラム（Google Sheets）

| カラム | 説明 |
|--------|------|
| 判定結果 | ENTRY / WATCH / REJECT |
| 判定理由 | Sector:連れ安 / Individual:固有下落 / News:下方修正検出 |
| News Hit | 検出されたニュースタイトル |

---

## Golden Configuration

| パラメータ | 値 | 説明 |
|-----------|-----|------|
| `DIP_THRESHOLD` | 0.97 | 押し目閾値（MA25の97%以下で買い） |
| `MARKET_BULLISH_THRESHOLD` | 0.40 | 市場環境フィルター（40%以上で買い許可） |
| `STOP_LOSS_PCT` | 0.05 | 損切り -5% |
| `TRAILING_STOP_PCT` | 0.10 | トレーリングストップ -10% |
| `MAX_POSITIONS` | 20 | 最大保有銘柄数（1銘柄5%） |

---

## 環境設定

### 必要な環境変数 (.env)
```
JQUANTS_API_KEY=your_api_key_here
GOOGLE_CSE_API_KEY=your_google_cse_api_key
GOOGLE_CSE_ID=your_search_engine_id
```

### 依存ライブラリ (requirements.txt)
```
jquants-api-client>=2.0.0
pandas>=2.0.0
python-dotenv>=1.0.0
tqdm>=4.65.0
gspread>=5.10.0
google-auth>=2.20.0
yfinance>=1.0
pandas-gbq>=0.19.0
requests>=2.28.0
```

### GCP設定
- **プロジェクトID:** nisa-jquant
- **BigQueryデータセット:** stock_data
- **テーブル:** prices, fundamentals
- **Custom Search API:** 有効化済み（100回/日無料）
- **Programmable Search Engine:** ウェブ全体を検索

---

## run_daily.bat 設定

```batch
:: データソース選択
set USE_YFINANCE=0  :: J-Quants使用
set USE_YFINANCE=1  :: yfinance使用（解約後）
```

---

## リポジトリ

- **GitHub:** https://github.com/yke-t/jquants-stock-collector
