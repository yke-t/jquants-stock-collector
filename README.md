# J-Quants Stock Data Collector

J-Quantsとyfinanceから日本株データを収集し、SQLiteへ保存して、日次シグナル・長期配当候補・評価・バックテストを生成するローカル運用プロジェクトです。

## Codexでの開始方法

このリポジトリではCodexを開発・レビュー・診断の標準環境として扱います。最初に[AGENTS.md](AGENTS.md)と[Codex移行ガイド](docs/CODEX_MIGRATION.md)を確認してください。

```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.lock.txt
Copy-Item .env.example .env
python scripts/verify_project.py
python scripts/verify_project.py --with-db
```

- 実値は`.env`と`secret_key.json`へ置き、Gitへ追加しません。
- Codexのプロジェクト設定は`.codex/config.toml`にあります。
- 既定の検証はオフラインです。外部APIやGoogleサービスを使う検証は明示的に実行します。

## 現在の主要フロー

### 日次シグナル

```powershell
python -m src.update_yfinance
python -m src.scan
python -m src.sync_bigquery
```

Windows Task Scheduler用の入口は`run_daily.bat`です。

### 長期配当候補

```powershell
python src/financial_collector.py --code 7203
python src/dividend_scan.py --limit 50 --with-news
python src/dividend_backtest.py --start 2025-01-01 --top-n 20
```

Google Sheets／Driveへの出力を含む入口は`run_dividend_daily.bat`です。

### 評価

```powershell
python -m src.evaluate --prev-month --charts --report
```

## ディレクトリ

```text
main.py                     J-Quants株価収集CLI
src/settings.py             .envを使う共通設定
src/database.py             SQLiteスキーマと保存処理
src/scan.py                 日次シグナル
src/dividend_scan.py        長期配当候補
src/dividend_backtest.py    配当戦略バックテスト
src/split_factor_backfill.py 株式分割時の限定価格修復・係数補完
src/notifier.py             Google Sheets出力
src/sync_bigquery.py        BigQuery差分同期
scripts/verify_project.py   Codex向けオフライン検証
tests/                      ユニットテスト
tests/integration/          明示実行する外部APIテスト
```

## 運用上の注意

配当スキャナと配当バックテストは、明示的な`adjustmentfactor`がある場合だけ1株指標を同じ株数基準へ補正します。大きな価格断絶に係数がない銘柄は、推測で補正せず`DATA_WARNING`として利回り計算から除外します。`src/split_factor_backfill.py`は、J-Quants原値のドライラン照合、更新対象とバックアップの一致確認、限定価格修復・係数補完を1トランザクションで行います。各ローカルDBは個別にバックアップしたうえで適用し、`python scripts/verify_project.py --with-db`で再検証してください。

また、配当財務の日次同期は現在`--missing-only`で、取得済み銘柄を継続更新しません。これらの修正と実データ回帰確認が完了するまでは、配当結果を参考値として扱ってください。

## Legacy collector quickstart

J-Quants API (Premium Plan) を使用して、日本株の過去データを収集しSQLiteデータベースに保存するスクリプトです。

## 機能

- 📈 過去10年分の株価データ（四本値 + 出来高 + 売買代金）
- 💰 財務情報（時価総額、セクター情報）
- 🔄 中断再開機能（進捗をDBに保存）
- 📊 tqdmによる進捗表示

## セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -r requirements.txt
```

### 2. 環境変数の設定

```bash
cp .env.example .env
# .envを編集してJ-Quants APIの認証情報を設定
```

### 3. 実行

```bash
# 全期間のデータを取得
python main.py --start 2014-01-01 --end 2024-12-23

# 特定期間のみ取得
python main.py --start 2024-01-01 --end 2024-12-23
```

## ディレクトリ構成

```
├── main.py              # エントリーポイント
├── src/
│   ├── __init__.py
│   ├── client.py        # J-Quants APIクライアント
│   ├── database.py      # SQLiteデータベース操作
│   └── collector.py     # データ収集ロジック
├── docs/
│   ├── task.md          # タスク管理
│   └── implementation_plan.md  # 実装計画
├── requirements.txt
├── .env.example
└── README.md
```

## 出力

- `stock_data.db` - SQLiteデータベース
  - `prices` テーブル: 株価データ
  - `fundamentals` テーブル: 財務情報
  - `sync_progress` テーブル: 同期進捗

## ライセンス

MIT License
