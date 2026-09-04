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
- GitHub ActionsもPython 3.11／Windows上で`python scripts/verify_project.py`だけを実行し、秘密情報や本番DBを渡しません。

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

`run_daily.bat`、`run_dividend_daily.bat`、`run_monthly_eval.bat`は、共通の
Windows名前付きロックを取得してから処理を開始します。別のフローが実行中なら
後続処理を開始せず、終了コード`75`と`[SKIP]`ログを残します。各運用ログは
10MiB以上になると実行前に日付付きファイルへ移動し、直近5世代を保持します。

### 評価

```powershell
python -m src.evaluate --prev-month --charts --report
```

### 保存シグナルの読み取り専用分析

```powershell
python scripts/analyze_signal_performance.py --output-dir reports/signal_analysis
python scripts/build_signal_analysis_report.py `
  --summary reports/signal_analysis/analysis_summary.json `
  --output-dir reports/signal_analysis
```

この分析はSQLiteを読み取り専用で開き、株式単位を明示的な調整係数で確認できる
価格窓だけを使います。APIやGoogleサービスには接続せず、DBも更新しません。
既存のポートフォリオ・バックテストは、約定時点、株式分割、資金制約、損益集計の
方法論を再設計するまで意思決定用の根拠に含めません。

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
scripts/audit_scheduled_operations.py 定期処理の読み取り専用監査
scripts/analyze_signal_performance.py 保存シグナルの株式単位検証・成績分析
scripts/build_signal_analysis_report.py 分析JSONから検証用レポート定義を構築
scripts/run_with_lock.ps1   BAT共通の排他実行・ログ世代管理
tests/                      ユニットテスト
tests/integration/          明示実行する外部APIテスト
```

## 運用上の注意

配当スキャナと配当バックテストは、明示的な`adjustmentfactor`がある場合だけ1株指標を同じ株数基準へ補正します。大きな価格断絶に係数がない銘柄は、推測で補正せず`DATA_WARNING`として利回り計算から除外します。`src/split_factor_backfill.py`は、J-Quants原値のドライラン照合、更新対象とバックアップの一致確認、限定価格修復・係数補完を1トランザクションで行います。各ローカルDBは個別にバックアップしたうえで適用し、`python scripts/verify_project.py --with-db`で再検証してください。

配当財務の日次同期は`--stale-days 7 --limit 500`で、未取得銘柄を先に、取得済み銘柄を最終更新が古い順にローテーション更新します。正常な空応答も取得試行として`sync_progress`へ記録するため、財務データのない銘柄で処理順が停滞しません。株式分割を含む実データ回帰確認は完了していますが、運用コードを変更した場合は、実行結果・DB更新・生成物を再確認してから運用可能と判断してください。

## 過去期間のJ-Quants株価収集

`main.py`は、J-Quants V2を使用して指定期間の日本株データをSQLiteへ保存します。通常の17:00日次タスクは`run_daily.bat`からyfinanceを使用し、18:00配当タスクはJ-Quantsを使用します。

```powershell
# 全期間のデータを取得
python main.py --start 2014-01-01 --end 2024-12-23

# 例: 直近期間のみ取得
python main.py --start 2026-01-01 --end 2026-08-31
```

このコマンドはDBを更新し、J-Quantsへアクセスします。通常の検証には使用せず、実行前に対象期間とDBバックアップを確認してください。

## ライセンス

MIT License
