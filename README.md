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

### ポートフォリオ・ウォークフォワード検証

```powershell
python -m src.backtest_wfa --start 2016-01-01 --splits 5
```

この検証はSQLiteを読み取り専用で開き、終値で生成したシグナルを翌取引日の始値で約定します。
明示的な`adjustmentfactor`だけでOHLCを同一株式数基準へ補正し、説明できない大幅な価格断絶を持つ
銘柄は除外します。売買手数料・スリッページ・100株単位・現金残高・最大保有数を反映し、
拡大型の学習期間と重複しない次区間でアウト・オブ・サンプル成績を複利集計します。
既定の出力先は`reports/wfa/`です。方法論と制約は
[ウォークフォワード検証仕様](docs/WALK_FORWARD_BACKTEST.md)を参照してください。

この分析はSQLiteを読み取り専用で開き、株式単位を明示的な調整係数で確認できる
価格窓だけを使います。APIやGoogleサービスには接続せず、DBも更新しません。
旧`src/backtest.py`の結果は、約定時点、株式分割、資金制約、損益集計を現在の基準で
扱わないため、意思決定用の根拠に含めません。再設計済みの検証経路は
`src/backtest_wfa.py`と、固定マニフェストを使う`src.forward_evaluation`です。

## ディレクトリ

```text
main.py                     J-Quants株価収集CLI
src/settings.py             .envを使う共通設定
src/database.py             SQLiteスキーマと保存処理
src/scan.py                 日次シグナル
src/dividend_scan.py        長期配当候補
src/dividend_backtest.py    配当戦略バックテスト
src/price_basis.py          明示的な調整係数によるOHLC株式数基準の共通化
src/backtest_wfa.py         翌取引日約定・資金制約付きポートフォリオWFA
src/split_factor_backfill.py 株式分割時の限定価格修復・係数補完
src/notifier.py             Google Sheets出力
src/sync_bigquery.py        BigQuery差分同期
scripts/verify_project.py   Codex向けオフライン検証
scripts/audit_scheduled_operations.py 定期処理の読み取り専用監査
scripts/analyze_signal_performance.py 保存シグナルの株式単位検証・成績分析
scripts/build_signal_analysis_report.py 分析JSONから検証用レポート定義を構築
scripts/run_with_lock.ps1   BAT共通の排他実行・ログ世代管理
scripts/backup_database.py  SQLiteオンラインバックアップ・復元検証
scripts/backup_retention.py 検証済み週次バックアップだけの世代管理
scripts/run_database_backup.ps1 バックアップ・復元検証・世代管理の週次実行
tests/                      ユニットテスト
tests/integration/          明示実行する外部APIテスト
```

## 運用上の注意

配当スキャナと配当バックテストは、明示的な`adjustmentfactor`がある場合だけ1株指標を同じ株数基準へ補正します。大きな価格断絶に係数がない銘柄は、推測で補正せず`DATA_WARNING`として利回り計算から除外します。`src/split_factor_backfill.py`は、J-Quants原値のドライラン照合、更新対象とバックアップの一致確認、限定価格修復・係数補完を1トランザクションで行います。各ローカルDBは個別にバックアップしたうえで適用し、`python scripts/verify_project.py --with-db`で再検証してください。

配当財務の日次同期は`--stale-days 7 --limit 500`で、未取得銘柄を先に、取得済み銘柄を最終更新が古い順にローテーション更新します。正常な空応答も取得試行として`sync_progress`へ記録するため、財務データのない銘柄で処理順が停滞しません。株式分割を含む実データ回帰確認は完了していますが、運用コードを変更した場合は、実行結果・DB更新・生成物を再確認してから運用可能と判断してください。

### SQLiteバックアップと復元訓練

本番DBを変更せず、SQLiteのオンラインバックアップを作成して復元可能性を検証できます。

```powershell
$backupRoot = Join-Path ([Environment]::GetFolderPath('MyDocuments')) 'Codex Backups\jquants-stock-collector\database'
python scripts\backup_database.py `
  --source stock_data.db `
  --output (Join-Path $backupRoot 'stock_data-YYYYMMDD.db') `
  --result (Join-Path $backupRoot 'stock_data-YYYYMMDD.verification.json') `
  --restore-drill
```

元DBは読み取り専用で開き、バックアップと一時復元DBについて`PRAGMA quick_check`、
スキーマSHA-256、全ユーザーテーブルの行数、SQLiteのapplication/user versionを照合します。
復元訓練用DBは照合後に削除し、バックアップと検証JSONは保持します。既存の出力は上書きせず、
古いバックアップも自動削除しません。本番DBへの復元は、定期処理を停止し、対象と退避先を
別途確認した明示的な作業として扱ってください。

### 検証済みバックアップの週次世代管理

週次ランナーは共有パイプラインロックを取得してからオンラインバックアップと一時復元を行い、
両方が成功した場合だけ世代管理を適用します。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\run_database_backup.ps1
```

自動管理対象は`stock_data-YYYYMMDD-HHMMSS.db`と同名の
`.verification.json`がそろい、検証・一時復元・一時DB削除を確認できる組だけです。
最新8世代、最低1世代を保持し、バックアップディレクトリ全体を20GiB以内にします。
手動バックアップ、不完全な組、不正な検証JSON、無関係なファイルは削除しません。上限を
満たせない場合は終了コード2で失敗を通知します。削除を伴わない確認は次のコマンドです。

```powershell
python scripts\backup_retention.py --directory $backupRoot
```

管理者PowerShellから`scripts\configure_database_backup_task.ps1`を実行すると、土曜9:00の
`NISA-JQuant Database Backup`を登録します。設定処理はバックアップ本体を開始しません。

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
