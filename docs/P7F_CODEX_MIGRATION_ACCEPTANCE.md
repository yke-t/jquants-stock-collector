# P7f Codex移行 最終受入監査

## 判定

2026-09-13時点で、AntigravityからCodexへの実行・開発・監視移行を`PASS`とする。
Codexをソース変更、レビュー、検証、障害診断の主環境とし、実データ処理はWindows
Task Schedulerから本リポジトリの固定バッチを直接起動する構成になっている。

P6j〜P6mの前向き評価は、固定済み戦略の将来データを待つ継続評価であり、移行の
未完了項目ではない。目標判定は252取引日まで行わない。

## 受入証拠

### リポジトリとCI

- 受入監査開始時の`main`と`origin/main`は
  `1e4d587d649c9d2d17bb088cfa1d5d8368fdde6d`で一致し、作業ツリーはクリーンだった。
- GitHub Actionsの`Offline verification` run
  [34762700806](https://github.com/yke-t/jquants-stock-collector/actions/runs/34762700806)
  は同じコミットに対して`success`で完了した。
- CIはWindows、Python 3.11、固定依存を使用し、権限は`contents: read`、checkoutの
  認証情報保持は無効、検証コマンドは`python scripts/verify_project.py`である。
- `python scripts\verify_project.py --with-db`は143テストと`pip check`を通過し、
  実DB検査および株式数基準の既知ケース`19610`、`20030`も`PASS`だった。
- `.env`、`secret_key.json`、`stock_data.db`は現在の追跡対象にもGit履歴にもない。
  `.gitignore`は秘密情報、SQLite、ログ、生成CSV・チャートを除外している。

### Antigravity依存

- リポジトリ内の`Antigravity`と`agyhub`参照は移行・受入履歴文書だけで、コード、
  バッチ、設定、CIには存在しない。
- プロジェクト固有のAntigravity状態は2026-08-22に削除済みである。共有キャッシュの
  2参照は他プロジェクトを壊さず個別削除できないため残しているが、実行経路ではない。

### Windows定期処理

2026-09-13にTask Schedulerを読み取り専用で照会した。

| タスク | 実行対象 | 最終実行 | 結果 | 未実行 | 次回 |
|---|---|---|---:|---:|---|
| NISA-JQuant Daily | `run_daily.bat` | 2026-09-11 17:00 | 0 | 0 | 2026-09-14 17:00 |
| NISA-JQuant Dividend Daily | `run_dividend_daily.bat` | 2026-09-11 18:00 | 0 | 0 | 2026-09-14 18:00 |
| SnowMoney_Monthly_Eval | `run_monthly_eval.bat` | 2026-09-01 09:00 | 0 | 0 | 2026-10-01 09:00 |

3タスクの実行対象はすべて
`C:\Users\yke\Projects\jquants-stock-collector`配下の絶対パスである。各バッチは冒頭で
自身のディレクトリへ移動するため、月次タスクのTask Scheduler側作業ディレクトリが
空でもリポジトリを基準に実行される。

`python scripts\audit_scheduled_operations.py --date 2026-09-11`は終了コード0、
`overall_status=pass`だった。日次・配当の終了ログ、DB鮮度、配当候補CSV、月次配当
バックテストCSVはすべて`pass`だった。2026-09-13は日曜のため当日指定の`pending`は
正常であり、次の平日実行は2026-09-14である。

### Codex監視

- `J-Quants 日次監査・前向き評価`は`ACTIVE`で、月〜金の20:00に稼働する。
- 正常時は無通知で、定期処理を起動・修復せず、P6j、P6k、P6l、P6mを順番に
  読み取り評価する。
- 役目を終えた単発`P5g 定期書き込み運用確認`は履歴を残して`PAUSED`にした。

### 復旧性

- P7eで1,472,598,016 bytesのSQLiteオンラインバックアップを作成した。
- 元DB、バックアップ、一時復元DBの`quick_check`、スキーマSHA-256、全ユーザー
  テーブル件数が一致した。
- 復元検証後の一時DBは削除し、本バックアップと検証JSONはリポジトリ外に保持した。
- 本番`stock_data.db`は変更していない。

## 残る運用条件

- ローカルのCodex監視にはPCとCodexデスクトップアプリの稼働が必要である。
- J-Quants、yfinance、Google Sheets、Drive、BigQueryの可用性は外部要因であり、
  平日20:00の監視で障害を検出する。
- P6j〜P6mは取引日数到達待ちである。途中値で設定変更や合否判定を行わない。
- バックアップの自動世代管理は未導入である。P7eバックアップを削除せず、次工程で
  保存期間と容量上限を定義してから自動化する。
