# P7g/P7h SQLiteバックアップ世代管理

## 目的

本番`stock_data.db`を変更せず、検証済みバックアップを継続的に確保しながら、保存容量が
無制限に増えないようにする。自動削除の対象を機械的に証明できるファイルだけに限定する。

## 固定方針

- 実行頻度: 毎週土曜9:00、開始できなかった場合はTask Schedulerの
  `StartWhenAvailable`を使用
- バックアップ方式: SQLiteオンラインバックアップ
- 復旧確認: 毎回、一時DBへの復元と`PRAGMA quick_check`、スキーマSHA-256、全ユーザー
  テーブル件数、application/user versionを照合
- 世代数: 最新8世代
- 最低保持数: 1世代
- ディレクトリ容量上限: 20GiB
- 競合防止: 日次・配当・月次処理と同じ
  `Global\JQuantsStockCollectorPipeline` mutex
- 実行時間上限: 2時間
- Windowsログオン方式: 対話ログオン。パスワードを保存しない

現行DBが約1.47GBのため、手動P7eバックアップ1組と週次8世代を保持しても約13.3GBで、
20GiB上限に余裕がある。DB増加時は世代数より容量上限を優先するが、最新1世代は削除せず、
上限を満たせなければ成功扱いにしない。

## 削除境界

自動管理するDB名は`stock_data-YYYYMMDD-HHMMSS.db`だけで、同名の
`stock_data-YYYYMMDD-HHMMSS.verification.json`との組を必須とする。検証JSONについて
次をすべて満たす組だけを古い順に削除候補にできる。

- `backup_verified=true`
- `source_database_modified=false`
- `existing_backup_overwritten=false`
- 記録された絶対パスと現在のDBパスが一致
- 記録サイズと現在のDBサイズが一致
- バックアップの`quick_check=ok`、スキーマハッシュ、テーブル件数が存在
- 一時復元を実施・検証し、一時DBを削除済み
- DBとJSONがシンボリックリンクではない

手動P7eバックアップ、不完全な組、不正なJSON、無関係なファイル、ディレクトリは保護対象で
あり、自動削除しない。実行前の計画と削除直前の再検証が一致しなければ停止する。

## 実装

- `scripts/backup_database.py`: 非上書きオンラインバックアップと復元検証
- `scripts/backup_retention.py`: 既定はドライラン、`--apply`時だけ検証済み組を限定削除
- `scripts/run_database_backup.ps1`: 共有ロック、バックアップ、復元、世代管理、外部ログ
- `scripts/configure_database_backup_task.ps1`: Task Scheduler定義の登録・検証・ロールバック
- `tests/test_backup_retention.py`: 世代、容量、最低保持、不正記録保護、手動ファイル保護、
  一時SQLiteを使ったPowerShellランナーの回帰試験

## 2026-09-13 実装前ゲート

- 集中テスト: 14件PASS（既存バックアップ6件＋世代管理8件）
- 実ディレクトリのドライラン: `status=passed`、`prune=[]`、`deleted=[]`
- P7eのDBと検証JSON: 管理命名規則外として保護
- 実ディレクトリ容量: 1,472,599,984 bytes、20GiB上限内

週次Task Scheduler登録と週次ランナーの実DB実行は、コードの全検証とGitチェックポイント後に
行う。設定処理自体はバックアップワークフローを開始しない。

