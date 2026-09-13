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
- Windowsログオン方式: 対話ログオン・Limited。パスワードや管理者権限を使用しない

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

実装は151テストPASS後に`2e9f943`としてコミットし、`origin/main`へpushしてから実機へ
適用した。

## 2026-09-14 P7h実DB・Task Scheduler検証

週次ランナーを実DBに対して直接1回実行し、終了コード0を確認した。

| 項目 | 結果 |
|---|---|
| バックアップ | `stock_data-20260914-000034.db` |
| 検証JSON | `stock_data-20260914-000034.verification.json` |
| DBサイズ | 1,472,598,016 bytes |
| `quick_check` | 元DB・バックアップ・一時復元DBがすべて`ok` |
| スキーマSHA-256 | 3DBで一致 |
| 全ユーザーテーブル件数 | 3DBで一致 |
| 一時復元DB | 検証後に削除、ディレクトリも不存在 |
| 元DB | 更新なし。最終更新日時は2026-09-11 18:09:23のまま |
| 管理世代 | 1 |
| 削除候補・削除実績 | 0・0 |
| 保護対象 | P7e手動DB、P7e検証JSON、運用ログの3件 |
| ディレクトリ総量 | 2,945,209,782 bytes、20GiB上限内 |

`NISA-JQuant Database Backup`を現在ユーザーの`Interactive`・`Limited` principalで
登録した。パスワードと管理者権限は使用しない。状態は`Ready`、土曜9:00、
`StartWhenAvailable=true`、`MultipleInstances=IgnoreNew`、2時間上限、次回は
2026-09-19 09:00、未実行回数0である。登録処理中のワークフロー実行はない。

登録直後の`LastTaskResult=0x41303`は
[MicrosoftのTask Scheduler定数](https://learn.microsoft.com/ja-jp/windows/win32/taskschd/task-scheduler-error-and-success-constants)
で「まだ実行されていない」状態であり、初回予定後の成功を示す値ではない。P7jとして
最初のTask Scheduler経由実行を別途確認する。

## P7i監視統合

`scripts/audit_scheduled_operations.py`は、日次・配当監査に加えて週次バックアップを
読み取り専用で検査する。タスク結果、未実行回数、検証済み管理世代、7日以内の鮮度、
最新タスク実行との対応、保持容量を確認する。初回予定前は実DB検証バックアップが新鮮な
場合だけ`not_due`を許容し、それ以外の不一致は全体監査を`fail`にする。

2026-09-14 00:07の実機監査は、2026-09-11の日次・配当・DB・成果物がすべて`pass`、
週次バックアップが`not_due`、全体が`pass`だった。平日20:00のCodex監視も
`database_backup`の異常通知とP7jの一度だけの証跡化に対応済みである。

最終ゲートは156テスト、`pip check`、実DB検査、既知の株式数基準ケース`19610`と
`20030`、PowerShell構文がすべてPASSした。
