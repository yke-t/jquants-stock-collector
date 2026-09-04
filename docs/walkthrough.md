# J-Quants Stock Data Collector - Walkthrough

> [!NOTE]
> この文書は初期コレクター実装時の記録です。現行の導入・運用手順は
> `README.md`と`docs/CODEX_MIGRATION.md`を参照してください。

## 概要
J-Quants API (Premium Plan) を使用して日本株データを収集し、SQLiteデータベースに保存するスクリプトを実装完了。

## 実装したファイル

| ファイル | 役割 |
|----------|------|
| [main.py](file:///C:/Users/yke/Projects/jquants-stock-collector/main.py) | CLIエントリーポイント |
| [src/client.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/client.py) | J-Quants APIクライアント |
| [src/database.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/database.py) | SQLiteデータベース操作 |
| [src/collector.py](file:///C:/Users/yke/Projects/jquants-stock-collector/src/collector.py) | データ収集ロジック |

## 機能一覧

- ✅ **認証処理**: J-Quants API V2の`x-api-key`ヘッダー
- ✅ **株価データ取得**: 期間指定で全銘柄の日足データを取得
- ✅ **財務情報取得**: 期間指定で財務データを取得
- ✅ **進捗管理**: `sync_progress`テーブルで中断再開可能
- ✅ **tqdm進捗表示**: チャンクごとの進捗をリアルタイム表示

## 検証結果

```powershell
python scripts\verify_project.py
python scripts\verify_project.py --with-db
```

## 使用方法

```powershell
# 1. 環境設定
cd C:\Users\yke\Projects\jquants-stock-collector
Copy-Item .env.example .env
# .envを編集してJQUANTS_API_KEYを設定

# 2. 実行
python main.py --start 2014-01-01 --end 2024-12-23
```

## 現在の後続作業

- 時価総額フィルターを再導入する場合は、基準日と株式分割後の株数基準を検証する。
- 定期処理の状態確認には`python scripts\audit_scheduled_operations.py`を使う。
- 外部APIを使う確認は、対象と副作用を明示してから個別に実行する。
