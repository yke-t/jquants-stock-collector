# J-Quants Stock Data Collector - Walkthrough

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

- ✅ **認証処理**: `jquants-api-client`ライブラリによる自動トークン管理
- ✅ **株価データ取得**: 期間指定で全銘柄の日足データを取得
- ✅ **財務情報取得**: 期間指定で財務データを取得
- ✅ **進捗管理**: `sync_progress`テーブルで中断再開可能
- ✅ **tqdm進捗表示**: チャンクごとの進捗をリアルタイム表示

## 検証結果

```bash
# インポートテスト
python -c "from src.client import JQuantsClient; from src.database import StockDatabase; from src.collector import DataCollector; print('All imports OK!')"
# → All imports OK!

# DB初期化テスト
python -c "from src.database import StockDatabase; db = StockDatabase('test.db'); print(f'Count: {db.get_price_count()}')"
# → Count: 0
```

## 使用方法

```bash
# 1. 環境設定
cd C:\Users\yke\Projects\jquants-stock-collector
cp .env.example .env
# .envを編集してJQUANTS_MAIL_ADDRESS, JQUANTS_PASSWORDを設定

# 2. 実行
python main.py --start 2014-01-01 --end 2024-12-23
```

## 次のステップ

> [!IMPORTANT]
> **時価総額データの取得方法**は、実際にAPIを叩いて確認が必要です。
> 
> `.env`に認証情報を設定後、以下のコマンドで確認してください：
> ```bash
> python -c "from src.client import JQuantsClient; import os; from dotenv import load_dotenv; load_dotenv(); c = JQuantsClient(os.getenv('JQUANTS_MAIL_ADDRESS'), os.getenv('JQUANTS_PASSWORD')); print(c.get_listed_stocks().columns.tolist())"
> ```
