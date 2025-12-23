# J-Quants Stock Data Collector

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
