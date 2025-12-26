# Snow Money - BigQuery Analysis Template

このノートブックはBigQueryに格納した株価データを分析するためのテンプレートです。

## 環境設定

```python
# 必要なライブラリをインストール
!pip install pandas-gbq matplotlib japanize-matplotlib -q

# ライブラリのインポート
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from google.colab import auth
from google.cloud import bigquery

# Google認証
auth.authenticate_user()
```

## BigQuery接続設定

```python
# プロジェクト設定（自分のプロジェクトIDに変更）
PROJECT_ID = "your-project-id"  # ← 変更してください
DATASET = "stock_data"

# BigQueryクライアント
client = bigquery.Client(project=PROJECT_ID)
```

## データ確認

```python
# 株価データのサンプル取得
query = f"""
SELECT date, code, close
FROM `{PROJECT_ID}.{DATASET}.prices`
ORDER BY date DESC
LIMIT 100
"""
df_sample = client.query(query).to_dataframe()
df_sample.head()
```

## 市場環境分析（MA75超え銘柄の割合）

```python
# 直近日の市場環境を計算
query = f"""
WITH latest_date AS (
  SELECT MAX(date) as max_date FROM `{PROJECT_ID}.{DATASET}.prices`
),
with_ma AS (
  SELECT 
    p.date,
    p.code,
    p.close,
    AVG(p.close) OVER (
      PARTITION BY p.code 
      ORDER BY p.date 
      ROWS BETWEEN 74 PRECEDING AND CURRENT ROW
    ) as ma75
  FROM `{PROJECT_ID}.{DATASET}.prices` p
)
SELECT 
  date,
  COUNT(*) as total_stocks,
  COUNTIF(close > ma75) as bullish_stocks,
  ROUND(COUNTIF(close > ma75) / COUNT(*) * 100, 1) as bullish_pct
FROM with_ma
WHERE date >= DATE_SUB((SELECT max_date FROM latest_date), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC
"""
df_market = client.query(query).to_dataframe()
df_market.head(10)
```

## 押し目買い候補の抽出

```python
# Golden Configuration に基づくシグナル抽出
query = f"""
WITH latest_date AS (
  SELECT MAX(date) as max_date FROM `{PROJECT_ID}.{DATASET}.prices`
),
with_indicators AS (
  SELECT 
    p.date,
    p.code,
    p.close,
    AVG(p.close) OVER (
      PARTITION BY p.code 
      ORDER BY p.date 
      ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
    ) as ma25,
    AVG(p.close) OVER (
      PARTITION BY p.code 
      ORDER BY p.date 
      ROWS BETWEEN 74 PRECEDING AND CURRENT ROW
    ) as ma75
  FROM `{PROJECT_ID}.{DATASET}.prices` p
),
signals AS (
  SELECT 
    i.*,
    f.coname,
    f.scalecat,
    (i.close / i.ma25 - 1) * 100 as dip_pct,
    i.close * 0.95 as stop_loss,
    i.ma25 as take_profit
  FROM with_indicators i
  LEFT JOIN `{PROJECT_ID}.{DATASET}.fundamentals` f ON i.code = f.code
  WHERE i.date = (SELECT max_date FROM latest_date)
    AND i.ma25 > i.ma75  -- GC形成中
    AND i.close < i.ma25 * 0.97  -- 押し目（3%以上下落）
    AND f.scalecat IN ('TOPIX Small 1', 'TOPIX Small 2', 'TOPIX Mid400')
)
SELECT 
  code,
  coname,
  scalecat,
  ROUND(close, 0) as close,
  ROUND(ma25, 0) as ma25,
  ROUND(dip_pct, 2) as dip_pct,
  ROUND(stop_loss, 0) as stop_loss,
  ROUND(take_profit, 0) as take_profit
FROM signals
ORDER BY dip_pct ASC
LIMIT 20
"""
df_signals = client.query(query).to_dataframe()
df_signals
```

## 新ロジックのテスト（カスタマイズ用）

```python
# ここに新しいロジックを記述
# 例: RSI計算、ボリンジャーバンド、出来高分析など

query = f"""
-- 新しい戦略ロジックをここに記述
SELECT 
  date,
  code,
  close,
  volume
FROM `{PROJECT_ID}.{DATASET}.prices`
WHERE date >= '2024-01-01'
LIMIT 1000
"""
df_custom = client.query(query).to_dataframe()
df_custom.head()
```

## 可視化

```python
# 市場環境の推移グラフ
plt.figure(figsize=(12, 6))
plt.plot(pd.to_datetime(df_market['date']), df_market['bullish_pct'])
plt.axhline(y=40, color='r', linestyle='--', label='Threshold (40%)')
plt.xlabel('Date')
plt.ylabel('Bullish %')
plt.title('Market Regime: % of Stocks Above MA75')
plt.legend()
plt.grid(True)
plt.show()
```
