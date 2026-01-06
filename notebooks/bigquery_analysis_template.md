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
PROJECT_ID = "nisa-jquant"  # プロジェクトID
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
# RSI（相対力指数）による「深すぎる押し目」の判定
query = f"""
WITH price_changes AS (
  SELECT 
    date, code, close,
    close - LAG(close) OVER (PARTITION BY code ORDER BY date) as diff
  FROM `nisa-jquant.stock_data.prices` -- プロジェクトIDは環境に合わせて変更してください
),
gains_losses AS (
  SELECT 
    *,
    IF(diff > 0, diff, 0) as gain,
    IF(diff < 0, ABS(diff), 0) as loss
  FROM price_changes
),
rsi_calc AS (
  SELECT 
    *,
    AVG(gain) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
    AVG(loss) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss
  FROM gains_losses
)
SELECT 
  date, code, close,
  ROUND(100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))), 1) as rsi_14
FROM rsi_calc
WHERE date >= '2024-01-01'
ORDER BY code, date
"""
df_custom = client.query(query).to_dataframe()
df_custom.head()
```

```python
# セクターローテーション分析（業種別モメンタム）
query = f"""
WITH sector_prices AS (
  SELECT 
    p.date,
    f.sector17codename as sector, -- 17業種区分を使用
    AVG(p.close) as avg_price -- 単純平均（時価総額加重ではない簡易版）
  FROM `nisa-jquant.stock_data.prices` p
  JOIN `nisa-jquant.stock_data.fundamentals` f ON p.code = f.code
  WHERE p.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
  GROUP BY 1, 2
),
momentum AS (
  SELECT 
    date,
    sector,
    avg_price,
    LAG(avg_price, 20) OVER (PARTITION BY sector ORDER BY date) as price_1m_ago,
    (avg_price / LAG(avg_price, 20) OVER (PARTITION BY sector ORDER BY date) - 1) * 100 as return_1m
  FROM sector_prices
)
SELECT * FROM momentum
WHERE date = (SELECT MAX(date) FROM momentum)
ORDER BY return_1m DESC
"""
df_custom = client.query(query).to_dataframe()
df_custom.head()
```

```python
# ボラティリティ（HV）によるリスク管理
query = f"""
WITH log_returns AS (
  SELECT 
    date, code, close,
    LN(close / LAG(close) OVER (PARTITION BY code ORDER BY date)) as log_ret
  FROM `nisa-jquant.stock_data.prices`
),
volatility AS (
  SELECT 
    date, code, close,
    -- 20日間の標準偏差 × √252 (年率換算)
    STDDEV(log_ret) OVER (PARTITION BY code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) * SQRT(252) * 100 as hv_annual
  FROM log_returns
)
SELECT 
  date, code, close, 
  ROUND(hv_annual, 2) as hv_score,
  -- HVが高い銘柄は損切り幅を広く、低い銘柄は狭くする例
  ROUND(close * (1 - (hv_annual / 100 * 0.5)), 0) as dynamic_stop_loss
FROM volatility
WHERE date >= '2024-01-01'
ORDER BY hv_score DESC
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
