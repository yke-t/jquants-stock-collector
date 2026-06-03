# Signal Performance Report: 2026-01

## Evaluation Rules
- Entry price: next trading day's open after `signal_date`.
- Evaluation window: 20 future trading days.
- Incomplete signals are kept in diagnostics but excluded from performance aggregates.
- `signal_price` sanity check: 0.2 <= signal_price / next_open <= 5.0.

## Data Quality
- total_signals: 140
- evaluated_complete: 140
- incomplete_or_missing_price: 0
- invalid_signal_price: 0

## Verdict Summary
| verdict | signals | evaluated | incomplete | avg_return_pct | median_return_pct | win_rate_pct | avg_max_gain_pct | avg_max_loss_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY | 11 | 11 | 0 | 9.25 | 5.49 | 72.73 | 13.94 | -5.73 | 36.36 | 63.64 |
| WATCH | 13 | 13 | 0 | 1.78 | 3.12 | 61.54 | 6.77 | -6.36 | 46.15 | 46.15 |
| REJECT | 76 | 76 | 0 | 8.46 | 4.04 | 60.53 | 14.79 | -6.29 | 53.95 | 56.58 |
| N/A | 40 | 40 | 0 | 3.47 | 2.62 | 62.50 | 12.20 | -4.86 | 32.50 | 25.00 |

## Month x Verdict Summary
| month | verdict | evaluated | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-01 | ENTRY | 11 | 9.25 | 5.49 | 72.73 | 36.36 | 63.64 |
| 2026-01 | N/A | 40 | 3.47 | 2.62 | 62.50 | 32.50 | 25.00 |
| 2026-01 | REJECT | 76 | 8.46 | 4.04 | 60.53 | 53.95 | 56.58 |
| 2026-01 | WATCH | 13 | 1.78 | 3.12 | 61.54 | 46.15 | 46.15 |

## Invalid signal_price Sample
_No data_

## Market Sentiment by Date
| signal_date | market_sentiment | market_bucket |
| --- | --- | --- |
| 2026-01-04 | 0.67 | bullish |
| 2026-01-05 | 0.68 | bullish |
| 2026-01-07 | 0.73 | bullish |
| 2026-01-08 | 0.72 | bullish |
| 2026-01-13 | 0.76 | bullish |
| 2026-01-14 | 0.78 | bullish |
| 2026-01-20 | 0.79 | bullish |

## Market Bucket x Verdict Summary
| market_bucket | verdict | count | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bullish | ENTRY | 11 | 9.25 | 5.49 | 72.73 | 36.36 | 63.64 |
| bullish | WATCH | 13 | 1.78 | 3.12 | 61.54 | 46.15 | 46.15 |
| bullish | REJECT | 76 | 8.46 | 4.04 | 60.53 | 53.95 | 56.58 |

## Market Bucket x MA25 Bucket x Verdict Summary
| market_bucket | ma25_bucket | verdict | count | avg_ma25_rate_pct | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bullish | <=-8 | ENTRY | 1 | -8.25 | 0.61 | 0.61 | 100.00 | 0.00 | 0.00 |
| bullish | <=-8 | WATCH | 3 | -8.62 | 6.98 | 6.30 | 100.00 | 33.33 | 66.67 |
| bullish | <=-8 | REJECT | 9 | -19.64 | 33.29 | 6.79 | 77.78 | 88.89 | 44.44 |
| bullish | -8..-5 | ENTRY | 5 | -6.61 | 11.95 | 5.92 | 60.00 | 40.00 | 60.00 |
| bullish | -8..-5 | WATCH | 5 | -6.06 | 2.86 | 3.12 | 60.00 | 60.00 | 40.00 |
| bullish | -8..-5 | REJECT | 35 | -6.12 | 5.92 | 5.00 | 65.71 | 37.14 | 65.71 |
| bullish | -5..-3 | ENTRY | 5 | -4.33 | 8.28 | 5.49 | 80.00 | 40.00 | 80.00 |
| bullish | -5..-3 | WATCH | 5 | -3.97 | -2.42 | -0.66 | 40.00 | 40.00 | 40.00 |
| bullish | -5..-3 | REJECT | 32 | -4.28 | 4.26 | -0.03 | 50.00 | 62.50 | 50.00 |

## Best 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-01-20 | 67870 | メイコー | REJECT | 115.14 | 123.31 | -0.44 | News:下方修正検出 |
| 2026-01-14 | 67870 | メイコー | REJECT | 90.80 | 94.68 | -6.54 | News:下方修正検出 |
| 2026-01-13 | 67870 | メイコー | REJECT | 69.08 | 80.42 | -8.23 | News:下方修正検出 |
| 2026-01-14 | 69610 | エンプラス | REJECT | 55.96 | 61.07 | -1.93 | News:ストップ安検出 |
| 2026-01-13 | 69610 | エンプラス | REJECT | 50.96 | 61.07 | -1.93 | News:減配検出 |
| 2026-01-08 | 69610 | エンプラス | ENTRY | 49.15 | 54.00 | -3.61 | Normal:通常押し目(市場-1.6%) |
| 2026-01-05 | 31100 | 日東紡績 | N/A | 43.13 | 67.98 | -4.99 |  |
| 2026-01-08 | 72360 | ティラド | REJECT | 39.21 | 39.83 | -0.62 | News:減配検出 |
| 2026-01-04 | 31100 | 日東紡績 | N/A | 37.13 | 70.72 | -3.44 |  |
| 2026-01-20 | 45060 | 住友ファーマ | REJECT | 33.16 | 49.56 | -6.39 | News:下方修正検出 |

## Worst 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-01-20 | 36260 | ＴＩＳ | REJECT | -34.30 | 0.68 | -35.08 | News:ストップ安検出 |
| 2026-01-07 | 48280 | ビジネスエンジニアリング | REJECT | -20.60 | 2.70 | -23.96 | News:下方修正検出 |
| 2026-01-05 | 48280 | ビジネスエンジニアリング | N/A | -16.76 | 0.59 | -17.29 |  |
| 2026-01-04 | 48280 | ビジネスエンジニアリング | N/A | -16.39 | 1.18 | -16.80 |  |
| 2026-01-14 | 37740 | インターネットイニシアティブ | REJECT | -16.00 | 1.38 | -20.73 | News:下方修正検出 |
| 2026-01-20 | 37740 | インターネットイニシアティブ | WATCH | -15.17 | 1.70 | -19.92 | Individual:固有下落(市場+2.3%) |
| 2026-01-07 | 37740 | インターネットイニシアティブ | ENTRY | -13.79 | 2.23 | -14.47 | Normal:通常押し目(市場-1.1%) |
| 2026-01-04 | 48120 | 電通総研 | N/A | -12.33 | 0.98 | -12.62 |  |
| 2026-01-05 | 48120 | 電通総研 | N/A | -11.34 | 2.51 | -11.89 |  |
| 2026-01-20 | 30640 | ＭｏｎｏｔａＲＯ | REJECT | -11.12 | 2.76 | -12.21 | News:減配検出 |
