# Signal Performance Report: 2026-04

## Evaluation Rules
- Entry price: next trading day's open after `signal_date`.
- Evaluation window: 20 future trading days.
- Incomplete signals are kept in diagnostics but excluded from performance aggregates.
- `signal_price` sanity check: 0.2 <= signal_price / next_open <= 5.0.

## Data Quality
- total_signals: 260
- evaluated_complete: 160
- incomplete_or_missing_price: 100
- invalid_signal_price: 0

## Verdict Summary
| verdict | signals | evaluated | incomplete | avg_return_pct | median_return_pct | win_rate_pct | avg_max_gain_pct | avg_max_loss_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY | 9 | 2 | 7 | 6.09 | 6.09 | 50.00 | 14.50 | -8.01 | 50.00 | 50.00 |
| WATCH | 35 | 22 | 13 | -2.25 | -5.69 | 31.82 | 21.37 | -12.45 | 72.73 | 40.91 |
| REJECT | 216 | 136 | 80 | 4.57 | -1.96 | 44.12 | 23.10 | -9.74 | 61.03 | 38.97 |

## Month x Verdict Summary
| month | verdict | evaluated | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04 | ENTRY | 2 | 6.09 | 6.09 | 50.00 | 50.00 | 50.00 |
| 2026-04 | REJECT | 136 | 4.57 | -1.96 | 44.12 | 61.03 | 38.97 |
| 2026-04 | WATCH | 22 | -2.25 | -5.69 | 31.82 | 72.73 | 40.91 |

## Invalid signal_price Sample
_No data_

## Market Sentiment by Date
| signal_date | market_sentiment | market_bucket |
| --- | --- | --- |
| 2026-04-01 | 0.44 | neutral |
| 2026-04-03 | 0.42 | neutral |
| 2026-04-06 | 0.43 | neutral |
| 2026-04-07 | 0.46 | neutral |
| 2026-04-08 | 0.55 | bullish |
| 2026-04-09 | 0.49 | neutral |
| 2026-04-10 | 0.45 | neutral |
| 2026-04-13 | 0.43 | neutral |

## Market Bucket x Verdict Summary
| market_bucket | verdict | count | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| neutral | ENTRY | 2 | 6.09 | 6.09 | 50.00 | 50.00 | 50.00 |
| neutral | WATCH | 20 | -1.25 | -5.47 | 35.00 | 70.00 | 45.00 |
| neutral | REJECT | 118 | 4.97 | -1.27 | 45.76 | 57.63 | 38.98 |
| bullish | WATCH | 2 | -12.26 | -12.26 | 0.00 | 100.00 | 0.00 |
| bullish | REJECT | 18 | 1.94 | -6.38 | 33.33 | 83.33 | 38.89 |

## Market Bucket x MA25 Bucket x Verdict Summary
| market_bucket | ma25_bucket | verdict | count | avg_ma25_rate_pct | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| neutral | <=-8 | ENTRY | 2 | -36.95 | 6.09 | 6.09 | 50.00 | 50.00 | 50.00 |
| neutral | <=-8 | WATCH | 11 | -10.96 | 8.57 | 16.68 | 63.64 | 45.45 | 63.64 |
| neutral | <=-8 | REJECT | 102 | -13.46 | 6.28 | 0.00 | 49.02 | 53.92 | 40.20 |
| neutral | -8..-5 | WATCH | 9 | -6.31 | -13.25 | -13.23 | 0.00 | 100.00 | 22.22 |
| neutral | -8..-5 | REJECT | 16 | -6.79 | -3.37 | -5.05 | 25.00 | 81.25 | 31.25 |
| bullish | <=-8 | REJECT | 6 | -13.80 | 21.32 | 6.67 | 66.67 | 66.67 | 50.00 |
| bullish | -8..-5 | WATCH | 2 | -6.03 | -12.26 | -12.26 | 0.00 | 100.00 | 0.00 |
| bullish | -8..-5 | REJECT | 8 | -6.81 | -5.28 | -6.38 | 25.00 | 87.50 | 37.50 |
| bullish | -5..-3 | REJECT | 4 | -4.24 | -12.72 | -12.84 | 0.00 | 100.00 | 25.00 |

## Best 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04-06 | 31030 | ユニチカ | REJECT | 137.21 | 304.43 | -7.20 | News:ストップ安検出 |
| 2026-04-03 | 31030 | ユニチカ | REJECT | 132.14 | 301.10 | -7.97 | News:ストップ安検出 |
| 2026-04-07 | 31030 | ユニチカ | REJECT | 108.68 | 296.02 | -2.35 | News:ストップ安検出 |
| 2026-04-08 | 31030 | ユニチカ | REJECT | 98.33 | 284.21 | -3.51 | News:ストップ安検出 |
| 2026-04-01 | 31030 | ユニチカ | REJECT | 96.61 | 253.23 | -18.95 | News:ストップ安検出 |
| 2026-04-06 | 59850 | サンコール | REJECT | 50.32 | 54.28 | -8.35 | News:減配検出 |
| 2026-04-03 | 62640 | マルマエ | REJECT | 48.33 | 50.51 | -9.40 | News:下方修正検出 |
| 2026-04-07 | 59850 | サンコール | REJECT | 43.57 | 55.71 | -1.36 | News:減配検出 |
| 2026-04-08 | 59850 | サンコール | REJECT | 41.84 | 48.30 | -5.85 | News:ストップ安検出 |
| 2026-04-03 | 59850 | サンコール | WATCH | 36.78 | 40.58 | -8.93 | Individual:固有下落(市場+0.6%) |

## Worst 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04-07 | 36560 | ＫＬａｂ | REJECT | -33.23 | 18.60 | -39.02 | News:不祥事検出 |
| 2026-04-08 | 36560 | ＫＬａｂ | REJECT | -32.12 | 17.88 | -39.39 | News:不祥事検出 |
| 2026-04-10 | 15140 | 住石ホールディングス | WATCH | -31.83 | 4.04 | -32.53 | Individual:固有下落(市場-0.7%) |
| 2026-04-13 | 15140 | 住石ホールディングス | REJECT | -30.94 | 6.12 | -31.18 | News:ストップ安検出 |
| 2026-04-09 | 15140 | 住石ホールディングス | WATCH | -30.50 | 3.44 | -31.31 | Individual:固有下落(市場+1.8%) |
| 2026-04-06 | 36560 | ＫＬａｂ | REJECT | -29.78 | 21.94 | -37.30 | News:不祥事検出 |
| 2026-04-09 | 21480 | アイティメディア | REJECT | -27.60 | 0.75 | -29.97 | News:減配検出 |
| 2026-04-13 | 21480 | アイティメディア | REJECT | -25.84 | 2.22 | -28.95 | News:下方修正検出 |
| 2026-04-10 | 21480 | アイティメディア | WATCH | -25.03 | 3.87 | -27.81 | Individual:固有下落(市場-0.7%) |
| 2026-04-10 | 36560 | ＫＬａｂ | REJECT | -23.86 | 27.12 | -34.64 | News:下方修正検出 |
