# Signal Performance Report: 2026-04

## Evaluation Rules
- Entry price: next trading day's open after `signal_date`.
- Evaluation window: 20 future trading days.
- Incomplete signals are kept in diagnostics but excluded from performance aggregates.
- `signal_price` sanity check: 0.2 <= signal_price / next_open <= 5.0.

## Data Quality
- total_signals: 260
- evaluated_complete: 100
- incomplete_or_missing_price: 160
- invalid_signal_price: 0

## Verdict Summary
| verdict | signals | evaluated | incomplete | avg_return_pct | median_return_pct | win_rate_pct | avg_max_gain_pct | avg_max_loss_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY | 9 | 2 | 7 | 6.09 | 6.09 | 50.00 | 14.50 | -8.01 | 50.00 | 50.00 |
| WATCH | 35 | 11 | 24 | 8.33 | 12.94 | 54.55 | 15.01 | -7.87 | 54.55 | 63.64 |
| REJECT | 216 | 87 | 129 | 9.62 | 1.04 | 52.87 | 30.07 | -8.52 | 52.87 | 42.53 |

## Month x Verdict Summary
| month | verdict | evaluated | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04 | ENTRY | 2 | 6.09 | 6.09 | 50.00 | 50.00 | 50.00 |
| 2026-04 | REJECT | 87 | 9.62 | 1.04 | 52.87 | 52.87 | 42.53 |
| 2026-04 | WATCH | 11 | 8.33 | 12.94 | 54.55 | 54.55 | 63.64 |

## Invalid signal_price Sample
_No data_

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
| 2026-04-06 | 36560 | ＫＬａｂ | REJECT | -29.78 | 21.94 | -37.30 | News:不祥事検出 |
| 2026-04-08 | 21480 | アイティメディア | REJECT | -20.35 | 0.31 | -20.35 | News:減配検出 |
| 2026-04-08 | 15140 | 住石ホールディングス | WATCH | -18.80 | 1.00 | -19.47 | Individual:固有下落(市場-0.7%) |
| 2026-04-08 | 70130 | ＩＨＩ | REJECT | -18.55 | 0.87 | -19.35 | News:減配検出 |
| 2026-04-07 | 21480 | アイティメディア | REJECT | -17.81 | 2.54 | -17.81 | News:減配検出 |
| 2026-04-01 | 70130 | ＩＨＩ | REJECT | -17.59 | 2.22 | -18.80 | News:ストップ安検出 |
| 2026-04-07 | 70130 | ＩＨＩ | REJECT | -17.41 | 0.55 | -19.32 | News:減配検出 |
| 2026-04-08 | 54510 | ヨドコウ | REJECT | -15.02 | 0.28 | -16.22 | News:減配検出 |
