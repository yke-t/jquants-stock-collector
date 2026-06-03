# Signal Performance Report: 2026-02

## Evaluation Rules
- Entry price: next trading day's open after `signal_date`.
- Evaluation window: 20 future trading days.
- Incomplete signals are kept in diagnostics but excluded from performance aggregates.
- `signal_price` sanity check: 0.2 <= signal_price / next_open <= 5.0.

## Data Quality
- total_signals: 240
- evaluated_complete: 240
- incomplete_or_missing_price: 0
- invalid_signal_price: 0

## Verdict Summary
| verdict | signals | evaluated | incomplete | avg_return_pct | median_return_pct | win_rate_pct | avg_max_gain_pct | avg_max_loss_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY | 14 | 14 | 0 | -3.04 | -0.82 | 50.00 | 5.30 | -8.85 | 71.43 | 14.29 |
| WATCH | 41 | 41 | 0 | -0.34 | -1.72 | 41.46 | 7.75 | -7.58 | 63.41 | 26.83 |
| REJECT | 185 | 185 | 0 | -3.76 | -4.52 | 35.14 | 8.10 | -10.42 | 70.81 | 18.92 |

## Month x Verdict Summary
| month | verdict | evaluated | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-02 | ENTRY | 14 | -3.04 | -0.82 | 50.00 | 71.43 | 14.29 |
| 2026-02 | REJECT | 185 | -3.76 | -4.52 | 35.14 | 70.81 | 18.92 |
| 2026-02 | WATCH | 41 | -0.34 | -1.72 | 41.46 | 63.41 | 26.83 |

## Invalid signal_price Sample
_No data_

## Best 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-02-26 | 38530 | アステリア | REJECT | 27.59 | 42.77 | -9.31 | News:ストップ安検出 |
| 2026-02-24 | 38530 | アステリア | REJECT | 25.75 | 27.24 | -7.49 | News:ストップ安検出 |
| 2026-02-26 | 58440 | 京都フィナンシャルグループ | WATCH | 21.11 | 25.38 | -0.89 | Individual:固有下落(市場+0.2%) |
| 2026-02-25 | 58440 | 京都フィナンシャルグループ | REJECT | 20.61 | 27.48 | -0.82 | News:ストップ安検出 |
| 2026-02-26 | 62690 | 三井海洋開発 | WATCH | 18.18 | 25.59 | -8.32 | Individual:固有下落(市場+0.2%) |
| 2026-02-26 | 46760 | フジ・メディア・ホールディングス | WATCH | 16.02 | 16.86 | 0.00 | Individual:固有下落(市場+0.2%) |
| 2026-02-25 | 46760 | フジ・メディア・ホールディングス | WATCH | 15.57 | 15.57 | -0.69 | Individual:固有下落(市場+0.3%) |
| 2026-02-24 | 46760 | フジ・メディア・ホールディングス | REJECT | 15.44 | 16.16 | -1.65 | News:不祥事検出 |
| 2026-02-20 | 46760 | フジ・メディア・ホールディングス | REJECT | 14.60 | 16.77 | -1.01 | News:不祥事検出 |
| 2026-02-27 | 62690 | 三井海洋開発 | REJECT | 14.39 | 20.60 | -11.97 | News:下方修正検出 |

## Worst 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-02-12 | 63300 | 東洋エンジニアリング | REJECT | -34.19 | 13.25 | -41.47 | News:ストップ安検出 |
| 2026-02-16 | 63300 | 東洋エンジニアリング | REJECT | -31.22 | 4.44 | -46.02 | News:ストップ安検出 |
| 2026-02-25 | 63300 | 東洋エンジニアリング | REJECT | -27.10 | 0.28 | -33.18 | News:下方修正検出 |
| 2026-02-25 | 65900 | 芝浦メカトロニクス | REJECT | -26.32 | 2.64 | -33.64 | News:下方修正検出 |
| 2026-02-27 | 65900 | 芝浦メカトロニクス | REJECT | -22.26 | 10.04 | -26.50 | News:下方修正検出 |
| 2026-02-19 | 35650 | アセンテック | ENTRY | -21.71 | 0.30 | -24.70 | Normal:通常押し目(市場-1.1%) |
| 2026-02-26 | 65900 | 芝浦メカトロニクス | REJECT | -21.52 | 5.24 | -29.70 | News:下方修正検出 |
| 2026-02-20 | 57070 | 東邦亜鉛 | REJECT | -21.50 | 22.99 | -25.65 | News:下方修正検出 |
| 2026-02-27 | 63300 | 東洋エンジニアリング | REJECT | -20.38 | 5.19 | -29.90 | News:下方修正検出 |
| 2026-02-27 | 52020 | 日本板硝子 | REJECT | -19.83 | 4.00 | -32.83 | News:下方修正検出 |
