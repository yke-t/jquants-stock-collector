# Signal Performance Report: 2026-03

## Evaluation Rules
- Entry price: next trading day's open after `signal_date`.
- Evaluation window: 20 future trading days.
- Incomplete signals are kept in diagnostics but excluded from performance aggregates.
- `signal_price` sanity check: 0.2 <= signal_price / next_open <= 5.0.

## Data Quality
- total_signals: 301
- evaluated_complete: 300
- incomplete_or_missing_price: 1
- invalid_signal_price: 4

## Verdict Summary
| verdict | signals | evaluated | incomplete | avg_return_pct | median_return_pct | win_rate_pct | avg_max_gain_pct | avg_max_loss_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ENTRY | 27 | 26 | 1 | 1.61 | -0.59 | 50.00 | 7.76 | -7.24 | 76.92 | 3.85 |
| WATCH | 32 | 32 | 0 | 4.02 | 1.50 | 59.38 | 14.04 | -8.64 | 90.62 | 18.75 |
| REJECT | 242 | 242 | 0 | 3.19 | 1.06 | 54.55 | 13.41 | -9.83 | 89.26 | 11.98 |

## Month x Verdict Summary
| month | verdict | evaluated | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-03 | ENTRY | 26 | 1.61 | -0.59 | 50.00 | 76.92 | 3.85 |
| 2026-03 | REJECT | 242 | 3.19 | 1.06 | 54.55 | 89.26 | 11.98 |
| 2026-03 | WATCH | 32 | 4.02 | 1.50 | 59.38 | 90.62 | 18.75 |

## Invalid signal_price Sample
| signal_date | code | name | verdict | signal_price | next_open | signal_price_ratio |
| --- | --- | --- | --- | --- | --- | --- |
| 2026-03-19 | 40220 | ラサ工業 | REJECT | 7900.00 | 1419.80 | 5.56 |
| 2026-03-19 | 64060 | フジテック | ENTRY | 5680.00 | 37097512960.00 | 0.00 |
| 2026-03-25 | 40220 | ラサ工業 | REJECT | 8240.00 | 1634.17 | 5.04 |
| 2026-03-26 | 40220 | ラサ工業 | REJECT | 8030.00 | 1555.22 | 5.16 |

## Best 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-03-26 | 31030 | ユニチカ | REJECT | 155.70 | 292.83 | -9.87 | News:ストップ安検出 |
| 2026-03-27 | 31030 | ユニチカ | REJECT | 127.19 | 287.95 | -10.98 | News:ストップ安検出 |
| 2026-03-25 | 69970 | 日本ケミコン | REJECT | 68.99 | 72.03 | -4.05 | News:下方修正検出 |
| 2026-03-17 | 31030 | ユニチカ | REJECT | 66.35 | 102.64 | -19.66 | News:下方修正検出 |
| 2026-03-27 | 40220 | ラサ工業 | REJECT | 43.28 | 43.56 | 0.00 | News:減配検出 |
| 2026-03-27 | 62640 | マルマエ | WATCH | 40.58 | 44.41 | -0.71 | Individual:固有下落(市場-0.4%) |
| 2026-03-06 | 62690 | 三井海洋開発 | ENTRY | 31.94 | 35.99 | -0.73 | Sector:連れ安(市場-5.2%) |
| 2026-03-17 | 65250 | ＫＯＫＵＳＡＩ　ＥＬＥＣＴＲＩＣ | WATCH | 31.77 | 41.65 | -9.43 | Individual:固有下落(市場+2.9%) |
| 2026-03-06 | 54610 | 中部鋼鈑 | REJECT | 30.11 | 33.70 | -0.73 | News:下方修正検出 |
| 2026-03-04 | 38530 | アステリア | REJECT | 28.84 | 45.38 | -3.80 | News:ストップ安検出 |

## Worst 10
| signal_date | code | name | verdict | return_pct | max_gain | max_loss | reason |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-03-04 | 36810 | ブイキューブ | REJECT | -58.33 | 10.83 | -61.67 | News:下方修正検出 |
| 2026-03-03 | 36810 | ブイキューブ | REJECT | -42.15 | 9.92 | -42.15 | News:下方修正検出 |
| 2026-03-02 | 65900 | 芝浦メカトロニクス | REJECT | -30.46 | 6.16 | -30.81 | News:下方修正検出 |
| 2026-03-04 | 57070 | 東邦亜鉛 | REJECT | -24.37 | 0.44 | -27.34 | News:下方修正検出 |
| 2026-03-02 | 63300 | 東洋エンジニアリング | REJECT | -23.59 | 7.21 | -28.56 | News:下方修正検出 |
| 2026-03-05 | 15150 | 日鉄鉱業 | WATCH | -20.88 | 2.58 | -26.66 | Individual:固有下落(市場+0.6%) |
| 2026-03-09 | 44610 | 第一工業製薬 | REJECT | -19.50 | 3.30 | -29.00 | News:減配検出 |
| 2026-03-02 | 63230 | ローツェ | REJECT | -19.16 | 1.01 | -22.57 | News:下方修正検出 |
| 2026-03-09 | 63300 | 東洋エンジニアリング | REJECT | -16.91 | 37.24 | -22.25 | News:下方修正検出 |
| 2026-03-06 | 63300 | 東洋エンジニアリング | REJECT | -16.71 | 38.12 | -17.96 | News:下方修正検出 |
