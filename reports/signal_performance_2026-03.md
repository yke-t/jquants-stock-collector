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

## Market Sentiment by Date
| signal_date | market_sentiment | market_bucket |
| --- | --- | --- |
| 2026-03-02 | 0.76 | bullish |
| 2026-03-03 | 0.67 | bullish |
| 2026-03-04 | 0.53 | bullish |
| 2026-03-05 | 0.62 | bullish |
| 2026-03-06 | 0.62 | bullish |
| 2026-03-09 | 0.50 | neutral |
| 2026-03-11 | 0.60 | bullish |
| 2026-03-12 | 0.50 | bullish |
| 2026-03-13 | 0.46 | neutral |
| 2026-03-17 | 0.48 | neutral |
| 2026-03-18 | 0.59 | bullish |
| 2026-03-19 | 0.43 | neutral |
| 2026-03-25 | 0.39 | bearish |
| 2026-03-26 | 0.38 | bearish |
| 2026-03-27 | 0.39 | bearish |

## Market Bucket x Verdict Summary
| market_bucket | verdict | count | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| bearish | WATCH | 13 | 6.84 | 6.06 | 69.23 | 100.00 | 7.69 |
| bearish | REJECT | 47 | 12.14 | 6.70 | 72.34 | 95.74 | 14.89 |
| neutral | ENTRY | 5 | 6.06 | 7.69 | 60.00 | 60.00 | 0.00 |
| neutral | WATCH | 13 | 4.14 | 0.74 | 53.85 | 76.92 | 30.77 |
| neutral | REJECT | 62 | 4.52 | 4.65 | 62.90 | 88.71 | 12.90 |
| bullish | ENTRY | 21 | 0.54 | -1.72 | 47.62 | 80.95 | 4.76 |
| bullish | WATCH | 6 | -2.37 | -2.19 | 50.00 | 100.00 | 16.67 |
| bullish | REJECT | 133 | -0.60 | -1.40 | 44.36 | 87.22 | 10.53 |

## Market Bucket x MA25 Bucket x Verdict Summary
| market_bucket | ma25_bucket | verdict | count | avg_ma25_rate_pct | avg_return_pct | median_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bearish | <=-8 | WATCH | 13 | -35.34 | 6.84 | 6.06 | 69.23 | 100.00 | 7.69 |
| bearish | <=-8 | REJECT | 47 | -25.85 | 12.14 | 6.70 | 72.34 | 95.74 | 14.89 |
| neutral | <=-8 | ENTRY | 5 | -17.39 | 6.06 | 7.69 | 60.00 | 60.00 | 0.00 |
| neutral | <=-8 | WATCH | 13 | -15.17 | 4.14 | 0.74 | 53.85 | 76.92 | 30.77 |
| neutral | <=-8 | REJECT | 62 | -16.25 | 4.52 | 4.65 | 62.90 | 88.71 | 12.90 |
| bullish | <=-8 | ENTRY | 19 | -11.17 | 0.84 | -1.72 | 47.37 | 78.95 | 5.26 |
| bullish | <=-8 | WATCH | 6 | -12.46 | -2.37 | -2.19 | 50.00 | 100.00 | 16.67 |
| bullish | <=-8 | REJECT | 122 | -14.67 | -0.20 | -0.97 | 45.08 | 86.89 | 10.66 |
| bullish | -8..-5 | ENTRY | 2 | -6.11 | -2.25 | -2.25 | 50.00 | 100.00 | 0.00 |
| bullish | -8..-5 | REJECT | 11 | -6.47 | -5.02 | -6.87 | 36.36 | 90.91 | 9.09 |

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
