# Market Bucket x MA25 Bucket Cross Analysis

## Scope
- Source reports: `reports/signal_performance_2025-12.md` to `reports/signal_performance_2026-04.md`.
- Evaluation basis: next trading day open, 20 trading days, completed rows only.
- Aggregation: weighted by monthly bucket counts.
- Median is intentionally omitted in the cross-month table because only monthly summary medians are available in the source reports.

## Combined Cross Summary
| market_bucket | ma25_bucket | verdict | count | avg_ma25_rate_pct | avg_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| bearish | <=-8 | WATCH | 13 | -35.34 | 6.84 | 69.23 | 100.00 | 7.69 |
| bearish | <=-8 | REJECT | 47 | -25.85 | 12.14 | 72.34 | 95.74 | 14.89 |
| neutral | <=-8 | ENTRY | 7 | -22.98 | 6.07 | 57.14 | 57.14 | 14.29 |
| neutral | <=-8 | WATCH | 24 | -13.24 | 6.17 | 58.34 | 62.50 | 45.84 |
| neutral | <=-8 | REJECT | 164 | -14.51 | 5.61 | 54.27 | 67.07 | 29.88 |
| neutral | -8..-5 | WATCH | 9 | -6.31 | -13.25 | 0.00 | 100.00 | 22.22 |
| neutral | -8..-5 | REJECT | 16 | -6.79 | -3.37 | 25.00 | 81.25 | 31.25 |
| bullish | <=-8 | ENTRY | 27 | -11.13 | 0.29 | 51.85 | 74.08 | 7.41 |
| bullish | <=-8 | WATCH | 41 | -11.06 | -0.34 | 48.78 | 65.85 | 26.83 |
| bullish | <=-8 | REJECT | 290 | -14.87 | -0.87 | 40.34 | 78.63 | 15.18 |
| bullish | -8..-5 | ENTRY | 11 | -6.59 | 3.83 | 54.55 | 63.64 | 36.36 |
| bullish | -8..-5 | WATCH | 16 | -6.46 | -0.21 | 37.50 | 68.75 | 31.25 |
| bullish | -8..-5 | REJECT | 81 | -6.51 | 1.42 | 53.08 | 58.02 | 46.91 |
| bullish | -5..-3 | ENTRY | 8 | -4.26 | 2.60 | 62.50 | 50.00 | 50.00 |
| bullish | -5..-3 | WATCH | 5 | -3.97 | -2.42 | 40.00 | 40.00 | 40.00 |
| bullish | -5..-3 | REJECT | 41 | -4.28 | 1.00 | 39.02 | 68.29 | 41.46 |

## ENTRY Only
| market_bucket | ma25_bucket | verdict | count | avg_ma25_rate_pct | avg_return_pct | win_rate_pct | stop_hit_pct | take_hit_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| neutral | <=-8 | ENTRY | 7 | -22.98 | 6.07 | 57.14 | 57.14 | 14.29 |
| bullish | <=-8 | ENTRY | 27 | -11.13 | 0.29 | 51.85 | 74.08 | 7.41 |
| bullish | -8..-5 | ENTRY | 11 | -6.59 | 3.83 | 54.55 | 63.64 | 36.36 |
| bullish | -5..-3 | ENTRY | 8 | -4.26 | 2.60 | 62.50 | 50.00 | 50.00 |

## Observations
- `bearish x ENTRY` is absent, so the existing market filter is already preventing entries below the 0.40 threshold.
- `bullish x <=-8` ENTRY has the largest ENTRY sample: 27 samples, +0.29% weighted average return, 51.85% win rate, 74.08% stop-hit rate. This is not a clean buy signal despite bullish market conditions.
- `bullish x -8..-5` ENTRY has 11 samples, +3.83% weighted average return, 54.55% win rate, 63.64% stop-hit rate. It performs better than `<=-8`, but still carries high stop-hit risk.
- `bullish x -5..-3` ENTRY has 8 samples, +2.60% weighted average return, 62.50% win rate, 50.00% stop-hit rate. Sample size is small, but risk looks lower than deeper drops.
- `neutral x <=-8` ENTRY has 7 samples, +6.07% weighted average return, 57.14% win rate, 57.14% stop-hit rate. It is promising, but sample size is too small for a hard rule.
- WATCH and REJECT contain many positive-return rebounds in deep-drop buckets, supporting a future Rebound-Watch split rather than a simple hard reject.

## Recommended Next Action
- Do not change the market threshold yet; the loss pattern is not caused by bearish entries.
- Move to Priority 2A: add a Rebound-Watch classification for deep MA25 drops, especially `<=-8`, so panic-selling candidates can be monitored separately from hard rejects.
- For ENTRY tuning, prefer a guardrail experiment over a permanent rule: flag `bullish x <=-8` as high-risk and require an additional rebound confirmation before ENTRY.
