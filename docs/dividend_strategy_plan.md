# Long-Term Dividend Strategy Plan

This strategy is separate from the short-term dip-entry scanner. Its purpose is
to maintain a long-term income watchlist, not to force daily buy signals.

## Goal

Find stocks that can be held for 3-5 years with acceptable dividend
sustainability, balance-sheet strength, liquidity, and price discipline.

## Initial Requirements

- Dividend yield >= 3.5%
- Dividend yield > 7.0% is treated as a risk warning
- EPS > 0
- Profit > 0
- Payout ratio <= 70%
- Payout ratio > 100% is avoid
- Equity ratio >= 30%
- 20-day average volume >= 10,000 shares
- Share price <= 10,000 yen
- Close <= MA75 * 1.05
- Close < MA200 * 0.90 is treated as a deep-downtrend warning

## Verdicts

- `BUY_ZONE`: quality dividend candidate and price is acceptable
- `WATCH`: usable watchlist candidate, but one or more cautions exist
- `AVOID`: dividend sustainability or profitability is poor
- `DATA_MISSING`: required financial data is not synced yet

## Data Flow

1. Sync J-Quants `/fins/summary` rows into `dividend_financials`.
2. Preserve raw API rows in `raw_json`.
3. Normalize the fields needed by the scanner:
   - dividend per share
   - forecast dividend per share
   - EPS
   - forecast EPS
   - profit
   - equity
   - total assets
4. Join the latest financial snapshot to the latest price data.
5. Calculate yield, payout ratio, equity ratio, MA75, MA200, and average volume.
6. Write a daily CSV under `reports/`.

## Commands

Inspect J-Quants response fields:

```bash
python src/financial_collector.py --code 7203 --show-fields
```

Sync one code:

```bash
python src/financial_collector.py --code 7203
```

Sync a trial batch from `fundamentals`:

```bash
python src/financial_collector.py --all-codes --missing-only --limit 100 --sleep 0.2
```

Run the dividend scanner:

```bash
python src/dividend_scan.py --limit 50
```

Run the scanner with dividend-risk news checks:

```bash
python src/dividend_scan.py --limit 50 --with-news
```

Update the dedicated Google Sheets tab:

```bash
python src/dividend_scan.py --limit 50 --notify
```

Run the monthly rebalance backtest:

```bash
python src/dividend_backtest.py --start 2025-01-01 --top-n 20
```

Export a CSV report as a native Google Sheets file on Google Drive:

```bash
python src/export_drive_spreadsheet.py --latest-prefix dividend_candidates --as-tab
python src/export_drive_spreadsheet.py --csv reports/dividend_backtest_monthly.csv
```

To keep CSV exports separate from the signal notification spreadsheet, create a
new Google Sheets file, share it with the service account as Editor, and set:

```text
GOOGLE_DRIVE_EXPORT_SPREADSHEET_KEY=<new spreadsheet id>
```

Run the daily dividend operation:

```bash
run_dividend_daily.bat
```

## Current Operational Notes

- `--missing-only` avoids re-fetching codes already present in
  `dividend_financials`.
- The Google CSE news risk check only accepts hits where the title appears to
  reference the target company, reducing broad keyword false positives.
- `run_dividend_daily.bat` syncs up to 500 missing codes, scans the top 50
  dividend candidates, updates Google Sheets, exports CSV reports to Google
  Drive as native Sheets tabs, and writes a monthly backtest CSV.

## Next Steps

- Run a broader financial sync once API rate limits are acceptable.
- Review scanner output after a broader financial sync and tune thresholds.
- Run the backtest again after the financial table is sufficiently populated.
- Improve the backtest with exact dividend ex-dates when that data is available.
