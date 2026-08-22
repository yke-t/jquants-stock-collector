# J-Quants Stock Collector Guidance

## Scope

This repository collects Japanese equity data, generates trading and dividend
signals, evaluates them, and optionally writes results to Google Sheets and
BigQuery. Treat financial correctness and external side effects as higher risk
than ordinary source edits.

## Repository map

- `main.py`, `src/collector.py`, `src/client.py`: J-Quants price collection.
- `src/update_yfinance.py`: alternate daily price source.
- `src/scan.py`, `src/news_analyzer.py`, `src/notifier.py`: daily signal flow.
- `src/dividend_*.py`, `src/financial_collector.py`: long-term dividend flow.
- `src/evaluate.py`, `src/backtest*.py`: evaluation and backtests.
- `stock_data.db`: local production-like SQLite data; ignored by Git.
- `reports/`, `charts/`, `*.log`: generated artifacts.

## Environment and commands

- Supported local runtime: Python 3.11 on Windows/PowerShell.
- Install the verified environment with
  `python -m pip install -r requirements.lock.txt`.
- Run the offline project verification with `python scripts/verify_project.py`.
- Run the DB-aware read-only verification with
  `python scripts/verify_project.py --with-db`.
- The focused unit suite is:
  `python -m unittest tests.test_export_drive_spreadsheet tests.test_dividend_scan tests.test_dividend_backtest tests.test_financial_collector tests.test_news_analyzer tests.test_scan tests.test_evaluate tests.test_settings tests.test_update_yfinance tests.test_split_factor_backfill`.
- Integration tests under `tests/integration/` call external services. Run them
  only when the task explicitly requires live validation.

## Safety and side effects

- Do not print or commit values from `.env` or `secret_key.json`.
- Do not replace, delete, recreate, or bulk-rewrite `stock_data.db` without an
  explicit request and a verified backup/checkpoint.
- Treat Google Sheets, Google Drive, BigQuery, J-Quants, Google CSE, and
  yfinance calls as live external operations. State the exact command and
  expected target before running a command that writes remotely.
- Default reviews and tests must remain offline. Network access is opt-in.
- Do not run `run_daily.bat` or `run_dividend_daily.bat` as a generic test;
  both can modify the DB and external services.

## Financial correctness gates

- Never call a workflow operational from code inspection or unit tests alone.
  Check actual command exit status, relevant DB rows/dates, and produced
  artifacts.
- `prices.close`, J-Quants `C`/`AdjC`/`AdjFactor`, and per-share financial
  values must be kept on one share basis. Do not mix adjusted prices with
  pre-split dividend or EPS fields.
- Corporate-action changes require concrete regression checks for known cases,
  including codes `20030` and `19610`, using API raw fields, stored DB fields,
  and generated results.
- If the share basis cannot be verified, produce `DATA_WARNING`; do not guess a
  normalization factor or leave the stock in `BUY_ZONE`.
- `DivTotalAnn` and similarly ambiguous aggregate fields are not accepted as
  one-share annual dividends without source-field verification.
- Before changing yield, EPS, payout, corporate-action, or backtest math,
  confirm that an auditable Git checkpoint exists and keep the change isolated.

## Done criteria

- Relevant offline tests pass.
- `python -m pip check` passes.
- No secrets or generated DB/log/report files enter the Git diff.
- Documentation and `.env.example` match the active code paths.
- For operational claims, live output and data checks are attached as evidence;
  otherwise report the result as code-complete but operationally unverified.
