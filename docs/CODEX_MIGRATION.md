# Codex Migration

## Target state

Codex is the primary environment for source changes, review, local tests, and
evidence-backed diagnostics. The market-data pipeline remains deterministic
Python invoked by Windows Task Scheduler until its operational blockers are
fixed and revalidated.

## Local setup

1. Open the repository as a trusted local Codex project.
2. Use Python 3.11.
3. Create and activate a virtual environment.
4. Install `requirements.lock.txt`.
5. Copy `.env.example` to `.env` and provide local values.
6. Place the service-account JSON at the configured
   `GOOGLE_SERVICE_ACCOUNT_FILE`; never commit it.
7. Run `python scripts/verify_project.py --with-db`.

Keep command networking disabled by default. Enable live network access only
for a task that explicitly needs J-Quants, yfinance, Google CSE, Sheets, Drive,
or BigQuery.

## What belongs where

- Durable repository behavior and verification rules: `AGENTS.md`.
- User-level Codex sandbox defaults: `~/.codex/config.toml`.
- Non-secret examples: `.env.example`.
- Actual credentials and resource IDs: ignored `.env` and service-account JSON.
- Generated datasets and logs: ignored local files or an explicitly managed
  external data location.

## Scheduling policy

Keep `run_daily.bat` and `run_dividend_daily.bat` under Windows Task Scheduler
for now. A Codex scheduled task may monitor logs or summarize failures, but it
must not be the only mechanism running the trading-data pipeline yet.

Codex worktrees do not automatically contain ignored files such as
`stock_data.db`, `.env`, or `secret_key.json`. Use the main local checkout for
DB-aware work, or move data/configuration to explicitly configured shared paths
before adopting worktree-based scheduled execution.

## Operational blockers

- Dividend scans and backtests now multiply per-share financial values by
  explicit point-in-time adjustment factors. Large price discontinuities with
  no factor are blocked as `DATA_WARNING` instead of being normalized by
  inference. `src/split_factor_backfill.py` provides a guarded dry-run/apply
  workflow for each ignored local DB: it verifies a SQLite backup and matching
  target rows before applying exact J-Quants repairs in one transaction.
- The recurring dividend sync uses `--missing-only`, so already-covered stocks
  are not refreshed.
- Several external-operation failures are logged without a non-zero process
  exit code.
- Live integration tests are not part of the offline unit suite.

Until the factor backfill and remaining items are validated with real outputs,
report the pipeline as locally executable with split-sensitive names safely
blocked, but not yet fully operational.
