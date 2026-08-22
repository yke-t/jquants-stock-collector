# Codex Migration

## Target state

Codex is the primary environment for source changes, review, local tests, and
evidence-backed diagnostics. The market-data pipeline remains deterministic
Python invoked from the main checkout by Windows Task Scheduler.

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

Keep `run_daily.bat`, `run_dividend_daily.bat`, and `run_monthly_eval.bat`
under Windows Task Scheduler. A Codex scheduled task may monitor logs or
summarize failures, but it must not be the only mechanism running the
trading-data pipeline.

Run the repository-managed configuration script from an administrator
PowerShell to install or reconcile the task definitions:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\configure_task_scheduler.ps1
```

The script configures these local tasks without starting any workflow:

- `NISA-JQuant Daily`: Monday through Friday at 17:00.
- `NISA-JQuant Dividend Daily`: Monday through Friday at 18:00.
- `SnowMoney_Monthly_Eval`: retained without modification.

Before changing a definition, the script exports the affected tasks beneath
the current user's `Documents\Codex Backups\jquants-stock-collector`
directory. It also preserves the daily task's principal, power policy, and
`IgnoreNew` behavior for the dividend task.

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
- The recurring dividend sync rotates through missing and stale codes with
  `--stale-days 7 --limit 500`, oldest refresh first. Successful empty
  responses are recorded in `sync_progress` so no-data codes do not starve the
  queue.
- Batch entry points return non-zero for unavailable input data, partial local
  saves, J-Quants/yfinance fetch failures, and failed Sheets or BigQuery writes,
  allowing Task Scheduler to stop instead of exporting stale artifacts.
- Live integration tests are not part of the offline unit suite.

The split-sensitive dividend paths and external output paths have been
revalidated with live outputs. Future operational changes still require an
actual command result, relevant DB rows or dates, and produced-artifact checks;
offline tests alone are not sufficient evidence.
