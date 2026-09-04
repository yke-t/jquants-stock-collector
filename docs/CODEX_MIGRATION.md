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

## J-Quants credential rotation

J-Quants V2 uses one `JQUANTS_API_KEY` value from the ignored repository
`.env`. V1 mail/password and refresh-token credentials are not accepted as
fallbacks. The 17:00 daily price task currently uses yfinance; the key is used
by J-Quants collection paths including the 18:00 dividend-financial sync.

Rotate a key outside the scheduled workflow windows:

1. Sign in to the official J-Quants dashboard and issue or regenerate the V2
   API key. Do not paste the key into Codex chat, a command-line argument, or a
   tracked file.
2. From the repository root in an interactive PowerShell, run:

   ```powershell
   python scripts\rotate_jquants_api_key.py
   ```

3. Paste the new key at the hidden prompt. The script makes one read-only
   request to `https://api.jquants.com/v2/equities/master`, refuses an invalid
   key, acquires the scheduled workflows' shared mutex, and atomically replaces
   only `JQUANTS_API_KEY` in `.env`.
4. Confirm the next `NISA-JQuant Dividend Daily` task result, the
   `dividend_operation.log` terminal marker, `sync_progress`, and the produced
   CSV before calling the rotated workflow operational.

The rotation script does not create a plaintext `.env` backup because doing so
would retain the superseded credential. Issuance/revocation is controlled by
the J-Quants dashboard; the script only validates and installs the newly issued
key locally.

The exposed historical key was regenerated on 2026-08-28. The dashboard
confirmed that regeneration makes the prior key unavailable, and the new
43-character key was installed as the single `JQUANTS_API_KEY` definition in
the ignored repository `.env` without a plaintext backup. Read-only live
checks returned HTTP 200 from both `/v2/equities/master?code=86970` and the
production client's `/v2/fins/summary?code=86970` path; the latter returned 12
financial-summary rows. This proves the rotated credential and the dividend
client authentication path, but the next complete 18:00 scheduled workflow
must still be checked separately before making a fresh end-to-end operational
claim.

Post-rotation scheduled evidence was checked on 2026-09-04. The dividend task
completed on both 2026-08-28 and 2026-08-31 with the rotated key. Subsequent
J-Quants collection also continued through 2026-09-03: `sync_progress` reached
that date, 2,500 code checkpoints advanced after rotation, and
`dividend_financials` reached 36,032 rows across 3,841 codes. This confirms that
the rotated J-Quants credential is operational in the scheduled workflow.

The 2026-09-01 through 2026-09-03 dividend failures, and the 2026-09-03 daily
failure, occurred later at Google Sheets `open_by_key` with HTTP 503. A
read-only retry outside the schedule succeeded and found 169 worksheets, so
the failure is not a J-Quants regression or a permanent Sheets authorization
failure. Google Sheets operations now retry only HTTP 429/500/502/503/504 and
connection timeouts, up to five attempts with 2/4/8/16-second delays. Permission
and configuration failures still fail immediately. The retry behavior is
offline-tested; its first scheduled write remains operationally unverified
until the next task result and log terminal marker are inspected.

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

All three batch entry points delegate their outer invocation to
`scripts\run_with_lock.ps1`. The runner uses one global Windows named mutex so
manual launches and different scheduled workflows cannot run concurrently. A
contending launch does not start the batch body; it appends a `[SKIP]` line to
that workflow's log and returns exit code `75`. The lock prevents concurrent
execution, but intentionally does not block a later explicit retry after the
first process has finished.

Before starting a workflow, the same runner rotates its operation log when it
is at least 10 MiB. Rotated logs use timestamped `*.log` names and the newest
five archives are retained. Logs remain ignored by Git.

Before changing a definition, the script exports the affected tasks beneath
the current user's `Documents\Codex Backups\jquants-stock-collector`
directory. It also preserves the daily task's principal, power policy, and
`IgnoreNew` behavior for the dividend task. If the existing daily task stores
Windows credentials for unattended execution, the script prompts once for the
Windows account password (not a Windows Hello PIN). The password is passed
only to Windows Task Scheduler and is never written to the backup or result
files.

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

## Antigravity cleanup status

Project-specific Antigravity workspace state, code-tracker snapshots, IDE file
history, conversation history, and project registration were removed on
2026-08-22. The cleanup removed 390 files (23,133,061 bytes), scrubbed ten
project references from the shared VS Code-style state databases, and removed
two project-specific rows from a shared conversation database. The local
cleanup manifest is stored outside the repository under
`Documents\Codex Backups\jquants-stock-collector\antigravity-cleanup-20260822`.

The active repository `.env`, `stock_data.db`, Windows Task Scheduler entries,
and histories belonging to other Antigravity projects were preserved. Two
shared `agyhub_summaries_proto.pb` cache files still contain a project-name
reference because they cannot be edited per project without risking unrelated
summaries. They are not an execution path. Remove the entire Antigravity user
data roots only if Antigravity is being retired for every project.

Historical `.env` snapshots included J-Quants credentials. Those snapshots
were deleted, but any credential that appeared in Antigravity history should
still be rotated at its issuing service; local deletion does not revoke it or
remove copies held by backups or synchronization services.
