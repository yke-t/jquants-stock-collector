@echo off
setlocal
cd /d %~dp0

set LOGFILE=dividend_operation.log
set PYTHONIOENCODING=utf-8
if exist ".venv\Scripts\python.exe" (
    set "PYTHON=.venv\Scripts\python.exe"
) else (
    set "PYTHON=python"
)


echo ======================================================== >> %LOGFILE%
echo [START] Dividend Routine: %date% %time% >> %LOGFILE%

echo [STEP 1] Syncing dividend financials... >> %LOGFILE%
"%PYTHON%" src\financial_collector.py --all-codes --stale-days 7 --limit 500 --sleep 0.2 >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [STEP 2] Scanning dividend candidates... >> %LOGFILE%
"%PYTHON%" src\dividend_scan.py --limit 50 --with-news --notify >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [STEP 3] Exporting dividend candidate CSV to Google Drive... >> %LOGFILE%
"%PYTHON%" src\export_drive_spreadsheet.py --latest-prefix dividend_candidates --as-tab >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [STEP 4] Running monthly dividend backtest... >> %LOGFILE%
"%PYTHON%" src\dividend_backtest.py --start 2025-01-01 --top-n 20 >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [STEP 5] Exporting backtest CSV to Google Drive... >> %LOGFILE%
"%PYTHON%" src\export_drive_spreadsheet.py --latest-prefix dividend_backtest_monthly --as-tab >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [END] Dividend Routine finished: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b 0

:error
set EXITCODE=%ERRORLEVEL%
echo [ERROR] Dividend Routine failed with exit code %EXITCODE%: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b %EXITCODE%
