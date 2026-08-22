@echo off
setlocal
cd /d %~dp0

:: --- Config ---
:: ログファイル名（追記モード）
set LOGFILE=daily_operation.log
set PYTHONIOENCODING=utf-8
if exist ".venv\Scripts\python.exe" (
    set "PYTHON=.venv\Scripts\python.exe"
) else (
    set "PYTHON=python"
)


:: データソース選択: jquants または yfinance
:: J-Quants解約後は USE_YFINANCE=1 に変更してください
set USE_YFINANCE=1

echo ======================================================== >> %LOGFILE%
echo [START] Daily Routine: %date% %time% >> %LOGFILE%


:: 2. Data Fetch
echo [STEP 1] Fetching Market Data... >> %LOGFILE%
if "%USE_YFINANCE%"=="1" (
    echo [INFO] Using yfinance data source >> %LOGFILE%
    "%PYTHON%" -m src.update_yfinance >> %LOGFILE% 2>&1
    if errorlevel 1 goto error
) else (
    echo [INFO] Using J-Quants data source >> %LOGFILE%
    "%PYTHON%" main.py >> %LOGFILE% 2>&1
    if errorlevel 1 goto error
)

:: 3. Signal Scan (市場環境判定とシグナル生成)
echo [STEP 2] Scanning Signals... >> %LOGFILE%
"%PYTHON%" -m src.scan >> %LOGFILE% 2>&1
if errorlevel 1 goto error

:: 4. BigQuery Sync (差分更新)
echo [STEP 3] Syncing to BigQuery... >> %LOGFILE%
"%PYTHON%" -m src.sync_bigquery >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [END] Finished: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b 0

:error
set EXITCODE=%ERRORLEVEL%
echo [ERROR] Daily Routine failed with exit code %EXITCODE%: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b %EXITCODE%

:: (オプション) エラー等を目視したい場合は以下のremを外す
:: pause
