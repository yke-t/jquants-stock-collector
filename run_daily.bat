@echo off
cd /d %~dp0

:: --- Config ---
:: ログファイル名（追記モード）
set LOGFILE=daily_operation.log

:: データソース選択: jquants または yfinance
:: J-Quants解約後は USE_YFINANCE=1 に変更してください
set USE_YFINANCE=0

echo ======================================================== >> %LOGFILE%
echo [START] Daily Routine: %date% %time% >> %LOGFILE%

:: 1. Activate venv (もし仮想環境を使用している場合、remを外してパスを調整してください)
:: call .venv\Scripts\activate.bat

:: 2. Data Fetch
echo [STEP 1] Fetching Market Data... >> %LOGFILE%
if "%USE_YFINANCE%"=="1" (
    echo [INFO] Using yfinance data source >> %LOGFILE%
    python -m src.update_yfinance >> %LOGFILE% 2>&1
) else (
    echo [INFO] Using J-Quants data source >> %LOGFILE%
    python main.py >> %LOGFILE% 2>&1
)

:: 3. Signal Scan (市場環境判定とシグナル生成)
echo [STEP 2] Scanning Signals... >> %LOGFILE%
python -m src.scan >> %LOGFILE% 2>&1

echo [END] Finished: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%

:: (オプション) エラー等を目視したい場合は以下のremを外す
:: pause
