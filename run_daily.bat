@echo off
cd /d %~dp0

:: --- Config ---
:: ログファイル名（追記モード）
set LOGFILE=daily_operation.log

echo ======================================================== >> %LOGFILE%
echo [START] Daily Routine: %date% %time% >> %LOGFILE%

:: 1. Activate venv (もし仮想環境を使用している場合、remを外してパスを調整してください)
:: call .venv\Scripts\activate.bat

:: 2. Data Fetch (Resume機能により最新分のみ取得されます)
echo [STEP 1] Fetching Market Data... >> %LOGFILE%
python main.py >> %LOGFILE% 2>&1

:: 3. Signal Scan (市場環境判定とシグナル生成)
echo [STEP 2] Scanning Signals... >> %LOGFILE%
python -m src.scan >> %LOGFILE% 2>&1

echo [END] Finished: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%

:: (オプション) エラー等を目視したい場合は以下のremを外す
:: pause
