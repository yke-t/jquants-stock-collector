@echo off
setlocal
cd /d %~dp0

if "%JQUANTS_PIPELINE_LOCK_HELD%"=="1" goto run_workflow
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run_with_lock.ps1" -CommandPath "%~f0" -LogPath "%~dp0monthly_evaluation.log"
exit /b %ERRORLEVEL%

:run_workflow

:: --- Config ---
:: ログファイル名（追記モード）
set LOGFILE=monthly_evaluation.log
set PYTHONIOENCODING=utf-8
if exist ".venv\Scripts\python.exe" (
    set "PYTHON=.venv\Scripts\python.exe"
) else (
    set "PYTHON=python"
)


echo ======================================================== >> %LOGFILE%
echo [START] Monthly Strategy Evaluation: %date% %time% >> %LOGFILE%

:: 先月分のシグナルパフォーマンスを評価し、チャートを生成
echo [INFO] Running evaluation for the previous month... >> %LOGFILE%
"%PYTHON%" -m src.evaluate --prev-month --charts >> %LOGFILE% 2>&1
if errorlevel 1 goto error

echo [END] Finished Monthly Evaluation: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b 0

:error
set EXITCODE=%ERRORLEVEL%
echo [ERROR] Monthly Strategy Evaluation failed with exit code %EXITCODE%: %date% %time% >> %LOGFILE%
echo ======================================================== >> %LOGFILE%
exit /b %EXITCODE%
