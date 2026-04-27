@echo off
setlocal
cd /d "%~dp0"
title Portfolio Agent
echo Starting Portfolio Agent...
echo.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0tools\run-app.ps1"
set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    echo.
    echo Portfolio Agent stopped with error code %EXIT_CODE%.
    pause
)
exit /b %EXIT_CODE%
