@echo off
rem VikingTalk launcher for Windows. Double-click it, or run: start.bat server, client, host or test
cd /d "%~dp0"
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY (
    python -c "import sys" >nul 2>nul && set "PY=python"
)
if not defined PY (
    echo Python 3 was not found.
    echo Install it from https://www.python.org/downloads/
    echo and tick "Add python.exe to PATH" during setup.
    pause
    exit /b 1
)
%PY% start.py %*
set "RC=%ERRORLEVEL%"
rem Keep the window open when started by double-click
echo %CMDCMDLINE% | find /i "%~nx0" >nul && pause
exit /b %RC%
