@echo off
REM Start the EDINET server with a clean cache — ensures portfolio routes are registered.
REM Run from project root: tools\start_fresh.bat

echo Killing existing Python processes...
taskkill /F /IM python.exe 2>nul
taskkill /F /IM pythonw.exe 2>nul
timeout /t 2 /nobreak >nul

echo Clearing Python caches...
for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d" 2>nul
del /s /q *.pyc 2>nul

echo.
echo Starting server...
echo Look for: "Portfolio API router explicitly registered"
echo Then test: http://127.0.0.1:8000/portfolio
echo.

python -m src.web_app.server
