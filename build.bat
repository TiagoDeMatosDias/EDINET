@echo off
rem Double-click to build release\windows\ShadeResearch.exe (see docs\BUILDING.md).
rem
rem The first run creates .venv-build\windows with the CPU build of torch and the
rem build dependencies; later runs reinstall them only when pyproject.toml or
rem constraints.txt changed. Arguments are passed on to scripts\build.py.
setlocal
cd /d "%~dp0"
set "VENV=.venv-build\windows"
set "PYTHON=%VENV%\Scripts\python.exe"

if exist "%PYTHON%" goto dependencies
echo Creating %VENV%
py -3.13 -m venv "%VENV%" 2>nul
if not exist "%PYTHON%" py -3.12 -m venv "%VENV%" 2>nul
if not exist "%PYTHON%" python -m venv "%VENV%"
if not exist "%PYTHON%" (
    echo Python 3.13 or 3.12 is required: install it from python.org.
    goto failed
)

:dependencies
fc /b pyproject.toml "%VENV%\pyproject.toml" >nul 2>&1 || goto install
fc /b constraints.txt "%VENV%\constraints.txt" >nul 2>&1 || goto install
goto build

:install
"%PYTHON%" -m pip install torch --index-url https://download.pytorch.org/whl/cpu || goto failed
"%PYTHON%" -m pip install -e ".[build]" -c constraints.txt || goto failed
rem pip leaves this beside the source; the environment keeps its own record.
if exist edinet_workstation.egg-info rmdir /s /q edinet_workstation.egg-info
copy /y pyproject.toml "%VENV%\" >nul
copy /y constraints.txt "%VENV%\" >nul

:build
"%PYTHON%" -B scripts\build.py %* || goto failed
echo.
echo Done.
pause
exit /b 0

:failed
echo.
echo FAILED.
pause
exit /b 1
