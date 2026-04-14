@echo off
REM ─────────────────────────────────────────
REM ASK MY CFO — M1 Automation
REM ─────────────────────────────────────────

cd /d "%~dp0"

echo.
echo ╔══════════════════════════════════════════════════╗
echo ║   ASK MY CFO — M1 Automation                    ║
echo ╚══════════════════════════════════════════════════╝
echo.

echo → Installing dependencies...
python -m pip install -r requirements.txt -q

echo.
echo → Starting web server...
echo   Open http://localhost:5000 in your browser
echo.
echo   Press Ctrl+C to stop
echo ──────────────────────────────────────────────────
echo.

python app.py
pause
