#!/usr/bin/env bash
# ─────────────────────────────────────────
# ASK MY CFO — M1 Automation
# ─────────────────────────────────────────
# One-command startup: installs dependencies and launches the web GUI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║   ASK MY CFO — M1 Automation                    ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "✗ Python 3 not found. Please install Python 3.9+."
    exit 1
fi

PYTHON=python3
PIP="$PYTHON -m pip"

# Install dependencies
echo "→ Installing dependencies..."
$PIP install -r requirements.txt --break-system-packages -q 2>/dev/null || \
$PIP install -r requirements.txt -q

echo ""
echo "→ Starting web server..."
echo "  Open http://localhost:5000 in your browser"
echo ""
echo "  Press Ctrl+C to stop"
echo "──────────────────────────────────────────────────"
echo ""

$PYTHON app.py
