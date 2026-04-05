#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$APP_DIR"

if [[ ! -d ".venv" ]]; then
  echo "[setup] Creating virtual environment..."
  python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if ! python -c "import flask" >/dev/null 2>&1; then
  echo "[setup] Installing dependencies..."
  python -m pip install -r requirements.txt
fi

echo "[run] Starting Kitchen Ops Dashboard on http://127.0.0.1:5000"
exec python app.py
