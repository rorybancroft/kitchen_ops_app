$ErrorActionPreference = 'Stop'

if (-Not (Test-Path .\.venv\Scripts\Activate.ps1)) {
  Write-Host "Virtual environment not found. Run setup first: .\scripts\setup-windows.ps1"
  exit 1
}

& .\.venv\Scripts\Activate.ps1

Write-Host "[Kitchen Ops] Starting Flask app on http://127.0.0.1:5000"
python app.py
