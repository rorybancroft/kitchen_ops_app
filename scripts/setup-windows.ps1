$ErrorActionPreference = 'Stop'

Write-Host "[Kitchen Ops] Creating virtual environment..."
python -m venv .venv

Write-Host "[Kitchen Ops] Activating virtual environment..."
& .\.venv\Scripts\Activate.ps1

Write-Host "[Kitchen Ops] Installing requirements..."
pip install -r requirements.txt

Write-Host "[Kitchen Ops] Setup complete."
Write-Host "Run next: powershell -ExecutionPolicy Bypass -File .\scripts\run-windows.ps1"
