# Kitchen Ops — Windows Field Test (PowerShell)

## 1) Prerequisites (Windows tester)
- Python 3.10+ installed
- PowerShell

Verify:
```powershell
python --version
```

## 2) Unzip and open folder
```powershell
cd path\to\kitchen_ops_app
```

## 3) One-time setup
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup-windows.ps1
```

## 4) Run app
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run-windows.ps1
```

Open in browser:
- http://127.0.0.1:5000

---

## Notes
- App data is stored in `kitchen_ops.db` in the app folder.
- Upload artifacts/reports are stored in `uploads\`.
- To stop: press `Ctrl + C` in PowerShell.
