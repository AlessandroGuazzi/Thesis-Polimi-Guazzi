Write-Host " Inizializzazione Space Cloud Ground Segment..." -ForegroundColor Cyan

# Assicuriamoci di essere nella cartella dello script
Set-Location $PSScriptRoot

# 1. Lancia il Simulatore Fisico (Eps)
Write-Host "   - Avvio Simulatore Fisico (EPS)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 1: FISICA (Batterie)'; python physics_sim.py }"

# Attesa tattica per dare tempo a Python di partire
Start-Sleep -Seconds 1

# 2. Lancia lo Scheduler (Cervello)
Write-Host "   - Avvio Space Scheduler..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 2: SCHEDULER (Decision Maker)'; python space_scheduler.py }"

Start-Sleep -Seconds 1

# 3. Lancia il Watchdog (Poliziotto)
Write-Host "   - Avvio Watchdog (Descheduler)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 3: WATCHDOG (Safety)'; python space_watchdog_reactive.py }"

Write-Host "`n SISTEMA ONLINE." -ForegroundColor Green
Write-Host "Usa QUESTA finestra per lanciare i comandi (kubectl apply -f space-task.yaml)" -ForegroundColor Yellow