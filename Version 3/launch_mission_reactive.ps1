Write-Host ">>> Inizializzazione Space Cloud Ground Segment (Auto-Deploy)..." -ForegroundColor Cyan

# Assicuriamoci di essere nella cartella dello script
Set-Location $PSScriptRoot

# ----------------------------------------------------------------
# 0. APERTURA CANALE DATI (REDIS - Auto-Reconnect)
# ----------------------------------------------------------------
Write-Host "   - Apertura Tunnel Telemetria (Redis)..."

$redisCommand = "& { 
    `$Host.UI.RawUI.WindowTitle = 'TERM 0: DATALINK (Redis Tunnel)'; 
    Write-Host '[...] Ricerca segnale DB...' -ForegroundColor Cyan;
    while (`$true) { 
        try {
            # Cerchiamo solo il pod Running per evitare errori
            `$pod = (kubectl get pods -l app=redis --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>`$null);
            if (`$pod) {
                Write-Host '[OK] Link stabile con DB: ' `$pod -ForegroundColor Green;
                kubectl port-forward `$pod 6379:6379;
                Write-Host '[!] Link DB perso. Riconnessione immediata...' -ForegroundColor Red;
            } else {
                Start-Sleep -Milliseconds 500;
            }
        } catch { Start-Sleep -Milliseconds 500 }
    } 
}"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $redisCommand

# Aspettiamo che il tunnel si stabilizzi
Write-Host "     (Attesa stabilizzazione Link...)" -ForegroundColor Gray
Start-Sleep -Seconds 3

# ----------------------------------------------------------------
# NOVITÀ: DEPLOY AUTOMATICO MISSIONE
# ----------------------------------------------------------------
Write-Host "   - INIEZIONE MISSIONE (Deploy Dashboard)..." -ForegroundColor Magenta
kubectl apply -f space-dashboard.yaml

# ----------------------------------------------------------------
# 1. SIMULATORE FISICO
# ----------------------------------------------------------------
Write-Host "   - Avvio Simulatore Fisico (EPS)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 1: FISICA (Batterie)'; python physics_sim.py }"

Start-Sleep -Seconds 1

# ----------------------------------------------------------------
# 2. SCHEDULER (Decision Maker)
# ----------------------------------------------------------------
Write-Host "   - Avvio Space Scheduler..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 2: SCHEDULER (Decision Maker)'; python space_scheduler.py }"

Start-Sleep -Seconds 1

# ----------------------------------------------------------------
# 3. WATCHDOG (Safety System)
# ----------------------------------------------------------------
Write-Host "   - Avvio Watchdog (FDIR)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'TERM 3: WATCHDOG (Safety)'; python space_watchdog_reactive.py }"

Start-Sleep -Seconds 1

# ----------------------------------------------------------------
# 4. INTERFACCIA GRAFICA (FAST RECONNECT)
# ----------------------------------------------------------------
Write-Host "   - Avvio Interfaccia Grafica (UI Link Aggressivo)..."

$uiCommand = "& { 
    `$Host.UI.RawUI.WindowTitle = 'TERM 4: UI LINK (Browser)'; 
    Write-Host '[...] Scansione frequenze video...' -ForegroundColor Cyan;
    
    while (`$true) { 
        try {
            # MODIFICA CRITICA:
            # 1. Cerchiamo SOLO i pod in stato 'Running'.
            # 2. Ignoriamo quelli in 'Terminating' (che causano il lag di connessione).
            `$podName = (kubectl get pods -l app=space-app --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>`$null);
            
            if (`$podName) {
                Write-Host '[OK] Agganciato satellite: ' `$podName -ForegroundColor Green;
                Write-Host '     Dashboard attiva su http://localhost:8080' -ForegroundColor White;
                
                # Il port-forward blocca il processo finché la connessione è viva.
                # Appena il Watchdog uccide il pod, questo comando termina e il loop ricomincia.
                kubectl port-forward `$podName 8080:3000;
                
                Write-Host '[!] Segnale perso (Handover). Ricerca immediata...' -ForegroundColor Yellow;
            } else {
                # Se non c'è nessun pod Running (es. durante lo switch), aspettiamo pochissimo.
                Start-Sleep -Milliseconds 500;
            }
        } catch { 
            # Gestione errori imprevisti
            Start-Sleep -Milliseconds 500 
        }
    } 
}"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $uiCommand


Write-Host "`n[OK] SISTEMA ONLINE." -ForegroundColor Green
Write-Host "[!] NOTA: Se il TERM 0 da errore, assicurati di aver lanciato 'kubectl apply -f space-redis.yaml'" -ForegroundColor Yellow