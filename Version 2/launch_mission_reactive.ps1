Write-Host ">>> Inizializzazione Space Cloud Ground Segment..." -ForegroundColor Cyan

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
            # Verifica se il pod Redis esiste
            `$pod = (kubectl get pods -l app=redis -o jsonpath='{.items[0].metadata.name}' 2>`$null);
            if (`$pod) {
                Write-Host '[OK] Link stabile con DB: ' `$pod -ForegroundColor Green;
                # Questo blocca finché la connessione è attiva
                kubectl port-forward service/redis-sat 6379:6379;
                Write-Host '[!] Link DB perso. Riconnessione...' -ForegroundColor Red;
            } else {
                Write-Host '.' -NoNewline;
                Start-Sleep -Seconds 2;
            }
        } catch { Start-Sleep -Seconds 2 }
    } 
}"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $redisCommand

# Aspettiamo qualche secondo per essere sicuri che la connessione sia stabilita
Start-Sleep -Seconds 3

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
# 4. INTERFACCIA GRAFICA (Auto-Reconnect)
# ----------------------------------------------------------------
Write-Host "   - Avvio Interfaccia Grafica (UI Link)..."

# Definiamo il comando come stringa semplice per evitare errori di parsing
$uiCommand = "& { 
    `$Host.UI.RawUI.WindowTitle = 'TERM 4: UI LINK (Browser)'; 
    Write-Host '[...] In attesa del segnale video dal satellite...' -ForegroundColor Cyan;
    while (`$true) { 
        try {
            `$podName = (kubectl get pods -l app=space-app -o jsonpath='{.items[0].metadata.name}' 2>`$null);
            if (`$podName) {
                Write-Host '[OK] Connesso a: ' `$podName -ForegroundColor Green;
                Write-Host '     Apri http://localhost:8080 nel browser' -ForegroundColor White;
                # Questo comando blocca finché il tunnel è attivo
                kubectl port-forward `$podName 8080:3000;
                Write-Host '[!] Segnale perso (Blackout). Ricerca nuovo satellite...' -ForegroundColor Yellow;
            } else {
                Write-Host '.' -NoNewline;
                Start-Sleep -Seconds 2;
            }
        } catch { Start-Sleep -Seconds 2 }
    } 
}"

Start-Process powershell -ArgumentList "-NoExit", "-Command", $uiCommand


Write-Host "`n[OK] SISTEMA ONLINE." -ForegroundColor Green
Write-Host "[!] NOTA: Se il TERM 0 da errore, assicurati di aver lanciato 'kubectl apply -f space-redis.yaml'" -ForegroundColor Yellow
Write-Host "Usa QUESTA finestra per lanciare la missione:" -ForegroundColor Gray
Write-Host "kubectl apply -f space-dashboard.yaml" -ForegroundColor White