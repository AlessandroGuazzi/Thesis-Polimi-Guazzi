<#
    ================================================================
    SPACE CLOUD V3.0 - LAUNCH CONTROL CENTER (INGRESS EDITION)
    ================================================================
    Architecture: Sidecar Pattern + MPC Scheduler + System Bus
    Networking: NGINX Ingress Controller (HTTP Port 80)
    Persistence: CRIU Simulation
#>

# Imposta la directory di lavoro
$ScriptPath = $PSScriptRoot
Set-Location $ScriptPath

Write-Host ">>> SPACE CLOUD MISSION V4.0: INITIALIZING..." -ForegroundColor Cyan
Write-Host "    Mode: INGRESS ROUTING ACTIVATED" -ForegroundColor DarkGray

# ----------------------------------------------------------------
# FASE 1: INFRASTRUTTURA & CLUSTER
# ----------------------------------------------------------------
$clusterName = "space-cloud"

if (-not (kind get clusters | Select-String $clusterName)) {
    Write-Host "   [INFRA] Cluster non trovato. Esegui la configurazione manuale FASE 1!" -ForegroundColor Red
    exit
} else {
    Write-Host "   [INFRA] Cluster '$clusterName' attivo." -ForegroundColor Green
}

# ----------------------------------------------------------------
# FASE 2: BUILD & DEPLOY
# ----------------------------------------------------------------
Write-Host "   [DEPLOY] Applicazione Manifesti..." -ForegroundColor Yellow

# 1. System Redis (Il Bus Dati)
if (Test-Path "system-redis.yaml") { kubectl apply -f system-redis.yaml }

# 2. Mission Pod (L'Applicazione)
if (Test-Path "space-mission.yaml") {
    # Force delete per simulare un lancio pulito
    kubectl delete -f space-mission.yaml --ignore-not-found=true 2>$null | Out-Null
    Start-Sleep -Seconds 1
    kubectl apply -f space-mission.yaml
}

# 3. Ingress Rules (Il Cartello Stradale)
if (Test-Path "space-ingress.yaml") {
    Write-Host "   [NET] Aggiornamento Regole di Navigazione (Ingress)..." -ForegroundColor Cyan
    kubectl apply -f space-ingress.yaml
}

# ----------------------------------------------------------------
# FASE 3: RETE E TUNNELS (HYBRID MODE)
# ----------------------------------------------------------------
Write-Host "   [NETWORK] Stabilizzazione Uplink..." -ForegroundColor Cyan

# A. SYSTEM BUS TUNNEL (Manteniamo il Port-Forward SOLO per il Database interno)
# Questo serve ai tuoi script Python (Scheduler e Fisica) che girano sul PC per parlare col cluster.
$sysCmd = "& { 
    `$Host.UI.RawUI.WindowTitle = 'SYSTEM BUS (Telemetry Link)'; 
    Write-Host 'Target: svc/system-redis (Internal Only)' -ForegroundColor Cyan;
    while (`$true) {
        try {
            # Tunnel silenzioso per il Bus Dati
            kubectl port-forward svc/system-redis 6379:6379 2>`$null;
        } catch { 
            Write-Host 'Connecting to System Bus...' -ForegroundColor Yellow; 
            Start-Sleep -Seconds 2 
        }
    }
}"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $sysCmd

# B. UI LINK (Nessun Tunnel! Usiamo l'Ingress)
Write-Host "   [INFO] UI Port-Forwarding disabilitato. Traffico gestito da NGINX." -ForegroundColor DarkGray

# Attendiamo che il tunnel Redis sia su per i motori Python
Write-Host "   [WAIT] Calibrazione Sistemi (5s)..." -ForegroundColor DarkGray
Start-Sleep -Seconds 5

# ----------------------------------------------------------------
# FASE 4: INTELLIGENZA ARTIFICIALE & FISICA
# ----------------------------------------------------------------
Write-Host "   [LAUNCH] Avvio Motori di Calcolo..." -ForegroundColor Green

# C. MOTORE FISICO
Start-Process powershell -WorkingDirectory $ScriptPath -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'PHYSICS ENGINE'; python physics_sim.py }"

# D. SCHEDULER MPC (CRIU ENABLED)
Start-Process powershell -WorkingDirectory $ScriptPath -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'MPC SCHEDULER (CRIU)'; python mpc_scheduler.py }"

Write-Host "`n>>> MISSION LAUNCHED SUCCESSFULLY." -ForegroundColor Green
Write-Host "--------------------------------------------------------"
Write-Host "   DASHBOARD LINK:  http://mission-control.local" -ForegroundColor Cyan
Write-Host "--------------------------------------------------------"
Write-Host "   1. Apri il link nel browser."
Write-Host "   2. Monitora la finestra 'MPC SCHEDULER' per vedere la migrazione."
Write-Host "   3. Goditi la continuità del servizio."
Write-Host "--------------------------------------------------------"