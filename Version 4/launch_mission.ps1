<#
    ================================================================
    SPACE CLOUD V2.4 - LAUNCH CONTROL CENTER (CLEAN UI)
    ================================================================
    Architecture: Sidecar Pattern + MPC Scheduler + System Bus
    Updates: Clean Logging for Port-Forwarding
#>

# Imposta la directory di lavoro alla cartella dello script corrente
$ScriptPath = $PSScriptRoot
Set-Location $ScriptPath

Write-Host ">>> SPACE CLOUD MISSION V2.4: INITIALIZING..." -ForegroundColor Cyan
Write-Host "    Working Directory: $ScriptPath" -ForegroundColor DarkGray

# ----------------------------------------------------------------
# FASE 1: INFRASTRUTTURA
# ----------------------------------------------------------------
$clusterName = "space-cloud"

if (-not (kind get clusters | Select-String $clusterName)) {
    Write-Host "   [INFRA] Creazione Cluster '$clusterName'..." -ForegroundColor Yellow
    if (Test-Path "space-cloud-config.yaml") {
        kind create cluster --config space-cloud-config.yaml --name $clusterName
    } else {
        kind create cluster --name $clusterName
    }
} else {
    Write-Host "   [INFRA] Cluster '$clusterName' attivo." -ForegroundColor Green
}

# ----------------------------------------------------------------
# FASE 2: BUILD & DEPLOY
# ----------------------------------------------------------------
Write-Host "   [DEPLOY] Applicazione Manifesti..." -ForegroundColor Yellow

# 1. System Redis (Il Bus Dati stabile)
if (Test-Path "system-redis.yaml") {
    kubectl apply -f system-redis.yaml
} else {
    Write-Error "MANCA IL FILE system-redis.yaml!"
    exit
}

# 2. Mission Pod (Il Payload che migra)
if (Test-Path "space-mission.yaml") {
    # Cancelliamo per forzare il clean state
    kubectl delete -f space-mission.yaml --ignore-not-found=true 2>$null | Out-Null
    Start-Sleep -Seconds 2
    kubectl apply -f space-mission.yaml
} else {
    Write-Error "MANCA IL FILE space-mission.yaml!"
    exit
}

# ----------------------------------------------------------------
# FASE 3: RETE E TUNNELS (SILENT MODE)
# ----------------------------------------------------------------
Write-Host "   [NETWORK] Stabilizzazione Uplink..." -ForegroundColor Cyan

# A. SYSTEM BUS TUNNEL
$sysCmd = "& { 
    `$Host.UI.RawUI.WindowTitle = 'SYSTEM BUS (Telemetry)'; 
    Write-Host 'Target: svc/system-redis' -ForegroundColor Cyan;
    while (`$true) {
        try {
            # Silenziamo errori anche qui
            kubectl port-forward svc/system-redis 6379:6379 2>`$null;
        } catch { 
            Write-Host 'Retrying connection to System Redis...' -ForegroundColor Yellow; 
            Start-Sleep -Seconds 2 
        }
    }
}"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $sysCmd

# B. UI TUNNEL (CLEAN LOGGING FIX)
$uiCmd = "& { 
    `$Host.UI.RawUI.WindowTitle = 'USER INTERFACE (Browser)'; 
    Write-Host 'Scanning for Active Mission Pod...' -ForegroundColor Cyan;
    while (`$true) {
        try {
            # Cerchiamo il pod Running della missione
            `$pod = (kubectl get pods -l app=space-app --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>`$null);
            
            if (`$pod) {
                Write-Host ('[LOCKED] Signal acquired: ' + `$pod) -ForegroundColor Green;
                Write-Host 'Dashboard: http://localhost:8080' -ForegroundColor White;
                
                # --- FIX: AGGIUNTO '2>$null' PER NASCONDERE L'ERRORE ROSSO ---
                kubectl port-forward `$pod 8080:3000 2>`$null; 
                # -------------------------------------------------------------
                
                Write-Host '[WARN] Signal Lost (Migration). Re-scanning...' -ForegroundColor Yellow;
            } else {
                Write-Host 'Waiting for Scheduler assignment...' -ForegroundColor DarkGray;
            }
            Start-Sleep -Seconds 1;
        } catch { Start-Sleep -Seconds 1 }
    }
}"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $uiCmd

# Attendiamo che il tunnel Redis sia su prima di lanciare Python
Write-Host "   [WAIT] Attesa stabilizzazione tunnel (5s)..." -ForegroundColor DarkGray
Start-Sleep -Seconds 5

# ----------------------------------------------------------------
# FASE 4: INTELLIGENZA ARTIFICIALE & FISICA
# ----------------------------------------------------------------
Write-Host "   [LAUNCH] Avvio Sottosistemi..." -ForegroundColor Green

# C. MOTORE FISICO
Start-Process powershell -WorkingDirectory $ScriptPath -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'PHYSICS ENGINE'; python physics_sim.py }"

# D. SCHEDULER MPC
Start-Process powershell -WorkingDirectory $ScriptPath -ArgumentList "-NoExit", "-Command", "& { `$Host.UI.RawUI.WindowTitle = 'MPC SCHEDULER'; python mpc_scheduler.py }"

Write-Host "`n>>> T-MINUS 0. MISSION START." -ForegroundColor Green
Write-Host "--------------------------------------------------------"
Write-Host "Monitorare la finestra 'MPC SCHEDULER' per il Binding iniziale."
Write-Host "Dashboard su: http://localhost:8080"
Write-Host "--------------------------------------------------------"