# start-all.ps1
# Starts the entire learn-jupyterhub-with-livy stack (Phase 5) in one shot:
#   1. Docker Compose data plane  (Hadoop YARN+HDFS / Livy / HMS / Postgres)
#   2. HDFS directory init + Hive Metastore cleanup (idempotent)
#   3. Kind / K8s control plane   (JupyterHub + KubeSpawner + socat bridge to Livy)
#
# Idempotent — safe to re-run at any time.
#
# Usage:
#   .\scripts\start-all.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot | Split-Path -Parent

# ---------------------------------------------------------------------------
# 1. Start the Compose data-plane (Hadoop YARN+HDFS / Livy / HMS / Postgres)
# ---------------------------------------------------------------------------
Write-Host "==> Starting Docker Compose stack..."
docker compose -f "$repoRoot/docker-compose.yml" up --build --remove-orphans -d

# Wait for Livy to be healthy — the K8s bootstrap verifies the socat proxy via Livy.
Write-Host "==> Waiting for Livy healthcheck..."
$maxWait = 120
$elapsed = 0
while ($elapsed -lt $maxWait) {
    # Use 'json' filter to avoid template errors when the Health key is absent
    # (happens during the first few seconds before the healthcheck runs).
    $healthJson = docker inspect livy --format '{{json .State.Health}}' 2>$null
    $health = if ($healthJson -and $healthJson -ne 'null') {
        ($healthJson | ConvertFrom-Json).Status
    } else { 'starting' }
    if ($health -eq 'healthy') { break }
    Start-Sleep -Seconds 3
    $elapsed += 3
    Write-Host "    Livy status: $health ($elapsed s / $maxWait s)"
}
if ($elapsed -ge $maxWait) { throw "Livy did not become healthy within $maxWait seconds." }
Write-Host "==> Livy is healthy."

# ---------------------------------------------------------------------------
# 2. Create HDFS directory structure and clean up stale Hive Metastore data
#    (idempotent — safe to re-run; skips if everything is already in order)
# ---------------------------------------------------------------------------
Write-Host ""
& "$repoRoot/scripts/phase5-init-hdfs.ps1"

# ---------------------------------------------------------------------------
# 3. Bootstrap the Kind / K8s control plane (JupyterHub + socat bridge to Livy)
# ---------------------------------------------------------------------------
Write-Host ""
& "$repoRoot/scripts/phase4-up.ps1"
