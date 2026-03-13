# start-all.ps1
# Starts the entire learn-jupyterhub-with-livy stack in one shot:
#   1. Docker Compose data plane  (Spark, Livy, HMS, Postgres)
#   2. Kind / K8s control plane   (JupyterHub + KubeSpawner)
#
# Idempotent — safe to re-run at any time.
#
# Usage:
#   .\scripts\start-all.ps1

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot | Split-Path -Parent

# ---------------------------------------------------------------------------
# 1. Start the Compose data-plane (Spark / Livy / HMS)
# ---------------------------------------------------------------------------
Write-Host "==> Starting Docker Compose stack..."
docker compose -f "$repoRoot/docker-compose.yml" up --build -d

# Wait for Livy to be healthy before proceeding — phase4-up.ps1 needs it.
Write-Host "==> Waiting for Livy healthcheck..."
$maxWait = 120
$elapsed = 0
while ($elapsed -lt $maxWait) {
    $health = docker inspect livy --format '{{.State.Health.Status}}' 2>$null
    if ($health -eq 'healthy') { break }
    Start-Sleep -Seconds 3
    $elapsed += 3
    Write-Host "    Livy status: $health ($elapsed s / $maxWait s)"
}
if ($elapsed -ge $maxWait) { throw "Livy did not become healthy within $maxWait seconds." }
Write-Host "==> Livy is healthy."

# ---------------------------------------------------------------------------
# 2. Bootstrap the Kind / K8s stack (JupyterHub + socat bridge to Livy)
# ---------------------------------------------------------------------------
Write-Host ""
& "$repoRoot/scripts/phase4-up.ps1"
