# stop-all.ps1
# Stops the stacks started by start-all.ps1.
#
# Usage:
#   .\scripts\stop-all.ps1                    # PAUSE   - stop containers, keep kind + all volumes
#   .\scripts\stop-all.ps1 -Destroy           # RESET   - delete kind cluster, stop containers, keep volumes (HDFS data survives)
#   .\scripts\stop-all.ps1 -Destroy -Wipe     # NUCLEAR - delete kind cluster, stop containers, DELETE all volumes (full clean slate)
#
# Choosing the right mode:
#   Pause   -> end of the day; resume with start-all.ps1 tomorrow; nothing is lost
#   Reset   -> tear down the K8s front-end but keep HDFS data; useful when kind config changes
#   Nuclear -> completely clean slate; start-all.ps1 re-creates everything from scratch

param(
    [switch]$Destroy,
    [switch]$Wipe        # only meaningful with -Destroy; removes all named volumes
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot       = $PSScriptRoot | Split-Path -Parent
$composeFile    = "$repoRoot/docker-compose.yml"
$network        = "learn-jupyterhub-with-livy_spark-net"
$kindNode       = "jhub-control-plane"

# ---------------------------------------------------------------------------
# Helper: disconnect the kind control-plane node from spark-net so that
# `docker compose down` can remove the network cleanly.
# Without this step Docker prints "Network … Resource is still in use".
# ---------------------------------------------------------------------------
function Disconnect-KindNode {
    $connected = docker network inspect $network `
        --format '{{range .Containers}}{{.Name}} {{end}}' 2>$null
    if ($connected -match $kindNode) {
        Write-Host "==> Disconnecting '$kindNode' from '$network'..."
        docker network disconnect $network $kindNode 2>$null
    }
}

if ($Destroy) {
    # -------------------------------------------------------------------------
    # RESET / NUCLEAR: delete kind cluster then tear down Compose
    # -------------------------------------------------------------------------
    Write-Host "==> Deleting kind cluster 'jhub'..."
    kind delete cluster --name jhub 2>&1 | Where-Object { $_ -notmatch "^$" }

    Disconnect-KindNode

    if ($Wipe) {
        Write-Host "==> Removing all containers and volumes (nuclear reset)..."
        docker compose -f $composeFile down --volumes --remove-orphans
        Write-Host ""
        Write-Host "==> Nuclear reset complete. All HDFS data and HMS metadata have been deleted."
        Write-Host "    Run .\scripts\start-all.ps1 to rebuild everything from scratch."
    } else {
        Write-Host "==> Stopping containers (volumes kept - HDFS data preserved)..."
        docker compose -f $composeFile down --remove-orphans
        Write-Host ""
        Write-Host "==> Reset complete. HDFS volumes intact."
        Write-Host "    Run .\scripts\start-all.ps1 to restart with existing data."
    }
} else {
    # -------------------------------------------------------------------------
    # PAUSE: stop containers but keep kind cluster and all volumes
    # -------------------------------------------------------------------------
    Write-Host "==> Pausing Docker Compose stack (kind cluster and volumes kept)..."
    docker compose -f $composeFile stop
    Write-Host ""
    Write-Host "==> Stack paused. Run .\scripts\start-all.ps1 to resume."
}
