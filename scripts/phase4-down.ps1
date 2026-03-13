# phase4-down.ps1
# Tears down the Phase 4 kind cluster cleanly.
#
# Usage:
#   .\scripts\phase4-down.ps1
#
# Does NOT touch the Docker Compose Spark stack.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$clusterName = "jhub"

Write-Host "==> Deleting kind cluster '$clusterName'..."
kind delete cluster --name $clusterName

Write-Host "==> Done. Docker Compose stack unchanged."
Write-Host "    To also stop Spark/Livy: docker compose down"
