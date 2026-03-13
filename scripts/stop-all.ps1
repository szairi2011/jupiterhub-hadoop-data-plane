# stop-all.ps1
# Stops the stacks started by start-all.ps1.
#
# Usage:
#   .\scripts\stop-all.ps1           # pause (keeps kind cluster + volumes)
#   .\scripts\stop-all.ps1 -Destroy  # full teardown (deletes kind cluster too)
#
# Most of the time you want the default (pause). Use -Destroy only when you
# need a completely clean slate. start-all.ps1 is idempotent either way.

param(
    [switch]$Destroy
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot | Split-Path -Parent

if ($Destroy) {
    # ---------------------------------------------------------------------------
    # Full teardown: delete the kind cluster first
    # ---------------------------------------------------------------------------
    & "$repoRoot/scripts/phase4-down.ps1"
    Write-Host "==> Stopping Docker Compose stack..."
    docker compose -f "$repoRoot/docker-compose.yml" down
    Write-Host "==> Full teardown complete."
} else {
    # ---------------------------------------------------------------------------
    # Pause: stop compose containers but keep the kind cluster intact.
    # Re-run start-all.ps1 to resume — it will reconnect the network and
    # restart the socat proxy automatically.
    # ---------------------------------------------------------------------------
    Write-Host "==> Pausing Docker Compose stack (kind cluster kept)..."
    docker compose -f "$repoRoot/docker-compose.yml" stop
    Write-Host "==> Compose stack paused. Run .\scripts\start-all.ps1 to resume."
}
