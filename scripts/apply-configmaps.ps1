# apply-configmaps.ps1
# Pushes SparkMagic config and shared notebooks into the jhub K8s namespace
# as ConfigMaps so KubeSpawner can mount them into user Pods.
#
# Run this once after `helm upgrade --install` and again whenever
# config/sparkmagic/config.json or notebooks/ change.
#
# Prerequisites: kubectl context must be pointing at the kind-jhub cluster.
#   kubectl config use-context kind-jhub

param(
    [string]$Namespace = "jhub"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot | Split-Path -Parent

Write-Host "==> Creating/updating sparkmagic-config ConfigMap..."
# Use config-k8s.json: Livy URL is 'livy' which resolves via the K8s Service
# created dynamically by phase4-up.ps1 (Service + Endpoints pointing at the
# Livy container's IP on the compose spark-net).
kubectl create configmap sparkmagic-config `
    --from-file=config.json="$repoRoot/config/sparkmagic/config-k8s.json" `
    --namespace $Namespace `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host "==> Creating/updating shared-notebooks ConfigMap..."
# Collect all .ipynb files from the notebooks/ directory.
$notebooks = Get-ChildItem "$repoRoot/notebooks" -Filter "*.ipynb"
$fromFileArgs = $notebooks | ForEach-Object { "--from-file=$($_.Name)=$($_.FullName)" }

kubectl create configmap shared-notebooks `
    @fromFileArgs `
    --namespace $Namespace `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host "==> Done. ConfigMaps in namespace '$Namespace':"
kubectl get configmap --namespace $Namespace
