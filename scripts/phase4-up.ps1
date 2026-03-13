# phase4-up.ps1
# Full Phase 4 bootstrap — idempotent, safe to re-run.
#
# What it does:
#   1. Creates a kind cluster named "jhub" (skips if it already exists)
#   2. Loads the singleuser Docker image into the cluster (no registry needed)
#   3. Deploys Zero-to-JupyterHub via Helm
#   4. Pushes ConfigMaps (SparkMagic config + shared notebooks)
#
# Prerequisites (install once):
#   winget install Kubernetes.kind
#   winget install Helm.Helm
#   winget install Kubernetes.kubectl
#
# Usage:
#   .\scripts\phase4-up.ps1
#
# JupyterHub will be available at http://localhost:8001 once the hub Pod is Ready.
# The Spark/Livy/HMS stack must still be started separately:
#   docker compose up -d

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = $PSScriptRoot | Split-Path -Parent
$clusterName = "jhub"
$namespace   = "jhub"
$image       = "learn-jupyterhub-with-livy-singleuser:latest"

# ---------------------------------------------------------------------------
# 1. Create kind cluster
# ---------------------------------------------------------------------------
$existing = & { $ErrorActionPreference = 'Continue'; kind get clusters 2>&1 }
if ($existing -match $clusterName) {
    Write-Host "==> kind cluster '$clusterName' already exists, skipping create."
} else {
    Write-Host "==> Creating kind cluster '$clusterName'..."
    kind create cluster --name $clusterName --config "$repoRoot/kind-config.yaml"
}

# Switch kubectl context to the new cluster
kubectl config use-context "kind-$clusterName"

# ---------------------------------------------------------------------------
# 1b. Connect kind node to Livy's network and register Livy as a K8s Service
# Pods can't route directly to the Compose network (Docker Desktop / WSL2
# doesn't MASQUERADE pod-CIDR traffic). The workaround is simple:
#   1. Connect the kind node to the Compose network (so Docker DNS resolves 'livy').
#   2. Run a socat TCP proxy on the kind node: 0.0.0.0:8998 → livy:8998.
#   3. Point a K8s Service + Endpoints at the node's InternalIP.
# Pods hit ClusterIP → node IP → socat (local delivery, no FORWARD chain) → Livy.
$kindNode = "jhub-control-plane"

# Discover Livy's network dynamically — no hardcoded compose project name.
$livyNetworkName = (docker inspect livy --format '{{json .NetworkSettings.Networks}}' |
    ConvertFrom-Json).PSObject.Properties.Name |
    Select-Object -First 1
if (-not $livyNetworkName) { throw "Could not find a network for the 'livy' container. Is the compose stack running?" }
Write-Host "==> Livy is on network: $livyNetworkName"

# Connect the kind node to Livy's network if not already connected.
$alreadyConnected = (docker inspect $kindNode --format '{{json .NetworkSettings.Networks}}' |
    ConvertFrom-Json).PSObject.Properties.Name
if ($alreadyConnected -contains $livyNetworkName) {
    Write-Host "==> kind node already on '$livyNetworkName', skipping connect."
} else {
    Write-Host "==> Connecting kind node to '$livyNetworkName'..."
    docker network connect $livyNetworkName $kindNode
}

# Install socat on the kind node if not present, then (re)start the TCP proxy.
docker exec $kindNode bash -c "which socat >/dev/null 2>&1 || (apt-get update -qq && apt-get install -y -qq socat >/dev/null 2>&1)"
docker exec $kindNode bash -c "pkill -f 'socat.*TCP-LISTEN:8998' 2>/dev/null || true"
Write-Host "==> Starting socat proxy on kind node (8998 -> livy:8998)..."
docker exec -d $kindNode socat TCP-LISTEN:8998,fork,reuseaddr TCP:livy:8998
Start-Sleep -Seconds 1

# Quick sanity check — make sure the proxy reaches Livy.
$probe = docker exec $kindNode curl -s --max-time 3 http://127.0.0.1:8998/sessions
if ($probe -notmatch '"sessions"') { throw "socat proxy is not forwarding to Livy. Is the compose stack running?" }
Write-Host "==> socat proxy verified — Livy reachable via kind node."

# Get the node IP via bash inside the container — avoids Go-template double-quote quoting
# issues on Windows (which cause 'docker inspect --format' to fail with non-zero exit code).
# eth0 is always the kind-network interface; any compose network attach lands on eth1+.
$nodeIp = & { $ErrorActionPreference = 'Continue'; docker exec $kindNode bash -c "ip -4 addr show eth0 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | head -1" 2>$null }
$nodeIp = "$nodeIp".Trim()
if (-not $nodeIp) { throw "Could not determine node InternalIP from eth0 - is the kind node running?" }
Write-Host "==> Node InternalIP: $nodeIp (will register K8s Service after Helm creates the namespace)"

# ---------------------------------------------------------------------------
# 2. Build singleuser image and load into kind
# ---------------------------------------------------------------------------
# Built directly with docker build — the singleuser image is a K8s concern
# and has no place in the Compose data-plane file.
Write-Host "==> Building singleuser image..."
docker build -t $image -f "$repoRoot/docker/singleuser/Dockerfile" "$repoRoot"

Write-Host "==> Loading '$image' into kind cluster '$clusterName'..."
kind load docker-image $image --name $clusterName

# ---------------------------------------------------------------------------
# 3. Helm — deploy Zero-to-JupyterHub
# ---------------------------------------------------------------------------
Write-Host "==> Adding JupyterHub helm repo..."
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/ | Out-Null
helm repo update | Out-Null

Write-Host "==> Deploying JupyterHub via Helm (namespace: $namespace)..."
helm upgrade --install jhub jupyterhub/jupyterhub `
    --namespace $namespace --create-namespace `
    --values "$repoRoot/config/jupyterhub/helm-values.yaml" `
    --timeout 5m `
    --wait

# Register the Livy socat proxy as a K8s Service so pods can resolve 'livy'
# via cluster DNS. Done here (after Helm) because Helm creates the namespace.
Write-Host "==> Registering Livy proxy as K8s Service in namespace '$namespace'..."
$yaml = "apiVersion: v1`nkind: Service`nmetadata:`n  name: livy`n  namespace: $namespace`nspec:`n  ports:`n    - port: 8998`n      targetPort: 8998`n      protocol: TCP`n---`napiVersion: v1`nkind: Endpoints`nmetadata:`n  name: livy`n  namespace: $namespace`nsubsets:`n  - addresses:`n      - ip: $nodeIp`n    ports:`n      - port: 8998`n"
$tmpYaml = [System.IO.Path]::GetTempFileName() + ".yaml"
Set-Content -Encoding UTF8 -Path $tmpYaml -Value $yaml
kubectl apply -f $tmpYaml
Remove-Item $tmpYaml
Write-Host "==> Livy Service registered (endpoint: $nodeIp`:8998)."

# ---------------------------------------------------------------------------
# 4. Push ConfigMaps
# ---------------------------------------------------------------------------
Write-Host "==> Applying ConfigMaps..."
& "$repoRoot/scripts/apply-configmaps.ps1" -Namespace $namespace

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "==> Phase 4 is up."
Write-Host "    JupyterHub : http://localhost:8001  (login: alice / sparkmagic)"
Write-Host "    Spark UI   : http://localhost:8080  (docker compose stack)"
Write-Host "    Livy REST  : http://localhost:8998/sessions"
Write-Host ""
Write-Host "    To watch hub Pod come up:"
Write-Host "      kubectl get pods -n $namespace -w"
