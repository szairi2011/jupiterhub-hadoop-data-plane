# =============================================================================
# JupyterHub configuration — Phase 2
# DockerSpawner + DummyAuthenticator (no real auth — dev/demo only)
#
# Architecture:
#   Browser → JupyterHub (port 8000)
#             └─ DockerSpawner ──► singleuser container (JupyterLab + SparkMagic)
#                                        └─ Livy REST (port 8998)
#                                               └─ Spark standalone cluster
#
# Production upgrade path:
#   Auth    → swap DummyAuthenticator for PAMAuthenticator or LDAPAuthenticator
#   Spawn   → swap DockerSpawner for KubeSpawner (Phase 5)
#   Network → add TLS termination in front of port 8000
#   Spark   → swap spark-master URL for YARN (Phase 3)
# =============================================================================

import os
from dockerspawner import DockerSpawner

# ---------------------------------------------------------------------------
# Authenticator
# DummyAuthenticator accepts ANY username with the shared password below.
# allowed_users is a second gate — only listed names can log in.
# ---------------------------------------------------------------------------
c.JupyterHub.authenticator_class = "dummy"
# Shared password for all dev users. Change per-user in production.
c.DummyAuthenticator.password = "sparkmagic"
# Add usernames here to grant access; remove the set entirely to allow all
# (not recommended even in dev — it exposes Spark resources).
c.Authenticator.allowed_users = {"alice", "bob", "data-engineer"}

# ---------------------------------------------------------------------------
# Spawner — DockerSpawner
# Each login creates an isolated container from the singleuser image.
# The container is destroyed when the user clicks "Stop My Server".
# ---------------------------------------------------------------------------
c.JupyterHub.spawner_class = DockerSpawner

# Image built by `docker compose build singleuser` (docker/singleuser/Dockerfile)
c.DockerSpawner.image = "learn-jupyterhub-with-livy-singleuser:latest"

# Singleuser containers must be on the same Docker network as Livy;
# otherwise `http://livy:8998` is not resolvable from inside the container.
c.DockerSpawner.network_name = "learn-jupyterhub-with-livy_spark-net"
# use_internal_ip: hub reaches singleuser by container IP, not host-port mapping
c.DockerSpawner.use_internal_ip = True
# remove: auto-delete the container on stop (volumes persist separately)
c.DockerSpawner.remove = True

# Volume mounts for every singleuser container.
# IMPORTANT: DockerSpawner calls the Docker daemon on the HOST, so bind-mount
# source paths must be valid on the HOST, not inside the hub container.
# REPO_ROOT is set in docker-compose.yml to ${COMPOSE_PROJECT_DIR}, which
# Docker Compose v2.24+ expands to the repo root on the Docker host.
# Normalise to forward slashes so Docker Desktop on Windows accepts the path.
_repo_root = os.environ.get("REPO_ROOT", "").replace("\\", "/")
c.DockerSpawner.volumes = {
    # SparkMagic Livy connection config — shared read-only across all users.
    # The singleuser image already has a baked-in copy of this file; this
    # mount overlays it so editing config.json takes effect on the next server
    # start without requiring an image rebuild.
    f"{_repo_root}/config/sparkmagic/config.json": {
        "bind": "/home/jovyan/.sparkmagic/config.json",
        "mode": "ro",
    },
    # Shared read-only notebooks (validation, demos).
    # Again, the image has a baked-in copy as fallback.
    f"{_repo_root}/notebooks": {
        "bind": "/home/jovyan/shared-notebooks",
        "mode": "ro",
    },
    # Named Docker volume — persists the user's own notebooks across container
    # restarts. Volume is auto-created on first login: jupyterhub-user-alice, etc.
    "jupyterhub-user-{username}": "/home/jovyan/work",
}

# Environment injected into every singleuser container at spawn time.
c.DockerSpawner.environment = {
    "JUPYTER_ENABLE_LAB": "yes",   # force JupyterLab UI instead of classic Notebook
}

# Hard resource caps per user container (Docker-level cgroups).
# In production size these to match the Spark executor resources so one user
# can't starve the cluster by leaving a large driver process running.
c.DockerSpawner.mem_limit = "2G"
c.DockerSpawner.cpu_limit = 2

# ---------------------------------------------------------------------------
# Hub networking
# hub_ip        = address the hub API server listens on inside its container
# hub_connect_ip = address singleuser containers use to REACH the hub
#   → must be the hub's container name (DNS resolvable on spark-net)
# ---------------------------------------------------------------------------
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = "jupyterhub"

# ---------------------------------------------------------------------------
# Public interface — what end-users browse to
# ---------------------------------------------------------------------------
c.JupyterHub.ip = "0.0.0.0"
c.JupyterHub.port = 8000

# ---------------------------------------------------------------------------
# Persistence — stored in the jupyterhub-data Docker volume
# sqlite DB keeps user/session state; cookie secret must survive restarts
# or all active sessions are invalidated.
# ---------------------------------------------------------------------------
c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/jupyterhub.sqlite"
c.JupyterHub.cookie_secret_file = "/srv/jupyterhub/jupyterhub_cookie_secret"
