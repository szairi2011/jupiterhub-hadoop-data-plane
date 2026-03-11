# learn-jupyterhub-with-livy

Incremental exploration of the **JupyterHub → SparkMagic → Livy → Spark/YARN** stack, built as a reference for proposing a scalable data-engineering platform to replace per-user Jupyter notebooks on a shared jumpbox.

---

## Problem Statement

| Current (jumpbox) | Target architecture |
|---|---|
| Spark driver runs locally on the jumpbox — doesn't scale | Spark driver runs remotely inside Livy on a cluster edge node |
| Each user opens a separate Jupyter server process | JupyterHub spawns isolated per-user pods/containers |
| Manual environment setup per user | Declarative, pre-built images; zero user setup |
| No resource governance | YARN queues + Kubernetes resource limits per team |

---

## Target Architecture

```
Browser
  └─► JupyterHub  (Kubernetes, Zero-to-JupyterHub Helm chart)
        └─► KubeSpawner → User Pod  (SparkMagic + kinit sidecar)
                └─► HTTPS/SPNEGO ──► Apache Livy  (Hadoop edge node)
                                          └─► YARN → Spark executors
                                                         └─► HDFS / Hive
```

> **Why Livy on the edge node (not a K8s pod)?**  
> In `yarn-client` deploy mode the Spark driver lives inside the Livy JVM. YARN executors must open
> TCP callbacks to that driver. If Livy runs in a K8s overlay network (e.g. Flannel `10.244.x.x`),
> Hadoop NodeManagers on a separate network cannot route to the driver — sessions hang and die.
> The edge node is already on the Hadoop network, so callbacks work natively.

---

## Exploration Roadmap

### Track A — Docker Compose + DockerSpawner
*One `docker-compose.yml` grown incrementally. Each phase adds services, never replaces them.*

| Phase | Focus | New services / files |
|---|---|---|
| **1** ✅ | Core chain (no auth) | `spark-master`, `spark-worker`, `livy`, `jupyter` |
| **2** | JupyterHub + DockerSpawner | Replace `jupyter` with `jupyterhub`; `Dockerfile.jupyter-user`; `jupyterhub_config.py` |
| **3** | Kerberos + SPNEGO | Add `krb5-kdc`; `livy-kerberos.conf`; `kinit-renewer.sh` |
| **4** | Hive Metastore | Add `hive-metastore`; update `session_configs` with `hive.metastore.uris` |

### Track B — kind (Kubernetes) + KubeSpawner
*Livy + Spark + KDC + HMS still run in Docker Compose. Only the spawner is replaced.*

| Phase | Focus | New files |
|---|---|---|
| **5** | KubeSpawner on kind | `kind-config.yaml`; `helm/values-local-kind.yaml`; `k8s/configmap-sparkmagic.yaml` |
| **6** | Production design | `docs/architecture.md`; `helm/values-prod.yaml`; `docs/production-checklist.md` |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker Desktop | ≥ 4.28 | With Linux containers mode |
| Docker Compose | v2 (`docker compose`) | Bundled with Docker Desktop |
| `kind` | ≥ 0.23 | Phase 5 only — `choco install kind` or https://kind.sigs.k8s.io |
| `helm` | ≥ 3.5 | Phase 5 only — `choco install kubernetes-helm` |
| `kubectl` | ≥ 1.32 | Phase 5 only — bundled with Docker Desktop |

---

## Phase 1 — Quick Start

```powershell
cd c:\Users\sofiane\work\learn-jupyterhub-with-livy

# Build images and start all services
docker compose up --build

# Watch for Livy healthcheck to pass (takes ~30s)
# Then open:
#   Jupyter Lab   http://localhost:8888  (token: sparkmagic)
#   Spark UI      http://localhost:8080
#   Livy REST     http://localhost:8998/sessions
```

Open `notebooks/01_test_connection.ipynb` in Jupyter Lab and run through the cells.

### Shut down

```powershell
docker compose down          # stop and remove containers
docker compose down -v       # also remove volumes
```

### Scale workers

```powershell
docker compose up --scale spark-worker=3
```

---

## Repository Structure

```
learn-jupyterhub-with-livy/
│
├── docker-compose.yml              # Phase 1: spark-master, spark-worker, livy, jupyter
│
├── docker/
│   ├── livy/
│   │   └── Dockerfile              # Livy 0.9.0-incubating built on bitnami/spark:3.5
│   └── jupyter/
│       └── Dockerfile              # quay.io/jupyter/pyspark-notebook + sparkmagic==0.23.0
│
├── config/
│   ├── livy/
│   │   ├── livy.conf               # Livy server config (spark master URL, auth, session limits)
│   │   └── livy-env.sh             # SPARK_HOME and HADOOP_CONF_DIR env vars for Livy
│   └── sparkmagic/
│       └── config.json             # SparkMagic: Livy URL, auth mode, session defaults
│
└── notebooks/
    └── 01_test_connection.ipynb    # Phase 1 validation: %%info, %%pyspark, %%sql
```

---

## Verification Checklist

### Phase 1
- [ ] `curl http://localhost:8998/sessions` returns `{"from":0,"total":0,"sessions":[]}`
- [ ] `curl http://localhost:8998/version` returns Livy version JSON
- [ ] Jupyter Lab opens at `http://localhost:8888` with token `sparkmagic`
- [ ] `%%info` in notebook returns a Livy session ID and Spark version (3.5.x)
- [ ] `%%pyspark` cell `sc.version` returns without error
- [ ] `%%sql SHOW DATABASES` returns at least `default`

---

## Key Configuration Files

### `config/sparkmagic/config.json`
Controls how SparkMagic connects to Livy. Key fields:

| Field | Phase 1 value | Phase 3 value |
|---|---|---|
| `kernel_python_credentials.url` | `http://livy:8998` | `http://livy:8998` |
| `kernel_python_credentials.auth` | `"None"` | `"Kerberos"` |
| `session_configs.driverMemory` | `"1G"` | `"1G"` |
| `session_configs.numExecutors` | `1` | `1` |

### `config/livy/livy.conf`
Controls the Livy server. Key fields:

| Field | Phase 1 value | Phase 3 value |
|---|---|---|
| `livy.server.auth.type` | `none` | `kerberos` |
| `livy.spark.master` | `spark://spark-master:7077` | `yarn` (when on real cluster) |
| `livy.impersonation.enabled` | `false` | `true` |

---

## Version Matrix

| Component | Version | Notes |
|---|---|---|
| Apache Spark | 3.5 (bitnami/spark:3.5) | Standalone for local; YARN on cluster |
| Apache Livy | 0.9.0-incubating | Latest stable (Feb 2026); requires Spark 3.0+ |
| SparkMagic | 0.23.0 | Latest stable (Jul 2025) |
| Jupyter base image | pyspark-notebook:spark-3.5.3 | Quay.io Jupyter project |
| JupyterHub Helm (Phase 5) | 5.x (Zero-to-JupyterHub) | Requires K8s ≥ 1.32 |
| kind (Phase 5) | ≥ 0.23 | K8s-in-Docker; no WSL2 needed on Windows |

---

## Known Gotchas

1. **Livy is still in Apache Incubator** after 9 years — infrequent releases. Consider [Lighter](https://github.com/exacaster/lighter) as a drop-in alternative for production.
2. **`auth` values in `config.json` are case-sensitive**: must be exactly `"None"`, `"Basic_Access"`, or `"Kerberos"`.
3. **Kerberos TGTs expire** (typically 10h). Long-running Jupyter sessions need a `kinit -R` sidecar — covered in Phase 3.
4. **`proxyUser` requires Hadoop admin** to add `hadoop.proxyuser.livy.*` entries to `core-site.xml` on every NameNode and ResourceManager.
5. **Livy 0.9 requires Spark 3.0+** — verify the client cluster's Spark version before deploying.
6. **YARN executor → Livy callback**: put Livy on the edge node, not a K8s pod, to avoid overlay network routing issues.
7. **`readOnly: true` on ConfigMap mounts** prevents SparkMagic's GUI widget from saving session changes — this is fine for production config delivery.
