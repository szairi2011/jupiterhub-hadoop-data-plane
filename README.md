# learn-jupyterhub-with-livy

> JupyterHub → SparkMagic → Livy → Spark on YARN/HDFS — an incremental, fully Dockerized learning stack.

## Contents

- [**Quick start**](#quick-start)
- [How it works](#how-it-works)
- [One binary, every role — the daemon dispatch pattern](#one-binary-every-role--the-daemon-dispatch-pattern)
- [Spark cluster managers: YARN, Kubernetes, Standalone](#spark-cluster-managers-yarn-kubernetes-standalone)
- [Hadoop deployment options](#hadoop-deployment-options)
- [Phase roadmap](#phase-roadmap)
- [Phase 1 — Core chain](#phase-1--core-chain)
- [Phase 2 — JupyterHub](#phase-2--jupyterhub)
- [Phase 3 — Hive Metastore](#phase-3--hive-metastore)
- [Phase 4 — kind + KubeSpawner](#phase-4--kind--kubespawner)
- [Phase 5 — YARN + HDFS](#phase-5--yarn--hdfs)
- [Cluster validation](#cluster-validation)
  - [Container health](#container-health)
  - [HDFS — DataNodes + block replication](#hdfs--datanodes--block-replication)
  - [YARN — NodeManagers + job submission](#yarn--nodemanagers--job-submission)
  - [Multi-user isolation (JupyterHub on kind)](#multi-user-isolation-jupyterhub-on-kind)
  - [Notebook test suite](#notebook-test-suite)
- [Resource contention](#resource-contention)
- [Resource optimization](#resource-optimization)
  - [Spark Dynamic Allocation](#spark-dynamic-allocation)
  - [YARN — AM resource limit](#yarn--am-resource-limit)
  - [YARN — Opportunistic Containers](#yarn--opportunistic-containers)
  - [YARN — Node oversubscription](#yarn--node-oversubscription)
  - [JupyterHub — per-user Pod limits](#jupyterhub--per-user-pod-limits)
  - [Livy — session lifecycle limits](#livy--session-lifecycle-limits)
  - [SparkMagic — session defaults](#sparkmagic--session-defaults)
  - [Optimization decision guide](#optimization-decision-guide)
- [Key config knobs](#key-config-knobs)
- [Version matrix](#version-matrix)
- [Gotchas](#gotchas)

---

## Quick start

### Prerequisites (install once)

| Tool | Purpose | Install |
|---|---|---|
| Docker Desktop | Runs all containers + kind nodes | https://docs.docker.com/desktop/ |
| `kind` | K8s-in-Docker cluster for JupyterHub | `winget install Kubernetes.kind` |
| `kubectl` | Manage the kind cluster | `winget install Kubernetes.kubectl` |
| `helm` | Deploy Zero-to-JupyterHub chart | `winget install Helm.Helm` |

```powershell
# Verify all tools are on PATH (restart terminal after install)
docker --version ; kind version ; kubectl version --client ; helm version
```

> Docker Desktop **must be running** before any script below. On first run, Docker pulls base images (~3–4 GB) and Maven downloads Hive JARs — budget 10–15 minutes.

### First boot (run once per machine)

```powershell
# 1. Clone and enter the repo
git clone <repo-url>
cd learn-jupyterhub-with-livy

# 2. Start everything in one command
.\scripts\start-all.ps1

#    start-all.ps1 does:
#      a) docker compose up --build -d   (builds images, starts Hadoop + YARN + Livy + HMS)
#      b) waits for Livy to become healthy
#      c) creates HDFS directory layout + cleans up any stale HMS metadata  ← automatic
#      d) creates the kind cluster + deploys JupyterHub via Helm (idempotent)

# 3. Wait for JupyterHub pod to be ready (~2 min after step 2)
kubectl get pods -n jhub -w     # wait until hub-* shows 1/1 Running, then Ctrl-C
```

### Open the stack

| URL / address | What |
|---|---|
| http://localhost:8001 | JupyterHub — login: `alice` / `sparkmagic` |
| http://localhost:8088 | YARN ResourceManager — running / queued jobs |
| http://localhost:9870 | HDFS NameNode — DataNodes, blocks, disk usage |
| http://localhost:8998/sessions | Livy REST — active Spark sessions |
| http://localhost:9083 (TCP) | Hive Metastore Thrift (debugging only) |
| `localhost:5433` | PostgreSQL (HMS backing store) — pgAdmin / psql |

Select the **PySpark** kernel when opening a notebook. The first cell (`%%info`) takes ~30 s while YARN starts the Spark executor.

### Inspect HMS metadata with pgAdmin

The PostgreSQL metastore is exposed on `localhost:5432` with `trust` auth — the server accepts any password. Connect from pgAdmin Desktop:

1. **Register > Server**: Name = `HMS (learn-jupyterhub)`
2. **Connection** tab:
   - Host: `localhost`
   - Port: `5433`
   - Database: `metastore`
   - Username: `hive`
   - Password: `hive` *(any value works — server ignores it under `trust` auth, but pgAdmin requires a non-empty field)*
3. Navigate to **Schemas > public > Tables** to browse the HMS schema.

Useful queries once connected:
```sql
-- Check which databases HMS knows about and where they're located
SELECT "DB_ID", "NAME", "DB_LOCATION_URI" FROM "DBS";

-- List all tables and their HMS types (MANAGED_TABLE, EXTERNAL_TABLE, ...)
SELECT d."NAME" AS db, t."TBL_NAME", t."TBL_TYPE"
FROM   "TBLS" t JOIN "DBS" d ON t."DB_ID" = d."DB_ID";

-- Check where a specific table stores its Parquet files
SELECT t."TBL_NAME", s."LOCATION"
FROM   "TBLS" t JOIN "SDS" s ON t."SD_ID" = s."SD_ID"
WHERE  t."TBL_NAME" = 'trades';
```

### Daily workflow

```powershell
# Morning — resume everything (idempotent, skips already-running services)
.\scripts\start-all.ps1

# Evening — pause (keeps kind cluster + volumes; fast to resume next day)
.\scripts\stop-all.ps1

# Reset — delete kind cluster, stop containers, keep HDFS volumes
.\scripts\stop-all.ps1 -Destroy

# Nuclear — delete kind cluster + ALL volumes (full clean slate; start-all.ps1 rebuilds from scratch)
.\scripts\stop-all.ps1 -Destroy -Wipe
```

> **Resuming after a pause**: `start-all.ps1` is idempotent. It reconnects the kind node to the Compose network, restarts the socat proxy, and re-runs the HDFS init + HMS cleanup check automatically. You do **not** need to re-run anything manually after a plain pause — HDFS data lives in named volumes (`namenode-data`, `datanode-1-data`, `datanode-2-data`) and survives container restarts.

### Validate first run

```powershell
# Quick cluster health check from the terminal
docker compose ps                                        # all services healthy?
docker exec namenode hdfs dfsadmin -report               # 2 DataNodes live?
docker exec namenode yarn node -list                     # 2 NodeManagers RUNNING?
```

Then open `notebooks/04_validate_yarn_hdfs.ipynb` in JupyterHub, select **PySpark** kernel, and run all cells.

---

## How it works

```
Browser → JupyterHub → JupyterLab (SparkMagic kernel)
                              │  POST /sessions, POST /statements
                              ▼
                         Livy :8998  ← Spark DRIVER lives here (deploy-mode=client)
                              │  submit → YARN  (Phase 5+)
                              ▼
                    YARN ResourceManager → NodeManager (executor JVMs)
                              │  executor callbacks → driver (TCP)
                        HDFS NameNode + DataNode  ← Parquet files
                    HMS Thrift :9083 → postgres   ← table metadata
```

The **kernel selector** decides where code runs:

| Kernel | Where code executes |
|---|---|
| `python3` (IPython) | Inside the user container / laptop |
| `PySpark` (SparkMagic) | Livy JVM on the edge node → YARN executors |

SparkMagic **never imports PySpark locally**. It serialises the cell, POSTs it to Livy, and polls for the result. The user container stays near-idle — that is the resource win.

The **Livy container is the edge node**: it has Spark jars + Hadoop client config, sits on the cluster network, and runs the Spark driver. YARN executors phone back to the driver over TCP, which is why `spark.driver.host=livy` must be set explicitly.

---

## One binary, every role — the daemon dispatch pattern

Hadoop ships as a single tarball. The `hdfs` and `yarn` CLIs inside it are not separate binaries per daemon — they are sub-commands of one distribution:

```
hdfs namenode          # HDFS metadata server
hdfs datanode          # HDFS block storage node
yarn resourcemanager   # YARN job scheduler
yarn nodemanager       # YARN executor host
```

This project's `docker/hadoop/Dockerfile` downloads that tarball once and bakes nothing else in. The role is passed at runtime via the `HADOOP_ROLE` environment variable. `docker/hadoop/entrypoint.sh` is the dispatcher — a `case` switch that calls the right sub-command:

```bash
case "$ROLE" in
  namenode)        exec hdfs namenode         ;;
  datanode)        exec hdfs datanode         ;;
  resourcemanager) exec yarn resourcemanager  ;;
  nodemanager)     exec yarn nodemanager      ;;
esac
```

This lets `docker-compose.yml` spin up six containers from one image, each with a different `HADOOP_ROLE`.

**Why `exec` and not just a bare command call?**
The `exec` shell built-in *replaces the shell process* with the Java daemon — the daemon becomes PID 1 inside the container. Docker sends `SIGTERM` to PID 1 on `docker compose stop`. Hadoop daemons handle it for graceful shutdown (flushing buffers, deregistering from NameNode, etc.). Without `exec`, the signal goes to `bash`, the JVM gets `SIGKILL`'d abruptly, and you risk HDFS block corruption or an unclean NameNode journal.

A secondary benefit: `docker logs datanode-1` shows only DataNode output — no shell noise, no interleaved daemon output.

**The first-run format guard (NameNode only)**

HDFS must be formatted exactly once to initialise the filesystem metadata structure. Re-formatting an existing cluster destroys all data and leaves DataNodes holding blocks the NameNode no longer knows about. The entrypoint guards with:

```bash
if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
  hdfs namenode -format -nonInteractive -force
fi
exec hdfs namenode
```

The `VERSION` file is written by `hdfs namenode -format` and never exists before the first successful format — making this check idempotent across restarts.

**Config is never baked in**

All XML files (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `capacity-scheduler.xml`, …) are bind-mounted from `config/hadoop/` at runtime. Change a property and `docker compose restart resourcemanager` — no image rebuild or layer invalidation.

> This same role-dispatch pattern is used by production-grade Hadoop-on-Kubernetes deployments (Bitnami Hadoop Helm chart, AWS EMR on EKS): the same image, same `HADOOP_ROLE` env var, but in a K8s `StatefulSet` spec rather than a Compose `service`.

---

## Spark cluster managers: YARN, Kubernetes, Standalone

Spark is a distributed computation engine — by itself it runs in a single JVM. To distribute executors across machines it delegates scheduling to a *cluster manager*. Spark ships with three built-in options:

| Cluster manager | What schedules executor JVMs / Pods | Typical storage | Used in this project |
|---|---|---|---|
| **Standalone** | Spark's own `spark-master` + `spark-worker` daemons | Local FS, NFS | Phase 1 |
| **YARN** | Hadoop `resourcemanager` + `nodemanager`s | HDFS | Phase 5+ |
| **Kubernetes** | K8s API server directly | S3 / GCS / MinIO / HDFS | — (Phase 6+) |

### `--master yarn` (this project)

Livy submits with `--master yarn --deploy-mode client`. YARN's ResourceManager receives the application, picks NodeManagers with free capacity, and launches executor JVMs there. HDFS is the storage layer but is completely separate from YARN — YARN only schedules *compute*, not storage.

### `--master k8s://https://…` (Kubernetes-native Spark)

This does **not** mean "Hadoop running on Kubernetes". It means Spark talks directly to the Kubernetes API server and requests executor *Pods* — no YARN, no ResourceManager, no NodeManager. Spark on K8s is a standalone execution path that bypasses the Hadoop execution stack entirely.

Storage in this mode is typically S3-compatible (MinIO, AWS S3, GCS) because operating HDFS inside Kubernetes, while possible, is operationally heavier. You can run `--master k8s://` with HDFS as storage — but HDFS is then just another set of Pods, not related to the scheduling path.

### Hadoop on Kubernetes — the full picture

You *can* deploy NameNode, DataNode, ResourceManager, and NodeManager as K8s Pods using a Helm chart (Bitnami's chart does exactly this). Under the hood it uses the same role-dispatch pattern:

```yaml
# K8s StatefulSet (same image, same entrypoint, different env var)
env:
  - name: HADOOP_ROLE
    value: datanode
```

Once running, Livy submits to YARN exactly as in Phase 5 — the Pods are just containers on a different orchestrator. The two axes are independent:

| | YARN (Hadoop execution) | K8s-native Spark |
|---|---|---|
| **Storage: HDFS** | Classic setup (this project) | HDFS Pods + `k8s://` master |
| **Storage: S3/MinIO** | YARN + `s3a://` paths | Typical cloud-native setup |

The reason Kubernetes-native Spark is popular in the cloud is operational: managed K8s (EKS, GKE, AKS) is ubiquitous, and object storage eliminates HDFS administration. YARN remains the dominant choice on-prem or when migrating an existing Hadoop estate.

---

## Hadoop deployment options

| Option | Disk | Notes |
|---|---|---|
| **Pseudo-distributed single node** (bare Hadoop install) | ~2–4 GB | Fragile to host changes; nothing is isolated |
| **Dockerized mini cluster** (2–4 containers) ← *this project* | ~5–8 GB | Ephemeral, repeatable; `docker compose up/down` |
| **Cloudera/Hortonworks sandbox VM** | ~15–20 GB | Pre-configured; heavyweight; overkill for wiring learning |

This project uses the **Dockerized approach**: a single `docker-compose.yml` that grows phase-by-phase. Nothing is installed on the host; tear down with one command; reset by deleting volumes.

---

## Phase roadmap

Each validated phase is tagged in git — any phase is reproducible:

```powershell
git checkout phase-3 ; docker compose up --build   # restore Phase 3 exactly
git checkout master                                 # back to latest
git tag                                             # list all tags
```

| Phase | Status | Git tag | Adds |
|---|---|---|---|
| 1 — Core chain | done | `phase-1` | `spark-master`, `spark-worker`, `livy`, single `jupyter` |
| 2 — JupyterHub | done | `phase-2` | DockerSpawner; per-user containers + volumes |
| 3 — Hive Metastore | done | `phase-3` | `postgres` + `hive-metastore`; persistent SQL catalog |
| 4 — kind + KubeSpawner | done | `phase-4` | K8s-in-Docker; Zero-to-JupyterHub Helm chart |
| 5 — YARN + HDFS | **in progress** | | 6-container Hadoop (1 daemon/container); Livy → YARN; replication=2; warehouse on HDFS |
| 6 — Kerberos + SPNEGO | planned | | MIT KDC; SPNEGO on Livy + HMS; `kinit`-renewer sidecar |
| 7 — Production docs | planned | | Architecture doc; production checklist; YARN queues |

> **Why Kerberos is last**: it is a cross-cutting concern that touches every service simultaneously. Validating each service individually first is much easier.

---

## Phase 1 — Core chain

> Single `jupyter` container wired to Livy → Spark standalone. No JupyterHub.

```powershell
git checkout phase-1 ; docker compose up --build -d
```

| URL | What |
|---|---|
| http://localhost:8888 | JupyterLab (no login) |
| http://localhost:8080 | Spark Master UI |
| http://localhost:8998/sessions | Livy REST |

---

## Phase 2 — JupyterHub

> Adds JupyterHub with DockerSpawner. Each login spawns a dedicated container + persistent volume.

```powershell
git checkout phase-2 ; docker compose up --build -d
```

http://localhost:8000 — login: `alice` / `sparkmagic`

Open `shared-notebooks/02_validate_resource_isolation.ipynb`, **PySpark** kernel.

| Check | Pass condition |
|---|---|
| `%%info` | Session `idle` |
| `socket.gethostname()` | Livy container hostname — NOT the jupyter container |
| `sc.master` | `spark://spark-master:7077` |
| `%%local` SparkContext check | `_active_spark_context` is `None` — no local driver |
| Monte-Carlo pi | Completes without error |

While a job runs: `docker stats --no-stream` — `spark-worker` ~80% CPU, `livy` ~20%, `jupyter-alice` ~0%.

---

## Phase 3 — Hive Metastore

> Adds `postgres` (HMS backing store) and `hive-metastore` (Thrift :9083).
> Table metadata persists across sessions; all users share one catalog.

```powershell
git checkout phase-3 ; docker compose up --build -d
```

http://localhost:8000 — login: `alice` / `sparkmagic`

Open `shared-notebooks/03_validate_hive_metastore.ipynb`, **PySpark** kernel, run all cells.

| Step | Pass condition |
|---|---|
| `%%info` | Session `idle` |
| `catalogImplementation` | `hive` |
| `metastore.uris` | `thrift://hive-metastore:9083` |
| `CREATE DATABASE/TABLE` | `SHOW DATABASES` lists `risk_dw` |
| `saveAsTable("risk_dw.trades")` | Prints `Loaded 10,000 rows` |
| Session restart → `COUNT(*)` | `10,000` — data on Docker volume survives |
| Login as **bob**, `SELECT trader …` | Returns rows — no DDL needed |

```powershell
# Inspect HMS metadata directly
docker exec -it postgres psql -U hive -d metastore -c 'SELECT TBL_NAME, TBL_TYPE FROM "TBLS";'
# Verify Parquet files on the shared volume
docker exec hive-metastore find /opt/hive/data/warehouse -name "*.parquet" | Select-Object -First 5
```

### Data flow (Phase 3)

```
SparkMagic → Livy (driver) → hive-metastore :9083 → postgres :5432
             Livy (driver) → spark-master:7077 → spark-worker (executors)
             spark-worker  → hive-warehouse volume (Parquet files)
```

---

## Phase 4 — kind + KubeSpawner

> Moves JupyterHub into a kind (K8s-in-Docker) cluster. The Compose stack becomes the data plane.

```
Browser :8001 → JupyterHub Pod (kind) → singleuser Pod
                    └─► SparkMagic → http://host.docker.internal:8998 → Livy (Compose)
```

### Prerequisites (install once)

```powershell
winget install Kubernetes.kind Helm.Helm Kubernetes.kubectl
# verify after restarting terminal:
kind version ; helm version ; kubectl version --client
```

> Alternatives: `choco install kind kubernetes-helm kubernetes-cli` or `brew install kind helm kubectl`

### Start

```powershell
docker compose up --build -d
.\scripts\phase4-up.ps1                  # kind cluster + Helm deploy (idempotent)
kubectl get pods -n jhub -w             # wait for hub-* → 1/1 Running (~2 min)
```

http://localhost:8001 — login: `alice` / `sparkmagic`

### Validate

Open `shared-notebooks/03_validate_hive_metastore.ipynb`, **PySpark** kernel.

| Check | Pass condition |
|---|---|
| `%%info` | Livy URL = `http://host.docker.internal:8998` |
| `%%sql SELECT COUNT(*) FROM risk_dw.trades` | `10000` |

```powershell
kubectl get pods -n jhub        # one hub Pod + one user Pod while notebook is open
.\scripts\phase4-down.ps1       # delete kind cluster when done
docker compose down             # stop data plane
```

| This stack | Production |
|---|---|
| `kind` single-node | EKS / GKE / AKS |
| `host.docker.internal:8998` | Livy hostname / load balancer |
| ConfigMap for SparkMagic config | Helm values / K8s Secret |

---

## Phase 5 — YARN + HDFS

> Replaces Spark standalone with a real Hadoop cluster running inside Docker.
> Livy now submits jobs to YARN instead of directly to `spark-master`.
> Hive warehouse moves from a Docker volume to HDFS.
> **No Kerberos** — `hadoop.security.authentication=simple` (Phase 6 adds auth).

### Architecture

```
SparkMagic → Livy  (Spark driver, deploy-mode=client)
               │
               │  submit() ─────────────────────────► YARN ResourceManager
               │                                        │  schedule + launch executor JVMs
               │                                        ▼
               │                              nodemanager-1  nodemanager-2
               │  executor callbacks (TCP) ◄──────────────────────────────
               │
               │  open(hdfs://namenode:9000/…)
               ▼
           NameNode  ── block map ──►  datanode-1   datanode-2
          (metadata)                  (block bytes, replication=2)
          hive-metastore :9083 ──► postgres  (table → HDFS path)
```

**deploy-mode=client**: the Spark *driver* runs inside the Livy container — not on a YARN node. Executor JVMs run on NodeManagers and phone home to the driver over TCP. This is why `spark.driver.host=livy` must exactly match the Livy container name: executors discover the driver address through YARN's application master metadata.

### One image, six containers

All six Hadoop containers build from the same `docker/hadoop/Dockerfile`. The `HADOOP_ROLE` environment variable tells `entrypoint.sh` which daemon to `exec` as PID 1:

```
HADOOP_ROLE=namenode        → exec hdfs namenode         (namenode)
HADOOP_ROLE=datanode        → exec hdfs datanode         (datanode-1, datanode-2)
HADOOP_ROLE=resourcemanager → exec yarn resourcemanager  (resourcemanager)
HADOOP_ROLE=nodemanager     → exec yarn nodemanager      (nodemanager-1, nodemanager-2)
```

Running each daemon as PID 1 means Docker receives SIGTERM directly on `docker compose stop`, enabling graceful shutdown. Logs stay focused on a single process per container (`docker logs datanode-1`), and any single daemon can be restarted without touching the others (`docker compose restart datanode-2`).

### Container inventory

| Container | Daemon | Accessible at | Persistent volume |
|---|---|---|---|
| `namenode` | `hdfs namenode` | http://localhost:9870 (UI), :9000 (RPC) | `namenode-data` |
| `datanode-1` | `hdfs datanode` | NameNode UI → DataNodes tab | `datanode-1-data` |
| `datanode-2` | `hdfs datanode` | NameNode UI → DataNodes tab | `datanode-2-data` |
| `resourcemanager` | `yarn resourcemanager` | http://localhost:8088 (UI) | — |
| `nodemanager-1` | `yarn nodemanager` | ResourceManager UI → Nodes tab | — |
| `nodemanager-2` | `yarn nodemanager` | ResourceManager UI → Nodes tab | — |

### HDFS replication = 2

When Spark writes a Parquet file, the HDFS client asks the NameNode for a block write pipeline. The NameNode returns two DataNode addresses. The client streams data to `datanode-1`, which forwards each 64 KB packet to `datanode-2`. Both nodes send acknowledgements back up the chain before the write is confirmed.

```
Write pipeline (replication=2):

  Spark executor
      │  write block
      ▼
  datanode-1  ──── forward ────►  datanode-2
      │                                │
      └─────────── ack chain ◄─────────┘
      │
  NameNode ◄── block report (after write completes)
```

If `datanode-2` goes offline, every block is still readable from `datanode-1`. The NameNode detects under-replication via missed heartbeats and automatically schedules re-replication once the DataNode returns.

> **Volume semantics**: `docker compose down` stops containers but **keeps volumes** — HDFS block data survives.
> `docker compose down -v` **deletes all volumes** — all HDFS data is gone. Re-run `phase5-init-hdfs.ps1` before starting over.

### YARN resource model

Two NodeManagers × 4 GB × 4 vCores = **8 GB + 8 vCores** total cluster capacity. YARN tracks both dimensions per executor allocation. Virtual memory checking is disabled (`vmem-check-enabled=false`) because JVM virtual address space always overflows NodeManager limits inside Linux containers.

NodeManagers are **stateless** — they hold no block data. Stopping one kills in-flight executor JVMs on that node; YARN marks those tasks failed and reschedules them on the surviving NodeManager.

### JAR staging (first YARN job only)

The first SparkContext created in a fresh cluster triggers a one-time upload of ~220 MB of Spark executor JARs to `hdfs://namenode:9000/tmp` (`spark.yarn.stagingDir`). Subsequent sessions reuse YARN's distributed-cache copy — only the first job pays the upload cost. The cache is invalidated only by `docker compose down -v`.

### Config changes from Phase 4

| File | Change |
|---|---|
| `config/livy/livy.conf` | `livy.spark.master = yarn` |
| `config/livy/livy-env.sh` | `HADOOP_CONF_DIR=/etc/hadoop/conf` |
| `config/livy/spark-defaults.conf` | `spark.driver.host=livy`, `spark.driver.bindAddress=0.0.0.0`, `spark.yarn.stagingDir` |
| `config/hive/hive-site.xml` | `hive.metastore.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse` |
| `config/hadoop/` (new) | `core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml` |

### Start

```powershell
# Start everything (data plane + HDFS init + HMS cleanup + K8s front-end)
.\scripts\start-all.ps1
kubectl get pods -n jhub -w
```

http://localhost:8001 — login: `alice` / `sparkmagic`

### Validate

Two notebooks cover different aspects of the cluster:

**`notebooks/04_validate_yarn_hdfs.ipynb`** — core wiring

| Check | Pass condition |
|---|---|
| `%%info` | Session `idle` |
| `sc.master` | `yarn` |
| `hadoopConf.get("fs.defaultFS")` | `hdfs://namenode:9000` |
| `sc._conf.get("spark.driver.host")` | `livy` |
| `catalogImplementation` | `hive` |
| Write table → restart → `COUNT(*)` | `10,000` |
| http://localhost:8088 | YARN app `RUNNING` → `SUCCEEDED` |
| http://localhost:9870 | HDFS shows `/user/hive/warehouse/risk_dw.db` |

**`notebooks/05_validate_cluster_elasticity.ipynb`** — multi-node resilience

| Check | Pass condition |
|---|---|
| HDFS JMX | 2 DataNodes `Live` |
| YARN REST | 2 NodeManagers `RUNNING` |
| Block replication check | Every block present on both DataNodes |
| Stop `datanode-2` | Blocks still readable from `datanode-1` |
| Restart `datanode-2` | NameNode re-replicates; 0 under-replicated blocks |
| Stop `nodemanager-2` | Job completes on `nodemanager-1` |
| `down` / `up -d` | Data intact — named volumes preserved |

```powershell
# Cluster health from the terminal
docker exec namenode hdfs dfsadmin -report      # DataNode live/dead + block totals
docker exec namenode yarn node -list            # NodeManager registrations
docker exec namenode hdfs dfs -ls -R /user/hive/warehouse
```

### Inspecting HDFS blocks

HDFS has three distinct layers you can inspect independently:

| Layer | What it shows | Command |
|---|---|---|
| **Logical** | File names, sizes, replication factor as HDFS sees them | `hdfs dfs -ls -h <path>` |
| **Block** | Maps each logical file to block IDs and the DataNode(s) holding each replica | `hdfs fsck <path> -files -blocks -locations` |
| **Physical** | Raw block files on the DataNode's local disk | `find /hadoop/dfs/data -name "blk_*"` |

> Each Parquet file smaller than the block size (128 MB default) maps to exactly **one** block. Large files are split into multiple numbered `blk_*` files. Every block has a companion `.meta` file storing CRC32c checksums for every 512-byte chunk — the DataNode verifies these on every read to detect silent disk corruption.

```powershell
# Logical view — file names, sizes, replication factor
docker exec namenode hdfs dfs -ls -h /user/hive/warehouse/risk_dw.db/trades

# Block view — maps each logical file to block IDs + DataNode addresses
docker exec namenode hdfs fsck /user/hive/warehouse/risk_dw.db/trades -files -blocks -locations

# Physical view — raw block files stored on the DataNode's disk
docker exec datanode-1 find /hadoop/dfs/data -name "blk_*" -not -name "*.meta" | sort

# Verify a block is readable — check for the Parquet magic bytes "PAR1" (hex: 50 41 52 31)
# Replace blk_* with a block ID from the fsck output above
docker exec datanode-1 sh -c 'cat /hadoop/dfs/data/current/BP-*/current/finalized/subdir0/subdir0/blk_<ID> | head -c 4 | xxd'
```

### Teardown

```powershell
.\scripts\phase4-down.ps1           # K8s front-end
docker compose down                 # stop all containers (volumes kept)
docker compose down -v              # stop + wipe all volumes (all HDFS data lost)
```

> **Data migration note**: Phase 3 Parquet files (Docker volume) do not auto-migrate to HDFS. HMS postgres metadata is preserved. The Phase 5 notebooks re-create and reload `risk_dw.trades` on HDFS.

### This stack vs. production

| This stack | Production equivalent |
|---|---|
| 2 DataNodes, replication=2 | 30+ DataNodes, replication=3, rack-aware placement |
| 2 NodeManagers, 4 GB / 4 vCores each | Hundreds of nodes, 256–1024 GB RAM each |
| Single active NameNode | HA NameNode pair (JournalNodes + ZooKeeper) |
| `hadoop.security.authentication=simple` | Kerberos (Phase 6) |
| PostgreSQL for HMS | Managed RDS / Cloud SQL |

---

## Cluster validation

Use these checks after `start-all.ps1` completes to confirm every layer of the stack is healthy before running notebooks.

### Container health

```powershell
docker compose ps                  # all services: Status = healthy / Up?
kubectl get pods -n jhub           # hub-* and proxy-* → 1/1 Running
```

Expected Compose services and their states:

| Container | Expected status |
|---|---|
| `namenode` | `healthy` |
| `datanode-1` | `Up` |
| `datanode-2` | `Up` |
| `resourcemanager` | `healthy` |
| `nodemanager-1` | `Up` |
| `nodemanager-2` | `Up` |
| `hive-metastore` | `healthy` |
| `postgres` | `healthy` |
| `livy` | `healthy` |

### HDFS — DataNodes + block replication

```powershell
# Confirm both DataNodes are registered and live
docker exec namenode hdfs dfsadmin -report 2>&1 | Select-String "Live datanodes|Name:"
# → Live datanodes (2): datanode-1 and datanode-2

# After writing data (e.g. running notebook 04), verify every block has 2 live replicas
docker exec namenode hdfs fsck /user/hive/warehouse -files -blocks -locations 2>&1 `
    | Select-String "Live_repl"
# → each block line ends with: Live_repl=2 [DatanodeInfo[datanode-1...], DatanodeInfo[datanode-2...]]

# Physical confirmation — the same blk_* IDs must appear on BOTH nodes
docker exec datanode-1 find /hadoop/dfs/data -name "blk_*" -not -name "*.meta" | sort
docker exec datanode-2 find /hadoop/dfs/data -name "blk_*" -not -name "*.meta" | sort
# → identical block IDs on both DataNodes = replication working
```

### YARN — NodeManagers + job submission

```powershell
# Confirm 2 NodeManagers are registered
docker exec namenode yarn node -list 2>&1 | Select-String "RUNNING|Total Nodes"
# → Total Nodes:2, both nodemanager-1 and nodemanager-2 in RUNNING state
```

While a notebook job is running, open http://localhost:8088 → **Applications** and watch the app progress: `ACCEPTED` → `RUNNING` → `SUCCEEDED`. The **Nodes** tab shows both NodeManagers with their allocated vCores and RAM.

### Multi-user isolation (JupyterHub on kind)

JupyterHub on kind spawns a separate Kubernetes Pod per user — they share the same Livy/YARN cluster but have completely separate notebook containers and Spark sessions.

**To test with two users simultaneously:**

1. Open http://localhost:8001 in your **main browser** → login as **`alice`** / `sparkmagic`
2. Open http://localhost:8001 in an **incognito / private window** → login as **`bob`** / `sparkmagic`
3. Both users open any notebook, select the **PySpark** kernel, and run `%%info`

```powershell
# While both sessions are active:
kubectl get pods -n jhub
# Expected output (4 pods):
#   hub-*              1/1 Running
#   proxy-*            1/1 Running
#   jupyter-alice-*    1/1 Running
#   jupyter-bob-*      1/1 Running
```

| Check | Pass condition |
|---|---|
| `kubectl get pods -n jhub` | Separate `jupyter-alice-*` and `jupyter-bob-*` pods |
| `%%info` (each user) | Same Livy URL, but different session IDs |
| http://localhost:8998/sessions | Two sessions listed — one per user |
| YARN http://localhost:8088 → Applications | Two separate applications while both kernels are active |
| `%%local` `import os; print(os.environ["JUPYTERHUB_USER"])` | `alice` in one tab, `bob` in the other |

> **Adding users**: edit `Authenticator.allowed_users` in `config/jupyterhub/helm-values.yaml`, then re-run `.\.scripts\phase4-up.ps1`. Any username in the list can log in with password `sparkmagic`.

### Notebook test suite

| Notebook | Kernel | What it validates |
|---|---|---|
| `notebooks/04_validate_yarn_hdfs.ipynb` | PySpark | Core wiring: `sc.master=yarn`, HDFS warehouse, HMS catalog, `saveAsTable` → `COUNT(*)=10,000` |
| `notebooks/05_validate_cluster_elasticity.ipynb` | PySpark | Resilience: DataNode failure/recovery, NodeManager failover, replication check, volume persistence |

See the [Phase 5 — Validate](#phase-5--yarn--hdfs) section for the full per-cell pass/fail checklist for each notebook.

---

## Resource contention

| Lever | Scope | Where configured |
|---|---|---|
| `KubeSpawner.mem_limit` / `cpu_limit` | Per-user Pod | `helm-values.yaml` |
| `numExecutors` / `executorMemory` | Per Spark session | `config-k8s.json` |
| `livy.server.session.max-creation` | Total concurrent sessions | `livy.conf` |
| YARN queue capacity | Team-level cluster share | `capacity-scheduler.xml` (Phase 7) |

With 2 NodeManagers × 4 vCores × 4 GB (8 vCores, 8 GB total) and the default session config (`numExecutors=1, executorCores=1, executorMemory=1G`), up to 8 sessions run concurrently before YARN starts queuing new ones.

---

## Resource optimization

This stack uses fixed resource allocation by default — every session pre-reserves `driverMemory + numExecutors × executorMemory` regardless of whether any code is running. On a laptop-sized cluster this exhausts YARN quickly. The following techniques address resource waste at every layer of the stack.

### Spark Dynamic Allocation

**What it solves**: executors sit idle between cells, holding YARN memory they are not using. Dynamic allocation releases idle executors and re-requests them when the next job is submitted.

**How it works**:
```
Session starts  → 0 executors allocated
Cell submitted  → YARN allocates executors up to spark.dynamicAllocation.maxExecutors
Cell finishes   → executors idle for executorIdleTimeout (default 60 s) → released
Next cell       → executors re-requested
```

Spark 3.x supports **Shuffle Tracking** (`spark.dynamicAllocation.shuffleTracking.enabled=true`), which eliminates the need for an External Shuffle Service daemon. Spark tracks which executors hold live shuffle data and only releases the rest. This is the recommended approach for this stack.

**Config — add to `config/sparkmagic/config-k8s.json` → `session_configs.conf`**:
```jsonc
"spark.dynamicAllocation.enabled": "true",
"spark.dynamicAllocation.shuffleTracking.enabled": "true",
"spark.dynamicAllocation.minExecutors": "0",
"spark.dynamicAllocation.maxExecutors": "4",
"spark.dynamicAllocation.executorIdleTimeout": "60s",
"spark.dynamicAllocation.cachedExecutorIdleTimeout": "300s"
```

When `minExecutors=0`, an idle session at a prompt holds **zero YARN containers** — the YARN app is `RUNNING` but consumes only driver memory. The executor slots are available to other users.

> **Trade-off**: first cell after an idle period pays executor startup latency (~10–15 s on YARN). For interactive notebooks this is usually acceptable.

### YARN — AM resource limit

**What it solves**: YARN's capacity scheduler caps Application Master memory as a fraction of total cluster RAM (`maximum-am-resource-percent`, default 10%). In `deploy-mode=client` the Spark *driver* is the AM — so this limit directly controls how many concurrent Livy sessions can start.

**The math for this stack**:
```
Total NM memory  = 2 nodes × 4096 MB = 8192 MB
Default AM limit = 8192 × 0.10 = 819 MB   → blocks even 1 × 1G driver
Phase 5 AM limit = 8192 × 0.50 = 4096 MB  → allows 4 × 1G drivers concurrently
```

**Config — `config/hadoop/capacity-scheduler.xml`**:
```xml
<property>
  <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
  <value>0.5</value>  <!-- 50% → 4 concurrent 1G-driver sessions; raise if you add NodeManagers -->
</property>
```

After editing, live-reload without restarting the ResourceManager:
```powershell
docker exec resourcemanager yarn rmadmin -refreshQueues
```

> **Rule of thumb**: set this to `(max_concurrent_sessions × driverMemory) / total_NM_memory`. For 4 sessions × 1G on 8G cluster = 0.5.

### YARN — Opportunistic Containers

**What it solves**: guaranteed containers hold their allocation even when the JVM is GC-pausing or waiting on I/O. Opportunistic containers use that idle headroom for short tasks without formal reservations.

**How it works**: the ResourceManager's `OpportunisticContainerAllocator` places opportunistic containers on nodes whose actual utilization is below the guaranteed allocation. They are **preempted instantly** (SIGKILL) when a guaranteed container needs the slot — no grace period.

**Suitable for**: data exploration cells, `COUNT(*)` queries, schema inspection. Not suitable for multi-stage jobs with shuffle data that must survive.

**Enable in `config/hadoop/yarn-site.xml`**:
```xml
<property>
  <name>yarn.resourcemanager.opportunistic-container-allocation.enabled</name>
  <value>true</value>
</property>
<property>
  <name>yarn.nodemanager.opportunistic-containers-max-queue-length</name>
  <value>10</value>
</property>
```

> **Phase 5 status**: not enabled by default. Useful to add in Phase 7 alongside queue preemption.

### YARN — Node oversubscription

**What it solves**: YARN tracks *allocated* memory, not *used* memory. A NodeManager with 4 GB allocated and 1.5 GB actually in use has 2.5 GB of physical headroom that YARN ignores.

**How it works**: the `NodeManager` runs an Elastic Resource Monitor that samples actual CPU/memory every few seconds. The RM can place additional opportunistic containers on nodes where the gap between allocated and actually used memory is large enough.

**Enable in `config/hadoop/yarn-site.xml`**:
```xml
<property>
  <name>yarn.nodemanager.container-monitor.interval-ms</name>
  <value>3000</value>
</property>
<property>
  <name>yarn.nodemanager.resource.detect-hardware-capabilities</name>
  <value>true</value>  <!-- let YARN auto-discover cgroups reported RAM/CPU -->
</property>
```

> **Risk**: oversubscript too aggressively and the OOM killer starts terminating containers. Safe starting point: allow no more than 20% overcommit over the guaranteed allocation. Production clusters pair this with cgroup memory limits per container (`yarn.nodemanager.linux-container-executor.cgroups.strict-resource-usage=true`).

### JupyterHub — per-user Pod limits

**What it solves**: a KubeSpawner Pod with no resource limits can consume all CPU/RAM on the kind node, starving other Pods (hub, proxy, other users).

**Config — `config/jupyterhub/helm-values.yaml`**:
```yaml
singleuser:
  memory:
    limit: 1G       # OOM-kill the Pod if it exceeds this
    guarantee: 256M # Kubernetes scheduler reserves this on the node
  cpu:
    limit: 1.0      # throttle at 1 vCore
    guarantee: 0.1  # scheduler reserves 0.1 vCore
```

These are Kubernetes resource `requests` (guarantee) and `limits`. The guarantee controls **scheduling** (where the Pod lands); the limit controls **enforcement** (throttle for CPU, OOM-kill for memory).

For a notebook Pod that runs zero local computation (all work goes to Livy/YARN), `memory.guarantee=256M` + `memory.limit=512M` is usually sufficient. The SparkMagic kernel itself uses ~150 MB.

**Profile-based limits** (give power users more headroom without raising the default):
```yaml
singleuser:
  profileList:
    - display_name: Standard
      description: "256 MB — for query/exploration notebooks"
      default: true
    - display_name: Heavy
      description: "1 GB — for local pandas/matplotlib work"
      kubespawner_override:
        mem_limit: 1G
        mem_guarantee: 512M
```

### Livy — session lifecycle limits

**What it solves**: abandoned sessions (browser closed, kernel crashed) hold YARN executor slots indefinitely.

**Config — `config/livy/livy.conf`**:
```properties
# Kill a session after it has been idle for this long (no statements submitted)
livy.server.session.timeout = 1h

# Maximum concurrent sessions — new requests are rejected beyond this
livy.server.session.max-creation = 20

# How long to retain metadata for a finished session (for log retrieval)
livy.server.session.state-retain.sec = 300
```

**Heartbeat-based cleanup**: SparkMagic sends a heartbeat to Livy every `heartbeat_timeout` seconds (`config-k8s.json`). If Livy receives no heartbeat for `livy_server_heartbeat_timeout` seconds it marks the session dead and cleans it up. Set `livy_server_heartbeat_timeout` to a small multiple of `heartbeat_timeout` (e.g. `heartbeat_timeout=30`, `livy_server_heartbeat_timeout=90`) to reclaim resources within ~2 minutes of a browser close.

```jsonc
// config/sparkmagic/config-k8s.json
"heartbeat_timeout": 30,
"livy_server_heartbeat_timeout": 90
```

> **Phase 5 default**: `heartbeat_timeout=60`, `livy_server_heartbeat_timeout=0` (disabled). Set `livy_server_heartbeat_timeout` > 0 to enable automatic cleanup.

### SparkMagic — session defaults

**What it solves**: the fixed `numExecutors=1, executorMemory=1G` in `config-k8s.json` applies to every user equally. A power user running ML training and an analyst running `COUNT(*)` both get the same footprint.

**Reduce the default footprint** for interactive/exploratory work:
```jsonc
// config/sparkmagic/config-k8s.json → session_configs
"driverMemory": "512m",   // was 1G — notebook Pod only marshals results
"driverCores": 1,
"executorMemory": "512m", // was 1G — sufficient for small aggregations
"executorCores": 1,
"numExecutors": 1         // combine with dynamicAllocation to start at 0
```

**Per-user overrides via `%manage_spark`**: in JupyterLab, run `%manage_spark` in a cell to open the session management panel. From there a user can delete their current session and start a new one with custom `executorMemory`, `numExecutors`, etc. — without touching the shared config file.

**Programmatic override at session start** (add to the first cell of a notebook):
```python
%%configure -f
{
  "driverMemory": "512m",
  "executorMemory": "2G",
  "numExecutors": 3,
  "conf": {
    "spark.dynamicAllocation.maxExecutors": "6"
  }
}
```

`%%configure -f` must be the **first magic** in the notebook — it reconfigures the session before any SparkContext is created. The `-f` flag forces a restart if a session already exists.

### Optimization decision guide

| Symptom | Root cause | Fix |
|---|---|---|
| Second session stuck `ACCEPTED` forever | AM resource limit too low | Raise `maximum-am-resource-percent` in `capacity-scheduler.xml` + `refreshQueues` |
| YARN runs out of memory with few users | Fixed executors held by idle sessions | Enable `spark.dynamicAllocation` with `minExecutors=0` |
| Notebook Pod OOM-killed | No Kubernetes memory limit | Set `mem_limit` in `helm-values.yaml` |
| Abandoned sessions not cleaned up | `livy_server_heartbeat_timeout=0` (disabled) | Set to `90` in `config-k8s.json` |
| One user monopolises all executors | No `maxExecutors` cap | Add `spark.dynamicAllocation.maxExecutors` per-session |
| Short cells slow due to executor startup | Dynamic allocation cold start | Set `minExecutors=1` to keep one warm executor per session |
| Cluster node OOM under heavy load | YARN oversubscription too aggressive | Reduce `maximum-am-resource-percent`; add cgroup enforcement |

---

## Key config knobs

**Who can log in** — `config/jupyterhub/helm-values.yaml`
```yaml
DummyAuthenticator:
  password: sparkmagic
Authenticator:
  allowed_users: [alice, bob, data-engineer]
```

**Livy URL + session defaults** — `config/sparkmagic/config-k8s.json`
```jsonc
"url": "http://host.docker.internal:8998",
"auth": "None",           // Phase 6: "Kerberos"
"numExecutors": 1,
"executorMemory": "1G"
```

**YARN NodeManager resources** — `config/hadoop/yarn-site.xml`
```xml
<property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
```

**ConfigMap refresh** (after editing `config-k8s.json` or notebooks)
```powershell
.\scripts\apply-configmaps.ps1
# then: Hub Control Panel → Stop My Server → Start My Server
```

---

## Version matrix

| Component | Version |
|---|---|
| Apache Spark | 3.5.3 |
| Apache Livy | 0.9.0-incubating |
| Apache Hadoop | 3.3.6 |
| Apache Hive | 4.0.0 |
| SparkMagic | 0.23.0 |
| JupyterHub | 4.1.6 |

---

## Gotchas

1. **No `%%pyspark` prefix.** Every cell is PySpark. Special magics: `%%info`, `%%sql`, `%%local`, `%manage_spark`.
2. **`auth` is case-sensitive.** Exactly `"None"`, `"Basic_Access"`, or `"Kerberos"`.
3. **Livy 0.9 — do not set `auth.type = none`.** Leave it commented out entirely.
4. **No `python` binary in `apache/spark`.** Fixed by `ln -s /usr/bin/python3 /usr/bin/python` in the Livy Dockerfile.
5. **singleuser image must be loaded into kind.** `phase4-up.ps1` does this via `kind load docker-image`. Rebuild = re-run the script.
6. **First YARN job is slow (~1–2 min).** Spark uploads ~220 MB of executor JARs to HDFS staging on first submit. Subsequent jobs reuse the cached archive.
7. **`vmem-check-enabled=false` is mandatory in Docker.** JVM virtual memory always exceeds NodeManager limits in containers. Set in `config/hadoop/yarn-site.xml`.
8. **NameNode format is one-time.** `entrypoint.sh` guards against re-format (checks for the `VERSION` file). Do not delete `namenode-data` while `datanode-data` still exists — the DataNode will reject the cluster ID mismatch.
9. **`spark.driver.host=livy` must match the container name exactly.** NodeManager executors phone home to this host:port. Wrong value = executors start but immediately disconnect.
10. **Config changes take effect on next server start.** Stop/restart from Hub Control Panel → Stop My Server → Start My Server.
11. **replication=2 requires 2 live DataNodes for new writes.** If one DataNode is stopped, write requests fail with `could not be replicated to 2 nodes`. Existing blocks are still readable from the remaining DataNode. Restart the stopped DataNode to resume writes.
12. **Never swap DataNode volumes between containers.** `datanode-1-data` contains a `VERSION` file embedding the cluster ID issued by the NameNode. Mounting it on `datanode-2` (or vice versa) causes an immediate cluster-ID mismatch and the DataNode refuses to connect.
13. **Under-replication is not a fatal error.** The NameNode UI reports under-replicated blocks after a DataNode outage but queries still succeed as long as at least one replica exists. The NameNode schedules re-replication automatically once the DataNode reconnects.
14. **YARN AM resource limit blocks concurrent sessions.** The capacity scheduler's `maximum-am-resource-percent` (default 10%) caps how much total cluster memory can be used by Application Masters — the Spark driver in `deploy-mode=client` counts as an AM. With 2 × 4 GB NodeManagers (8192 MB total), 10% = 819 MB, which is not enough for even one 1 GB driver. Symptom: second session stays `ACCEPTED` forever; YARN diagnostics say `Queue's AM resource limit exceeded`. Fix: set `yarn.scheduler.capacity.maximum-am-resource-percent` to `0.5` in `config/hadoop/capacity-scheduler.xml` and run `docker exec resourcemanager yarn rmadmin -refreshQueues` (no restart needed). At 50% = 4096 MB, four 1 GB-driver sessions can run concurrently.
