# learn-jupyterhub-with-livy

> Migrate data engineers off a shared Jupyter jumpbox onto **JupyterHub -> SparkMagic -> Livy -> Spark/YARN**.

---

## The kernel selector decides where your code runs

```
+---------------------------------------------------------------------+
|  Jupyter kernel dropdown                                            |
|                                                                     |
|  * python3  (IPython)    -> code executes IN the user container     |
|                             Spark driver = user's container/laptop  |
|             -------------------------------------------------------  |
|  * PySpark  (SparkMagic) -> cell text POSTed to Livy over HTTP     |
|                             Spark driver = Livy JVM on edge node   |  <- this stack
+---------------------------------------------------------------------+
```

SparkMagic **never imports PySpark locally**. It serialises the cell, POSTs it to
`http://livy:8998`, and polls for the result. The user container stays near-idle regardless of job
size -- that is the resource win.

---

## Full data path

```
Browser
  | WebSocket
  v
JupyterLab  (jupyter-alice container)
  | SparkMagic kernel -- POST /sessions/0/statements  (HTTP only)
  v
Livy :8998  <-- Spark DRIVER lives here  (deploy-mode = client)
  | spark://spark-master:7077
  v
Spark Master  (coordinator -- steps aside after job launch)
  | assigns executors to workers
  v
Spark Worker  <-- executors run here
  | executor callbacks back to driver  (TCP, both directions)
  +-----------------------------------------------------> Livy
```

> **`deploy-mode = client`** keeps the driver in the Livy JVM (edge node).
> `deploy-mode = cluster` would push the driver onto a worker node -- away from Livy and
> unreachable via the REST API. Livy is designed for `client` mode only.

---

## What is the edge node?

The edge node is **not a Hadoop concept**. It is simply a machine with:
- Spark jars (`$SPARK_HOME/jars`) to run a driver JVM
- Client config files pointing at the cluster addresses
- Network access to the master **and** every worker node (executors phone home to the driver)

Hadoop has zero knowledge of it. In this repo the **`livy` container is the edge node** --
it shares `spark-net` with all other services.

```
Outside world
  |
  v
Edge node (Livy)  ----------------------------------------+
  |                                                        | driver <-- executor callbacks
  | cluster-internal network                               |
  +-> Master node                                          |
  |     +- NameNode         (HDFS metadata index)          |
  |     +- ResourceManager  (YARN scheduler)               |
  |                                                        |
  +-> Worker nodes  (N machines each running)              |
          +- DataNode    -- HDFS: stores file blocks        |
          +- NodeManager -- YARN: launches containers       |
          +- Spark executor --------------------------------+
```

> **HDFS and YARN** share the same hardware but are independent systems:
> each has one coordinator brain + one local agent per machine.

---

## Spark Standalone vs YARN -- what the edge node needs

### Spark Standalone (this stack)

```
livy.spark.master = spark://spark-master:7077
```

One URL. Done. No Hadoop config files needed.

### YARN (production Hadoop cluster)

```
livy.spark.master = yarn     # just the word "yarn" -- no host:port
```

Spark then reads `$HADOOP_CONF_DIR` to find the actual addresses:

| File | Key | What it points at |
|---|---|---|
| `yarn-site.xml` | `yarn.resourcemanager.address` | YARN ResourceManager -- where jobs are submitted |
| `core-site.xml` | `fs.defaultFS` | HDFS NameNode -- where `hdfs://` paths are resolved |

**You do not write these files.** The Hadoop cluster admin hands them to you as a config package
(or you download them from Cloudera Manager / CDP / EMR web UI). Copy them onto the edge node,
set `HADOOP_CONF_DIR=/etc/hadoop/conf`, and Spark/Livy find them automatically.

In `config/livy/livy-env.sh`:
```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf   # Phase 6 only
```

And in `config/livy/livy.conf`:
```
livy.spark.master = yarn                  # Phase 6 only
```

---

## Phase 2 validation checklist

Run these steps after `docker compose up --build`.

### 1. Log in and open the validation notebook

```
http://localhost:8000
login: alice    password: sparkmagic
```

Open **`shared-notebooks/02_validate_resource_isolation.ipynb`**, select the **PySpark** kernel,
run all cells in order.

### 2. Expected results per cell

| Cell | Pass condition |
|---|---|
| `%%info` | Returns a Livy session ID with state `idle` |
| `socket.gethostname()` | Prints the **Livy container hostname** (e.g. `livy`) -- NOT `jupyter-alice` |
| `sc._conf.get("spark.submit.deployMode")` | Returns `"client"` |
| `sc.master` | `spark://spark-master:7077` |
| executor list | `driver` host = Livy container; executor host = `spark-worker` |
| `%%local` SparkContext check | `_active_spark_context` is `None` -- no local driver |
| Monte-Carlo pi | Completes without error |

### 3. Confirm with docker stats

While the Monte-Carlo cell is running, in a second terminal:

```powershell
docker stats --no-stream
```

| Container | Expected CPU | Meaning |
|---|---|---|
| `spark-worker` | ~80% | executors doing the work |
| `livy` | ~20% | Spark driver |
| `jupyter-alice` | ~0% | just an HTTP client |
| `jupyterhub` | ~0% | idle |

### 4. Confirm with Spark Master UI

Open **http://localhost:8080** -> **Running Applications** -> click the app -> **Executors** tab.
Driver host = `livy`, executor host = `spark-worker`.

---

## Phase 3 validation checklist

### Objectives

By the end of this phase you should be able to demonstrate:

| # | Objective |
|---|---|
| 1 | Spark sessions use the **Hive catalog** (`hive`), not the default in-memory catalog |
| 2 | Databases and tables can be created via `%%sql` and are stored persistently |
| 3 | Data written to a Hive-managed table **survives a Livy session restart** |
| 4 | A second user can query tables created by another user **without re-creating anything** |
| 5 | Window functions and multi-table aggregations execute correctly on Hive-managed Parquet tables |

### What Phase 3 adds

| Service | Role |
|---|---|
| `postgres` | Stores HMS metadata (database/table/column/partition records) |
| `hive-metastore` | Thrift server on `:9083`; Spark connects here to resolve table locations |
| `hive-warehouse` volume | Shared Docker volume mounted in both `hive-metastore` and `spark-worker`; holds the actual Parquet files |

Two keys are injected into every SparkMagic session via `config/sparkmagic/config.json`:

```jsonc
"conf": {
  "spark.sql.catalogImplementation": "hive",
  "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083"
}
```

### 1. Log in and open the validation notebook

```
http://localhost:8000
login: alice    password: sparkmagic
```

Open **`shared-notebooks/03_validate_hive_metastore.ipynb`**, select the **PySpark** kernel,
then run all cells in order **through Step 4** before moving to the persistence test.

### 2. Expected results per step

| Step | Cell / action | Pass condition |
|---|---|---|
| 0 — Livy session | `%%info` | Returns a session ID with state `idle` |
| 1 — Hive catalog | print `catalogImplementation` and `metastore.uris` | `hive` · `thrift://hive-metastore:9083` |
| 2 — Create objects | `CREATE DATABASE` + `CREATE TABLE` + `DESCRIBE EXTENDED` | `SHOW DATABASES` lists `risk_dw`; table shows `Provider: hive`, `Type: MANAGED` |
| 3 — Load data | `df.write.mode("overwrite").saveAsTable("risk_dw.trades")` | Prints `Loaded 10,000 rows into risk_dw.trades`; assert passes |
| 4 — Analytics | Notional by trader · top-2 instruments · monthly flow | Each `%%sql` cell returns a result set with rows (no errors) |

### 3. Persistence test (Step 5)

1. Restart the Livy session with `%manage_spark` (or the SparkMagic toolbar button).
2. **Do not re-run Step 3** — the data must already be there.
3. Run the Step 5 cell:

```python
count = spark.table("risk_dw.trades").count()
print(f"Rows after session restart: {count:,}")
assert count == 10_000
```

**Pass condition**: prints `Rows after session restart: 10,000` and the assert passes.  
**If it fails**: the `hive-warehouse` Docker volume is not mounted correctly, or `postgres` data was lost (check `docker compose down -v` was not run).

### 4. Cross-user catalog visibility (Step 6)

1. Open a **second browser tab** and log in as **bob** (`http://localhost:8000`, password: `sparkmagic`).
2. Start a new notebook, select the **PySpark** kernel.
3. Run the following cell — **no `CREATE DATABASE` or `CREATE TABLE` needed**:

```sql
%%sql
SELECT trader, COUNT(*) AS trades
FROM risk_dw.trades
GROUP BY trader
ORDER BY trades DESC
```

**Pass condition**: the query returns rows (alice/bob/carol/dave trade counts). This is the core
value of a shared metastore — **one user creates, all users discover**.

### 5. Inspect active sessions (Step 7)

Back in alice's notebook, run the `%%local` cell in Step 7. It calls the Livy REST API directly
from the singleuser container:

```
Active Livy sessions: 2
  id=0  state=idle  kind=pyspark  appId=app-...
  id=1  state=idle  kind=pyspark  appId=app-...
```

**Pass condition**: both session IDs appear, each with state `idle`.

### 6. Verify warehouse files on disk

```powershell
docker exec hive-metastore find /opt/hive/data/warehouse -name "*.parquet" | Select-Object -First 5
```

You should see Parquet part-files under `risk_dw.db/trades/`. The same path is visible inside
`spark-worker` because both containers mount the same `hive-warehouse` volume.

### 7. Validate the Hive Metastore in PostgreSQL

The Hive Metastore stores all catalog metadata (databases, tables, columns, partitions, storage
descriptors) in PostgreSQL. After running Step 3 of the notebook you should see those records
directly in the DB.

#### Connect to the postgres container

```powershell
docker exec -it postgres psql -U hive -d metastore
```

#### Check the schema was initialised

```sql
\dt
```

Expected output: a list of ~70 HMS tables (`TBLS`, `DBS`, `COLUMNS_V2`, `SDS`, `PARTITIONS`, …).

If the list is empty, the HMS schema initialisation failed — check `hive-metastore` container logs:
```powershell
docker logs hive-metastore --tail 50
```

#### Verify the `risk_dw` database record

```sql
SELECT DB_ID, NAME, DB_LOCATION_URI
FROM "DBS"
WHERE NAME = 'risk_dw';
```

Expected:

| DB_ID | NAME    | DB_LOCATION_URI                                        |
|-------|---------|--------------------------------------------------------|
| 2     | risk_dw | file:/opt/hive/data/warehouse/risk_dw.db               |

#### Verify the `trades` table record

```sql
SELECT t.TBL_ID, t.TBL_NAME, t.TBL_TYPE, d.NAME AS db_name
FROM "TBLS" t
JOIN "DBS"  d ON d.DB_ID = t.DB_ID
WHERE d.NAME = 'risk_dw';
```

Expected one row: `TBL_NAME = trades`, `TBL_TYPE = MANAGED_TABLE`.

#### Inspect column definitions

```sql
SELECT COLUMN_NAME, TYPE_NAME, INTEGER_IDX
FROM "COLUMNS_V2"
WHERE CD_ID = (
    SELECT CD_ID FROM "SDS"
    WHERE SD_ID = (
        SELECT SD_ID FROM "TBLS"
        WHERE TBL_NAME = 'trades'
    )
)
ORDER BY INTEGER_IDX;
```

Expected columns (alphabetical within the schema):

| COLUMN_NAME  | TYPE_NAME |
|--------------|-----------|
| instrument   | string    |
| notional     | double    |
| trade_date   | string    |
| trade_id     | string    |
| trader       | string    |

#### Check storage descriptor (file format and location)

```sql
SELECT s.LOCATION, s.INPUT_FORMAT, s.OUTPUT_FORMAT
FROM "SDS" s
JOIN "TBLS" t ON t.SD_ID = s.SD_ID
WHERE t.TBL_NAME = 'trades';
```

| LOCATION | INPUT_FORMAT | OUTPUT_FORMAT |
|---|---|---|
| `file:/opt/hive/data/warehouse/risk_dw.db/trades` | `org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat` | `org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat` |

#### Confirm row count is not stored in PostgreSQL

HMS does not store row counts in PostgreSQL by default — the count lives in the Parquet files. To
verify the count you must go through Spark (or `hive-metastore` stats collection). If you see
`NUM_ROWS = 10000` in `TABLE_PARAMS` it means Spark wrote statistics:

```sql
SELECT PARAM_KEY, PARAM_VALUE
FROM "TABLE_PARAMS"
WHERE TBL_ID = (
    SELECT TBL_ID FROM "TBLS" WHERE TBL_NAME = 'trades'
)
ORDER BY PARAM_KEY;
```

Look for `numRows`, `totalSize`, `spark.sql.statistics.numRows` — these confirm Spark wrote
Parquet statistics back to HMS. It is normal for these to be absent on first load.

#### Exit psql

```sql
\q
```

### Architecture data flow (Phase 3)

```
SparkMagic kernel
  | POST /sessions/0/statements
  v
Livy (driver)
  | thrift://hive-metastore:9083   <- resolve table metadata
  v
hive-metastore  ------>  postgres:5432  (store/read table records)

Livy (driver)
  | spark://spark-master:7077
  v
spark-worker (executor)
  | writes Parquet files to
  v
/opt/hive/data/warehouse  (hive-warehouse Docker volume)
  ^-- same volume mounted in hive-metastore
```

### Production equivalence

| This stack | Production |
|---|---|
| `postgres` in Docker | Managed RDS / Cloud SQL |
| `/opt/hive/data/warehouse` (Docker volume) | `hdfs:///user/hive/warehouse` |
| No auth on HMS | SASL + Kerberos (Phase 5) |
| Single HMS instance | HMS with ZooKeeper HA |

---

## Phase 4 validation checklist

Phase 4 moves JupyterHub out of Docker Compose and into Kubernetes (kind).
The Compose stack becomes a pure **data plane** — Spark, Livy, HMS, Postgres unchanged.
KubeSpawner replaces DockerSpawner: per-user **Pods** instead of containers.

```
Browser :8001
  └─► JupyterHub Pod  (kind K8s cluster)
        └─► singleuser Pod  (KubeSpawner)
              └─► SparkMagic → http://host.docker.internal:8998
                    └─► Livy  (Docker Compose)
                          └─► spark-worker  (Docker Compose)
```

### Prerequisites (install once)

The tools below must be on your `PATH` before running `phase4-up.ps1`.
All three are available via `winget` on Windows.

```powershell
# Kubernetes-in-Docker — creates local K8s clusters as Docker containers
winget install Kubernetes.kind

# Helm — K8s package manager used to deploy Zero-to-JupyterHub
winget install Helm.Helm

# kubectl — K8s CLI for inspecting Pods, ConfigMaps, logs
winget install Kubernetes.kubectl
```

Verify after install (restart your terminal first):

```powershell
kind    version   # kind v0.27.x or later
helm    version   # v3.x
kubectl version --client
```

Docker Desktop must also be running — kind creates its cluster nodes as Docker containers.

> **Alternative package managers**
> - [Chocolatey](https://chocolatey.org/): `choco install kind kubernetes-helm kubernetes-cli`
> - [Scoop](https://scoop.sh/): `scoop install kind helm kubectl`
> - macOS/Linux: `brew install kind helm kubectl`

### 1. Start the Compose data plane

```powershell
docker compose up --build -d
docker compose ps   # wait until all services are healthy
```

### 2. Bootstrap kind + JupyterHub

```powershell
.\scripts\phase4-up.ps1
```

Idempotent — safe to re-run. Does:
1. `kind create cluster` (NodePort 30000 → `localhost:8001`)
2. `docker build` singleuser image + `kind load docker-image` (no registry needed)
3. `helm upgrade --install` Zero-to-JupyterHub chart
4. `kubectl apply` ConfigMaps for SparkMagic config and shared notebooks

Watch the hub Pod start:
```powershell
kubectl get pods -n jhub -w   # wait for hub-* to reach 1/1 Running (~2 min)
```

### 3. Validate

```
http://localhost:8001    login: alice / sparkmagic
```

Open **`shared-notebooks/03_validate_hive_metastore.ipynb`** (same notebook as Phase 3),
select the **PySpark** kernel, run all cells.

| Check | Pass condition |
|---|---|
| `%%info` | Session `idle`; Livy URL = `http://host.docker.internal:8998` |
| `catalogImplementation` | `hive` |
| `%%sql SHOW TABLES IN risk_dw` | `trades` table visible (Phase 3 data persists) |
| `%%sql SELECT COUNT(*) FROM risk_dw.trades` | `10000` |

### 4. Verify Pod lifecycle

```powershell
# While notebook is open — one hub Pod + one user Pod
kubectl get pods -n jhub

# After Hub → Stop My Server — user Pod is gone within ~30 s
kubectl get pods -n jhub
```

### 5. Confirm Livy traffic from kind

```powershell
docker logs livy --tail 20   # look for POST /sessions and POST /sessions/0/statements
```

### Teardown

```powershell
.\scripts\phase4-down.ps1   # delete kind cluster
docker compose down          # stop data plane  (add -v to wipe volumes)
```

### Production equivalence

| This stack | Production |
|---|---|
| `kind` single-node | EKS / GKE / AKS |
| `host.docker.internal:8998` | Livy service hostname / LB |
| ConfigMap for SparkMagic config | Helm values / K8s Secret |
| `image_pull_policy: Never` | Registry image + `imagePullSecrets` |
| No TLS | Ingress with TLS termination (Phase 5) |

---

## Preventing resource contention (many engineers in parallel)

Moving the driver from the jumpbox to Livy eliminates per-user driver RAM piling up on the
jumpbox, but engineers can still compete for the same executor slots. These are the levers:

### Per-user Pod limits (KubeSpawner)

Each JupyterLab Pod is capped via `config/jupyterhub/helm-values.yaml`:

```python
c.KubeSpawner.cpu_limit = 2
c.KubeSpawner.mem_limit = "2G"
```

### Per-session executor limits (SparkMagic config)

Cap executors at the session level in `config/sparkmagic/config.json`:

```jsonc
"numExecutors": 1,
"executorCores": 1,
"executorMemory": "1G",
"driverMemory": "512M"
```

With 2 workers x 2 cores (4 slots total), `numExecutors=1, executorCores=1` means at most 4
concurrent sessions before queuing begins.

### Livy session limit

In `config/livy/livy.conf`, cap the total number of concurrent interactive sessions:

```
livy.server.session.max-creation = 10    # max simultaneous sessions across all users
```

### YARN queues (production only)

On a real YARN cluster the ResourceManager enforces a queue per team:

```xml
<!-- capacity-scheduler.xml (on cluster, configured by admin) -->
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>engineering,analytics,etl</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.engineering.capacity</name>
  <value>40</value>   <!-- 40% of cluster for this team -->
</property>
```

Map Livy sessions to a queue in `config/sparkmagic/config.json`:

```jsonc
"session_configs": {
  "queue": "engineering"    // Phase 6: submit to this YARN queue
}
```

### Summary

| Lever | Scope | Where configured |
|---|---|---|
| `KubeSpawner.mem_limit` / `cpu_limit` | Per-user Pod | `helm-values.yaml` |
| `numExecutors` / `executorMemory` | Per Spark session | `config-k8s.json` |
| `livy.server.session.max-creation` | Total concurrent sessions on edge node | `livy.conf` |
| YARN queue capacity | Team-level cluster share | `capacity-scheduler.xml` on cluster |

---

## Quick start

```powershell
# Data plane (Spark / Livy / HMS)
docker compose up --build -d

# JupyterHub on K8s (Phase 4)
.\scripts\phase4-up.ps1
```

| URL | What |
|---|---|
| http://localhost:8001 | JupyterHub (KubeSpawner) — `alice` / `bob` / `data-engineer`, password: `sparkmagic` |
| http://localhost:8080 | Spark Master UI |
| http://localhost:8998/sessions | Livy REST API |

```powershell
.\scripts\phase4-down.ps1              # remove kind cluster
docker compose down                    # stop data plane (volumes kept)
docker compose down -v                 # stop + wipe volumes
docker compose up --scale spark-worker=3 # scale workers
```

---

## Architecture diagram

```mermaid
graph LR
    Browser -->|HTTP :8000| JupyterHub
    JupyterHub -->|spawn| JupyterLab["JupyterLab (SparkMagic kernel)"]
    JupyterLab -->|"REST POST /sessions POST /statements"| Livy["Livy :8998 - Spark driver here"]
    Livy -->|"submit + callbacks"| Master["Spark Master :7077"]
    Master --> Worker["Spark Worker (executors)"]
```

---

## Key config knobs

**Who can log in** -- `config/jupyterhub/helm-values.yaml`
```yaml
DummyAuthenticator:
  password: sparkmagic
Authenticator:
  allowed_users: [alice, bob, data-engineer]
```

**Livy URL + session defaults** -- `config/sparkmagic/config-k8s.json`
```jsonc
"url": "http://host.docker.internal:8998",  // K8s Pods reach Compose via host-gateway
"auth": "None",                              // Phase 5: "Kerberos"
"numExecutors": 1,
"executorMemory": "1G"                       // must be <= SPARK_WORKER_MEMORY
```

**Spark worker resources** -- `docker-compose.yml`
```yaml
SPARK_WORKER_MEMORY: "2G"
SPARK_WORKER_CORES: "2"
```

**Per-user Pod limits** -- `config/jupyterhub/helm-values.yaml` (`hub.extraConfig`)
```python
c.KubeSpawner.cpu_limit = 2
c.KubeSpawner.mem_limit = "2G"
```

**ConfigMap refresh** (after editing `config-k8s.json` or `notebooks/`)
```powershell
.\scripts\apply-configmaps.ps1
# then restart the user server in JupyterHub to pick up the new ConfigMap
```

---

## Phase roadmap

A single `docker-compose.yml` grows incrementally — each phase adds services with a comment
marking when they were introduced. Each validated phase is captured as a git tag:

```powershell
git checkout phase-2          # restore exact Phase 2 state
docker compose up --build     # spin it up
```

To see all tags: `git tag`. To return to the latest: `git checkout master`.

| Phase | Status | Git tag | Adds |
|---|---|---|---|
| 1 -- Core chain | done | `phase-1` | `spark-master`, `spark-worker`, `livy`, single `jupyter` |
| 2 -- JupyterHub | done | `phase-2` | DockerSpawner; per-user containers + volumes |
| 3 -- Hive Metastore | done | `phase-3` | `postgres` + `hive-metastore`; persistent SQL catalog; `%%sql SHOW TABLES` |
| 4 -- kind + KubeSpawner | in progress | | K8s-in-Docker; Zero-to-JupyterHub Helm chart |
| 5 -- Kerberos + SPNEGO | planned | | MIT KDC; SPNEGO on Livy + HMS; `kinit`-renewer sidecar |
| 6 -- Production docs | planned | | Architecture doc; production checklist; YARN config |

> Kerberos is deliberately last -- it is a cross-cutting concern that touches every service
> (Livy, HMS, HDFS, YARN). Adding it after all services are validated individually is much easier.

---

## Version matrix

| Component | Version |
|---|---|
| Apache Spark | 3.5.3 (`apache/spark:3.5.3`) |
| Apache Livy | 0.9.0-incubating |
| SparkMagic | 0.23.0 |
| JupyterHub | 4.1.6 |
| dockerspawner | 13.0.0 |

---

## Gotchas

1. **No `%%pyspark` prefix.** `pysparkkernel` treats every cell as PySpark. Only special magics: `%%info`, `%%sql`, `%%local`, `%manage_spark`.
2. **`auth` in `config.json` is case-sensitive.** Exactly `"None"`, `"Basic_Access"`, or `"Kerberos"`.
3. **Livy 0.9 -- do not set `auth.type = none`.** Leave it commented out entirely.
4. **No `python` binary in `apache/spark` image.** Both Livy and spark-worker need `ln -s /usr/bin/python3 /usr/bin/python`.
5. **singleuser image must be loaded into kind.** `phase4-up.ps1` does this automatically via `kind load docker-image`. If you rebuild the image, re-run the script.
6. **Kerberos TGTs expire (~10h).** Long sessions need a `kinit -R` sidecar -- Phase 5.
7. **Executor -> driver callbacks require network.** Livy must be on the Hadoop network; NodeManagers must have a TCP route back to the driver's IP:port -- the firewall rule ops teams most often forget.
8. **Windows RDP jumpbox cookies are isolated by the OS.** Each engineer RDPs as their own AD account; cookies land in `C:\Users\<account>\AppData\...` under separate ACLs. The risk is a shared service account -- a Windows/AD problem, not a JupyterHub one.
9. **Config changes take effect on next server start.** Stop/restart from Hub Control Panel -> Stop My Server -> Start My Server.
