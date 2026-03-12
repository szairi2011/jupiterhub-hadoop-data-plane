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

## Preventing resource contention (many engineers in parallel)

Moving the driver from the jumpbox to Livy eliminates per-user driver RAM piling up on the
jumpbox, but engineers can still compete for the same executor slots. These are the levers:

### Per-user container limits (already in this stack)

Each JupyterLab container is capped at spawn time:

```python
# config/jupyterhub/jupyterhub_config.py
c.DockerSpawner.mem_limit = "2G"
c.DockerSpawner.cpu_limit = 2
```

This prevents one user hogging the singleuser container -- but the user can still request
unlimited Spark executors.

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
| `DockerSpawner.mem_limit` / `cpu_limit` | Per-user JupyterLab container | `jupyterhub_config.py` |
| `numExecutors` / `executorMemory` | Per Spark session | `config.json` |
| `livy.server.session.max-creation` | Total concurrent sessions on edge node | `livy.conf` |
| YARN queue capacity | Team-level cluster share | `capacity-scheduler.xml` on cluster |

---

## Quick start

```powershell
copy .env.example .env   # edit REPO_ROOT to this folder's absolute path
docker compose up --build
```

| URL | What |
|---|---|
| http://localhost:8000 | JupyterHub -- login: `alice` / `bob` / `data-engineer`, password: `sparkmagic` |
| http://localhost:8080 | Spark Master UI |
| http://localhost:8998/sessions | Livy REST API |

```powershell
docker compose down                      # stop (volumes kept)
docker compose down -v                   # stop + wipe volumes
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

**Who can log in** -- `config/jupyterhub/jupyterhub_config.py`
```python
c.Authenticator.allowed_users = {"alice", "bob", "data-engineer"}
c.DummyAuthenticator.password = "sparkmagic"   # dev only; replace with PAM/LDAP in Phase 3
```

**Livy URL + session defaults** -- `config/sparkmagic/config.json`
```jsonc
"url": "http://livy:8998",   // change to edge-node hostname in prod
"auth": "None",               // Phase 3: "Kerberos"
"numExecutors": 1,
"executorMemory": "1G"        // must be <= SPARK_WORKER_MEMORY
```

**Spark worker resources** -- `docker-compose.yml`
```yaml
SPARK_WORKER_MEMORY: "2G"
SPARK_WORKER_CORES: "2"
```

**Per-user container limits** -- `jupyterhub_config.py`
```python
c.DockerSpawner.mem_limit = "2G"
c.DockerSpawner.cpu_limit = 2
```

---

## Phase roadmap

Each completed phase is tagged in git (`phase-1`, `phase-2`, …) so you can always restore an
exact working state with `git checkout phase-N`. New phases are added as **Docker Compose overlay
files** (`docker-compose.phaseN.yml`) that extend the base rather than modify it:

```powershell
# run phase 3 (Hive) on top of the base stack
docker compose -f docker-compose.yml -f docker-compose.phase3.yml up
```

The base `docker-compose.yml` always stays runnable at the latest validated phase.

| Phase | Status | Git tag | Compose file | Adds |
|---|---|---|---|---|
| 1 -- Core chain | done | `phase-1` | `docker-compose.yml` | `spark-master`, `spark-worker`, `livy`, single `jupyter` |
| 2 -- JupyterHub | done | `phase-2` | `docker-compose.yml` | DockerSpawner; per-user containers + volumes |
| 3 -- Hive Metastore | next | | `docker-compose.phase3.yml` | HMS container; `%%sql SHOW TABLES` against real catalog |
| 4 -- kind + KubeSpawner | planned | | `docker-compose.phase4.yml` | K8s-in-Docker; Zero-to-JupyterHub Helm chart |
| 5 -- Kerberos + SPNEGO | planned | | `docker-compose.phase5.yml` | MIT KDC; SPNEGO on Livy + HMS; `kinit`-renewer sidecar |
| 6 -- Production docs | planned | | -- | Architecture doc; production checklist; YARN config |

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
5. **DockerSpawner volume paths are HOST paths.** `REPO_ROOT` must be set in `.env` (copy `.env.example`).
6. **Kerberos TGTs expire (~10h).** Long sessions need a `kinit -R` sidecar -- Phase 5.
7. **Executor -> driver callbacks require network.** Livy must be on the Hadoop network; NodeManagers must have a TCP route back to the driver's IP:port -- the firewall rule ops teams most often forget.
8. **Windows RDP jumpbox cookies are isolated by the OS.** Each engineer RDPs as their own AD account; cookies land in `C:\Users\<account>\AppData\...` under separate ACLs. The risk is a shared service account -- a Windows/AD problem, not a JupyterHub one.
9. **Config changes take effect on next server start.** Stop/restart from Hub Control Panel -> Stop My Server -> Start My Server.
