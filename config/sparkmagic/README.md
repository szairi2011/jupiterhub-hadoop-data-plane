# SparkMagic configuration — `config.json`

SparkMagic reads this file at kernel startup. It is bind-mounted read-only into
every singleuser container at `/home/jovyan/.sparkmagic/config.json`.

## Key fields explained

| Field | Value | Why |
|---|---|---|
| `kernel_python_credentials.url` | `http://livy:8998` | Docker DNS name of the Livy container on `spark-net`. Change to the actual Livy host in production. |
| `kernel_python_credentials.auth` | `"None"` | No HTTP auth. Phase 3 will set this to `"Kerberos"`. |
| `session_configs.numExecutors` | `1` | One executor is enough for the Phase 1/2 dev cluster. Raise for parallel workloads. |
| `session_configs.driverMemory` | `"1G"` | Memory for the Spark driver (runs inside the Livy container in client deploy-mode). |
| `session_configs.executorMemory` | `"1G"` | Memory per executor. Must be ≤ `SPARK_WORKER_MEMORY` in `docker-compose.yml`. |
| `session_configs.conf` | `{}` | Extra Spark properties injected per session (e.g. `spark.sql.shuffle.partitions`). |
| `livy_session_waiting_timeout` | `120` | Seconds SparkMagic waits for a Livy session to reach `idle` before giving up. 120 s is generous; the cluster typically starts in < 5 s. |
| `wait_for_idle_timeout` | `120` | Seconds SparkMagic waits for a submitted statement to complete before timing out. |
| `heartbeat_timeout` | `60` | Seconds between SparkMagic keep-alive pings to Livy. If Livy doesn't respond within this window, the kernel marks the session dead. |
| `cleanup_all_sessions_on_exit` | `true` | Deletes the Livy session (and releases Spark executors) when the kernel shuts down. Set to `false` if you want sessions to persist across notebook restarts. |
| `shutdown_session_on_spark_statement_errors` | `false` | Keep the session alive after a Spark error so you can inspect `%%info` / retry. |
| `use_auto_viz` | `true` | Auto-renders Spark DataFrames as HTML tables in `%%sql` output. |
| `max_results_sql` | `2500` | Row limit returned to the notebook from `%%sql`. Raise with care — large results are serialised through Livy JSON. |

## Changing Livy endpoint

To point at a different Livy server (e.g. a remote YARN cluster), update both
`kernel_python_credentials.url` and `kernel_scala_credentials.url`:

```json
"url": "http://yarn-edge-node.corp:8998"
```

Then restart the notebook kernel — SparkMagic reads the file once at kernel start.

## Phase 3 — Kerberos auth

```json
"auth": "Kerberos",
"username": "alice@CORP.LOCAL"
```

The singleuser container also needs a valid Kerberos ticket cache
(`KRB5CCNAME`) — handled by the kinit-renewer sidecar in Phase 3.
