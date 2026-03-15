# phase5-init-hdfs.ps1
# Called automatically by start-all.ps1 (and safe to run manually at any time).
# Idempotent — every operation is a no-op if already done.
#
# What it does:
#   1. HDFS directories
#        /tmp                     (1777) — Spark staging dir for YARN JAR uploads
#        /user/root               (700)  — Spark user home for staging artefacts
#        /user/hive/warehouse     (775)  — Hive Metastore default warehouse root
#   2. HMS cleanup (only when stale data is detected)
#        If any database in the Hive Metastore has a non-HDFS warehouse location
#        (Phase 3 leftovers from a persisted postgres-data volume), the metastore
#        database is dropped and recreated.  On restart, HMS runs
#        `schematool -initSchema --ifNotExists` and rebuilds a clean schema.
#        This prevents the silent write-to-wrong-path / count=0 bug where Spark
#        "succeeds" but writes Parquet files to the NodeManager's local FS.
#
# Prerequisite: namenode container must be healthy and out of safe mode.
#               postgres and hive-metastore containers must be running.

# This script makes native Docker + Hadoop calls that write log4j warnings to
# stderr.  Override any inherited Stop preference so those warnings don't abort
# the script; we do our own explicit error handling throughout.
$ErrorActionPreference = "Continue"

Write-Host "==> Waiting for NameNode to leave safe mode..."

$maxRetries = 40
$retries = $maxRetries

while ($retries -gt 0) {
    $result = docker exec namenode hdfs dfsadmin -safemode get 2>&1
    if ($result -match "Safe mode is OFF") {
        Write-Host "    NameNode is ready."
        break
    }
    $remaining = $retries
    Write-Host "    Still starting ($remaining retries left) - $result"
    Start-Sleep -Seconds 5
    $retries--
}

if ($retries -eq 0) {
    Write-Error "NameNode did not leave safe mode after $($maxRetries * 5) seconds. Check: docker logs namenode"
    exit 1
}

Write-Host ""
Write-Host "==> Creating HDFS directory structure..."

# /tmp — world-writable sticky (Spark uses this for staging JARs on first YARN submit)
docker exec namenode hdfs dfs -mkdir -p /tmp
docker exec namenode hdfs dfs -chmod 1777 /tmp

# /user/root — home dir for the root process user inside the Livy container
docker exec namenode hdfs dfs -mkdir -p /user/root
docker exec namenode hdfs dfs -chmod 700 /user/root

# /user/hive/warehouse — default location for Hive-managed tables
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod -R 775 /user/hive/warehouse

Write-Host ""
Write-Host "==> HDFS structure created."

# ---------------------------------------------------------------------------
# 2. Hive Metastore cleanup
#
# Check whether any database in the HMS has a non-HDFS warehouse location.
# That means Phase 3 metadata survived in the postgres-data volume.
# If Spark uses those stale entries, it writes Parquet to the NodeManager's
# local FS instead of HDFS — the write "succeeds" silently but count() = 0.
#
# Fix: drop + recreate the metastore database.  The hive-metastore container
# runs `schematool -initSchema --ifNotExists` on every startup, so after a
# restart it rebuilds a fully-initialised, empty schema automatically.
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "==> Checking Hive Metastore for stale (non-HDFS) databases..."

$staleCount = 'SELECT COUNT(*) FROM "DBS" WHERE "NAME" <> ''default'' AND "DB_LOCATION_URI" NOT LIKE ''hdfs://%'';' |
    docker exec -i postgres psql -U hive -d metastore -tAq

if ([int]($staleCount.Trim()) -gt 0) {
    Write-Host "    Found $($staleCount.Trim()) stale database(s). Resetting HMS schema..."

    # Drop the database (FORCE terminates any open connections) then recreate it.
    # `hive` is the postgres superuser in this stack (POSTGRES_USER=hive).
    docker exec postgres psql -U hive -d postgres -c "DROP DATABASE metastore WITH (FORCE);"
    docker exec postgres psql -U hive -d postgres -c "CREATE DATABASE metastore OWNER hive;"

    # Restart HMS so it reinitialises the schema via schematool.
    $repoRoot = $PSScriptRoot | Split-Path -Parent
    docker compose -f "$repoRoot/docker-compose.yml" restart hive-metastore

    Write-Host "    Waiting for Hive Metastore to reinitialise..."
    $hmsWait = 120
    $hmsElapsed = 0
    while ($hmsElapsed -lt $hmsWait) {
        $hmsHealth = docker inspect hive-metastore --format '{{json .State.Health}}' 2>$null
        $hmsStatus = if ($hmsHealth -and $hmsHealth -ne 'null') {
            ($hmsHealth | ConvertFrom-Json).Status
        } else { 'starting' }
        if ($hmsStatus -eq 'healthy') { break }
        Start-Sleep -Seconds 5
        $hmsElapsed += 5
        Write-Host "    HMS status: $hmsStatus ($hmsElapsed s / $hmsWait s)"
    }
    if ($hmsElapsed -ge $hmsWait) {
        Write-Warning "Hive Metastore did not become healthy within $hmsWait s. Check: docker logs hive-metastore"
    } else {
        Write-Host "==> HMS is healthy - schema has been reset."
    }
} else {
    Write-Host "==> HMS is clean, no stale databases."
}

Write-Host ""
Write-Host "Done. You can now open JupyterHub and run the validation notebooks."
