#!/bin/bash
# entrypoint.sh — start the Hadoop daemon specified by HADOOP_ROLE.
#
# Usage (set via docker-compose environment:):
#   HADOOP_ROLE=namenode         → hdfs namenode  (formats on first run)
#   HADOOP_ROLE=datanode         → hdfs datanode
#   HADOOP_ROLE=resourcemanager  → yarn resourcemanager
#   HADOOP_ROLE=nodemanager      → yarn nodemanager

set -euo pipefail

ROLE="${HADOOP_ROLE:-namenode}" # Default to namenode for easier testing, but in production this should be set explicitly for each container.

export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-/etc/hadoop/conf}"

case "$ROLE" in
  namenode)
    # Format only on first run — guard by checking for the VERSION file.
    # Re-formatting a live cluster wipes all data and invalidates DataNode blocks.
    if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
      echo "==> [namenode] Formatting HDFS (first run)..."
      hdfs namenode -format -nonInteractive -force
    else
      echo "==> [namenode] Already formatted, skipping format."
    fi
    exec hdfs namenode
    ;;

  datanode)
    exec hdfs datanode
    ;;

  resourcemanager)
    exec yarn resourcemanager
    ;;

  nodemanager)
    # Ensure NM work dirs exist — YARN won't create them automatically
    mkdir -p /tmp/nm-local-dir /tmp/nm-logs
    exec yarn nodemanager
    ;;

  *)
    echo "ERROR: Unknown HADOOP_ROLE='$ROLE'."
    echo "Valid roles: namenode | datanode | resourcemanager | nodemanager"
    exit 1
    ;;
esac
