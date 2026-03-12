#!/usr/bin/env bash
# livy-env.sh — Environment variables sourced by bin/livy-server at startup.
# Only shell exports are valid here (no Java properties syntax).

# Must match the SPARK_HOME in the Livy container (apache/spark:3.5.3 installs to /opt/spark)
export SPARK_HOME=/opt/spark

# JAVA_HOME is set by the apache/spark base image — no override needed
# export JAVA_HOME=/usr/local/openjdk-11

# Phase 3 (YARN): point to the cluster's Hadoop XML files so Livy can reach YARN/HDFS
# export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=

# Livy log directory — must be writable by the Livy process (created in Dockerfile)
export LIVY_LOG_DIR=${LIVY_HOME:-/opt/livy}/logs

# JVM heap for the Livy server process itself (not the Spark driver).
# 512 m is enough for ~20 concurrent sessions; raise to 1-2 g in production.
export LIVY_SERVER_JAVA_OPTS="-Xms256m -Xmx512m"
