#!/usr/bin/env bash
# livy-env.sh — Environment variables sourced by bin/livy-server at startup.
# Only shell exports are valid here (no Java properties syntax).

# Must match the SPARK_HOME in the Livy container (apache/spark:3.5.3 installs to /opt/spark)
export SPARK_HOME=/opt/spark

# JAVA_HOME is set by the apache/spark base image — no override needed
# export JAVA_HOME=/usr/local/openjdk-11

# Phase 6 (YARN): point to the cluster's Hadoop XML files so Livy can reach YARN/HDFS
# export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_CONF_DIR=

# Phase 3: Point Spark to the Livy conf dir so spark-defaults.conf is picked up.
# spark.sql.catalogImplementation is a static SQL conf — it MUST be in
# spark-defaults.conf (read before SparkContext creation); setting it via
# SparkMagic session_configs.conf is too late because the SparkSession is already
# built by the time user-supplied conf is applied.
export SPARK_CONF_DIR=${LIVY_HOME:-/opt/livy}/conf

# Livy log directory — must be writable by the Livy process (created in Dockerfile)
export LIVY_LOG_DIR=${LIVY_HOME:-/opt/livy}/logs

# JVM heap for the Livy server process itself (not the Spark driver).
# 512 m is enough for ~20 concurrent sessions; raise to 1-2 g in production.
export LIVY_SERVER_JAVA_OPTS="-Xms256m -Xmx512m"
