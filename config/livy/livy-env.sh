#!/usr/bin/env bash
# livy-env.sh — Environment variables for the Livy server process

export SPARK_HOME=/opt/spark
# JAVA_HOME is inherited from the apache/spark base image
export HADOOP_CONF_DIR=

# Livy log directory (must be writable)
export LIVY_LOG_DIR=${LIVY_HOME:-/opt/livy}/logs

# Increase Livy server JVM heap for many concurrent sessions
export LIVY_SERVER_JAVA_OPTS="-Xms256m -Xmx512m"
