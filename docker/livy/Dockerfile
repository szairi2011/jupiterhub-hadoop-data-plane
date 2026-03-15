# Stage 1: pull Spark from the official image so we get the exact same JARs
#          as the version we want, without being tied to its OS.
FROM apache/spark:3.5.3 AS spark-source

# ---------------------------------------------------------------------------
# Stage 2: runtime — Ubuntu Jammy (22.04), same base as the nodemanager.
#
# Why multi-stage instead of FROM apache/spark directly?
#   apache/spark:3.5.3 is built on Ubuntu Focal (20.04) → Python 3.8.
#   eclipse-temurin:11-jre-jammy is Ubuntu Jammy (22.04) → Python 3.10.
#   PySpark enforces exact minor-version matching between driver (Livy) and
#   worker (NodeManager).  Using the same base OS on both sides makes the
#   Python versions identical without any PPA or manual patching.
#   We copy only /opt/spark from stage 1 — same JARs, newer OS.
# ---------------------------------------------------------------------------
FROM eclipse-temurin:11-jre-jammy

# Copy the complete Spark distribution from the official image.
# This is identical to what apache/spark:3.5.3 would use at runtime.
COPY --from=spark-source /opt/spark /opt/spark

# python3  → Python 3.10 (Jammy default, matches nodemanager exactly)
# procps   → `ps`, used by Livy health-check scripts
# unzip    → needed to unpack the Livy binary archive at build time
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget procps unzip \
        python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Livy 0.9 requires Scala 2.12 build (matches Spark 3.x default)
ENV LIVY_VERSION=0.9.0-incubating
ENV LIVY_HOME=/opt/livy
ENV SPARK_HOME=/opt/spark
# Make Spark + Hadoop client binaries available on PATH (needed by Livy's spark-submit call)
ENV PATH=$PATH:$SPARK_HOME/bin

RUN mkdir -p ${LIVY_HOME} && \
    wget -q "https://downloads.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" \
         -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /tmp/livy-extracted && \
    mv /tmp/livy-extracted/apache-livy-${LIVY_VERSION}_2.12-bin/* ${LIVY_HOME}/ && \
    rm -rf /tmp/livy.zip /tmp/livy-extracted

# Livy writes session logs here; must be world-writable (Livy runs as root in this image)
RUN mkdir -p ${LIVY_HOME}/logs && chmod -R 777 ${LIVY_HOME}/logs

EXPOSE 8998

WORKDIR ${LIVY_HOME}

# Run Livy in the FOREGROUND (no subcommand).
# `bin/livy-server start` daemonizes the process and exits immediately (code 0),
# which makes Docker think the container finished successfully and stops it.
CMD ["bin/livy-server"]
