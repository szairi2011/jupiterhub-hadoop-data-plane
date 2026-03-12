# Livy 0.9.0-incubating on top of the official Spark 3.5.3 image.
# Why build on apache/spark and not a plain JDK image?
#   Livy needs $SPARK_HOME/jars on its classpath to launch drivers.
#   Re-using the Spark image guarantees version alignment with the standalone cluster.
FROM apache/spark:3.5.3

USER root

# procps provides `ps`, used by Livy's health-check scripts
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget procps unzip \
    && rm -rf /var/lib/apt/lists/*

# Livy 0.9 requires Scala 2.12 build (matches Spark 3.x default)
ENV LIVY_VERSION=0.9.0-incubating
ENV LIVY_HOME=/opt/livy
# Must match the Spark image's install path
ENV SPARK_HOME=/opt/spark

RUN mkdir -p ${LIVY_HOME} && \
    wget -q "https://downloads.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" \
         -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /tmp/livy-extracted && \
    mv /tmp/livy-extracted/apache-livy-${LIVY_VERSION}_2.12-bin/* ${LIVY_HOME}/ && \
    rm -rf /tmp/livy.zip /tmp/livy-extracted

# apache/spark:3.5.3 ships only python3 (no "python" symlink).
# Livy's PythonInterpreter.scala calls `python` directly — without this symlink
# every pyspark session goes idle but all statements return "Fail to start interpreter".
RUN ln -s /usr/bin/python3 /usr/bin/python

# Livy writes session logs here; must be world-writable (Livy runs as spark user)
RUN mkdir -p ${LIVY_HOME}/logs && \
    chmod -R 777 ${LIVY_HOME}/logs

EXPOSE 8998

WORKDIR ${LIVY_HOME}

# Run Livy in the FOREGROUND (no subcommand).
# `bin/livy-server start` daemonizes the process and exits immediately (code 0),
# which makes Docker think the container finished successfully and stops it.
CMD ["bin/livy-server"]
