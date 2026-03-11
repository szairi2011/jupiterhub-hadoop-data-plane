FROM apache/spark:3.5.3

USER root

# Install curl, wget, unzip for Livy download and startup
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget procps unzip \
    && rm -rf /var/lib/apt/lists/*

# Download Apache Livy 0.9.0-incubating
ENV LIVY_VERSION=0.9.0-incubating
ENV LIVY_HOME=/opt/livy
ENV SPARK_HOME=/opt/spark

RUN mkdir -p ${LIVY_HOME} && \
    wget -q "https://downloads.apache.org/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}_2.12-bin.zip" \
         -O /tmp/livy.zip && \
    unzip -q /tmp/livy.zip -d /tmp/livy-extracted && \
    mv /tmp/livy-extracted/apache-livy-${LIVY_VERSION}_2.12-bin/* ${LIVY_HOME}/ && \
    rm -rf /tmp/livy.zip /tmp/livy-extracted

# Create logs and upload dirs required by Livy
RUN mkdir -p ${LIVY_HOME}/logs && \
    chmod -R 777 ${LIVY_HOME}/logs

EXPOSE 8998

WORKDIR ${LIVY_HOME}

# "start" daemonizes Livy and exits (code 0) — the container dies immediately.
# Running without a subcommand keeps the Java process in the foreground.
CMD ["bin/livy-server"]
