# Hadoop 3.3.6 — single image, role selected by HADOOP_ROLE env var.
# Handles all four daemons: namenode, datanode, resourcemanager, nodemanager.
#
# Built on eclipse-temurin:11-jre (Hadoop 3.3.x recommended Java 11 baseline).
# Hadoop is NOT pre-installed — downloaded at build time so the image version
# is pinned here rather than in docker-compose.yml.
#
# Config files are NOT baked in; they are mounted from config/hadoop/ so you can
# tune cluster settings without rebuilding.

FROM eclipse-temurin:11-jre-jammy

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
# eclipse-temurin sets JAVA_HOME already; make it explicit for Hadoop scripts.
ENV JAVA_HOME=/opt/java/openjdk

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget procps \
        python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/local/bin/python \
    && rm -rf /var/lib/apt/lists/*

# Download + install Hadoop
RUN wget -q \
        "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
        -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz

# Suppress "groups: cannot find name for group ID" warning in YARN scripts
RUN groupadd -g 1000 hadoop && useradd -u 1000 -g hadoop hadoop || true

# Default data dirs used by HDFS (overridable via hdfs-site.xml)
RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data && chmod -R 755 /hadoop

# Silence the Hadoop native library warning (no native libs in this lean image)
ENV HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib/native"

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
