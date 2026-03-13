# Hive Metastore Service (Phase 3)
# Runs only the HMS thrift server — not HiveServer2, not Tez.
# Built on the official apache/hive:4.0.0 image (JRE 17, Ubuntu focal).
#
# Why a Dockerfile instead of using the image directly?
#   apache/hive:4.0.0 does not ship the PostgreSQL JDBC driver.
#   We add it here so hive-site.xml can point at the postgres service.
FROM apache/hive:4.0.0

USER root

# apache/hive:4.0.0 base image does not include curl or wget.
# We add curl to download the PostgreSQL JDBC driver (not bundled with Hive).
# Version 42.7.3: latest stable as of early 2025, compatible with PostgreSQL 16.
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL \
        "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
        -o /opt/hive/lib/postgresql-42.7.3.jar && \
    chown hive:hive /opt/hive/lib/postgresql-42.7.3.jar

USER hive

# SERVICE_NAME and DB_DRIVER are set in docker-compose.yml, not here,
# so the same image can be reused for other Hive services if needed.
