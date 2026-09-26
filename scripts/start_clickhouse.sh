#!/bin/bash
# ClickHouse server for Phase 5. Reads the Iceberg table on host HDFS through
# host.docker.internal; no data is copied into ClickHouse.
#
# Recent images refuse network logins for the passwordless default user unless
# CLICKHOUSE_SKIP_USER_SETUP=1. Ports are published on 127.0.0.1 only, so that
# passwordless user is reachable from this machine alone.
#
# Keep it stopped during E1/E2 (SPEC section 6.3).
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# The NameNode hands out the DataNode as 127.0.0.1, which inside a container
# is the container. conf/clickhouse/hdfs.xml makes the client dial the
# DataNode's registered hostname instead; map that name to the host here.
DATANODE_HOST="${DATANODE_HOST:-$(JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home \
    "$HOME/hadoop/hadoop-3.3.6/bin/hdfs" dfsadmin -report 2>/dev/null | awk '/^Hostname:/ {print $2; exit}')}"
DATANODE_HOST="${DATANODE_HOST:-$(hostname -s)}"

if docker ps -a --format '{{.Names}}' | grep -qx clickhouse-server; then
    docker rm -f clickhouse-server >/dev/null
fi

docker run -d --name clickhouse-server \
    -p 127.0.0.1:8123:8123 -p 127.0.0.1:9001:9000 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    --add-host host.docker.internal:host-gateway \
    --add-host "$DATANODE_HOST":host-gateway \
    -v "$PROJECT_DIR/conf/clickhouse/hdfs.xml":/etc/clickhouse-server/config.d/hdfs.xml:ro \
    --ulimit nofile=262144:262144 \
    clickhouse/clickhouse-server:latest >/dev/null

until curl -s localhost:8123/ping | grep -q Ok; do sleep 1; done
echo "ClickHouse ready on http://localhost:8123"
