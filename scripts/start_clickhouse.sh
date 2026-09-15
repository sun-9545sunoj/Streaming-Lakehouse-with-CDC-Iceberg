#!/bin/bash
set -e
echo "Starting ClickHouse server container..."

# We need ClickHouse to communicate with the Hadoop container at localhost:9000
# Since they are on the same machine, using --net=host on Linux works, but on Mac we should just rely on the exposed port and host.docker.internal
docker run -d --name clickhouse-server -p 8123:8123 -p 9001:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server:latest

echo "ClickHouse started! You can connect via HTTP on port 8123."
