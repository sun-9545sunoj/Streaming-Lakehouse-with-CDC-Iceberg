#!/bin/bash
# Single-node Kafka in KRaft mode (no ZooKeeper), topic orders.cdc with 3 partitions.
#
# Uses the official apache/kafka image, whose CLI lives at /opt/kafka/bin - the
# path bench/e3_chaos.sh calls. The earlier version ran bitnami/kafka, whose CLI
# is at /opt/bitnami/kafka/bin, so every E3 topic reset failed.
set -e

IMAGE="apache/kafka:3.8.0"

if docker ps -a --format '{{.Names}}' | grep -qx kafka; then
    echo "Container 'kafka' exists, starting it..."
    docker start kafka >/dev/null
else
    echo "Starting Kafka container ($IMAGE)..."
    # The image defaults to a combined broker+controller KRaft node advertising
    # PLAINTEXT://localhost:9092, which is what the host-side Spark job needs.
    docker run -d --name kafka -p 9092:9092 "$IMAGE" >/dev/null
fi

echo "Waiting for the broker..."
until docker exec kafka /opt/kafka/bin/kafka-topics.sh --list \
        --bootstrap-server localhost:9092 >/dev/null 2>&1; do
    sleep 2
done

docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders.cdc \
    --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

echo "Kafka ready on localhost:9092, topic orders.cdc"
