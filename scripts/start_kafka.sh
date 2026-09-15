#!/bin/bash
set -e

KAFKA_VERSION="3.8.0"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

# 1. Download Kafka if not exists
if [ ! -d "$KAFKA_DIR" ]; then
    echo "Downloading Kafka ${KAFKA_VERSION}..."
    curl -O https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_DIR}.tgz
    tar -xzf ${KAFKA_DIR}.tgz
    rm ${KAFKA_DIR}.tgz
fi

cd $KAFKA_DIR

# 2. Format storage for KRaft (if not already formatted)
if [ ! -d "/tmp/kraft-combined-logs" ]; then
    echo "Formatting KRaft storage..."
    KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
fi

# 3. Start Kafka broker
echo "Starting Kafka (KRaft mode)..."
bin/kafka-server-start.sh config/kraft/server.properties > kafka.log 2>&1 &
KAFKA_PID=$!
echo "Kafka started with PID $KAFKA_PID. Logs at ${KAFKA_DIR}/kafka.log"

# Wait a moment for it to start
sleep 5

# 4. Create topic orders.cdc
echo "Creating topic 'orders.cdc'..."
bin/kafka-topics.sh --create --topic orders.cdc --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 --if-not-exists

echo "Kafka setup complete."
