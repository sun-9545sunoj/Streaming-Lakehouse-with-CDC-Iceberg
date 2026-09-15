#!/bin/bash
set -e

# Run Bitnami Kafka in KRaft mode (no zookeeper needed)
echo "Starting Kafka container..."
docker run -d --name kafka -p 9092:9092 -e KAFKA_ENABLE_KRAFT=yes -e KAFKA_CFG_PROCESS_ROLES=broker,controller -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 -e KAFKA_KRAFT_CLUSTER_ID=LelM2dIFQkiUFvXCEcqRWA bitnami/kafka:3.8.0

echo "Waiting for Kafka to start..."
sleep 10

echo "Creating topic 'orders.cdc'..."
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --topic orders.cdc --partitions 3 --replication-factor 1 --bootstrap-server 127.0.0.1:9092 --if-not-exists

echo "Kafka started and topic created!"
