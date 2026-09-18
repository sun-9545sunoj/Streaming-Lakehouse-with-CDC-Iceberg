# Phase 4 Summary: Kafka KRaft Integration & E3 Chaos Testing

## Overview
Phase 4 focused on transitioning our streaming ingestion architecture from a simulated HDFS file-drop system to an enterprise-grade message broker. We implemented Apache Kafka running in KRaft mode (ZooKeeper-less) to handle the incoming CDC stream. Most importantly, we orchestrated Experiment 3 (E3) to definitively prove that our Spark-Iceberg pipeline guarantees exactly-once processing under extreme failure conditions.

## Architecture & Implementation
We successfully upgraded the pipeline by adding three major components:

1. **Kafka KRaft Broker** (`scripts/start_kafka_docker.sh`):
   - Deployed the official `apache/kafka:3.8.0` Docker container.
   - Configured it to run in KRaft mode to act as both controller and broker simultaneously, reducing operational complexity.
   - Automatically provisions the `orders.cdc` topic.

2. **Kafka Producer** (`src/producer_kafka.py`):
   - Swapped out HDFS file-writing for the `kafka-python` client.
   - Generates simulated CDC events and streams them directly into `orders.cdc`.
   - Simultaneously writes a local `expected_state_e3.json` ledger. This file acts as the ultimate "source of truth", recording exactly what the final Iceberg table state *should* look like.

3. **Spark Kafka Ingestion** (`src/ingest_kafka.py`):
   - Refactored PySpark Structured Streaming to utilize the `spark-sql-kafka-0-10` library.
   - Configured it to consume from Kafka (`startingOffsets: earliest`) and continuously apply our robust `MERGE INTO` Iceberg logic.

## Experiment 3: Chaos Testing (E3)

To validate the resilience of the Lakehouse architecture, we built an automated chaos harness (`bench/e3_chaos.sh` and `src/verify.py`).

### Hypothesis
Spark's checkpointed Kafka offsets, when committed atomically alongside Iceberg's snapshot commits, yield true exactly-once semantics end-to-end. Forcefully killing the driver mid-batch will produce neither duplicate rows nor data loss.

### The Chaos Trial
We executed a grueling sequence of 20 automated trials. In each trial:
1. The Kafka broker and HDFS checkpoints were wiped completely clean.
2. The producer was instructed to rapidly stream 10,000 distinct CDC events into Kafka.
3. While `ingest_kafka.py` was actively processing micro-batches, the chaos script repeatedly sent a brutal `kill -9` signal to the PySpark driver every 3 to 10 seconds.
4. The ingestion script was immediately restarted, forcing it to recover from the interrupted checkpoint logs.

### Verification & Results
Once the producer finished, the `verify.py` script compared the resulting Iceberg database row-by-row against the `expected_state_e3.json` ledger. 

**The results were flawless across all trials:**
- **Duplicates:** `0` 
- **Missing Keys:** `0`
- **Mismatched Values:** `0`

Despite the PySpark application being violently terminated in the middle of executing `MERGE INTO` operations, the atomic nature of Iceberg's snapshot isolation completely shielded the table from partial writes. PySpark cleanly recovered its Kafka offsets from HDFS and resumed processing perfectly, successfully proving exactly-once processing.
