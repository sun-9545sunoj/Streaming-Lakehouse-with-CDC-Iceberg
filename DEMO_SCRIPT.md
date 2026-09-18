# Live Demo Script: Streaming Lakehouse with CDC & Iceberg

This document provides a step-by-step narrative and command script to flawlessly present your project. 

---

## 1. Introduction (The Problem & Architecture)
**What to say:** 
> "Welcome to my Streaming Lakehouse presentation. Traditional data warehouses are rigid and expensive, while data lakes are swampy and lack ACID transactions. My project implements a modern **Data Lakehouse** architecture using Apache Iceberg, Apache Kafka, and PySpark to stream Change Data Capture (CDC) events with exact transactional guarantees while keeping storage and compute entirely decoupled."

## 2. Environment Overview
**What to say:** 
> "To prove this runs locally, I have containerized the infrastructure using Docker."
**Action:** Run this command to show your running services.
```bash
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```
*Point out the Hadoop/HDFS container, the Kafka (KRaft) broker, and the ClickHouse analytics server.*

## 3. Streaming Ingestion in Action (Phase 1 & 4)
**What to say:** 
> "Let's start the CDC data pipeline. On the left, my Kafka producer simulates an upstream operational database emitting Insert, Update, and Delete events. On the right, my PySpark structured streaming job consumes these events and dynamically merges them into an Apache Iceberg table."
**Action:** 
Open two terminals side-by-side.
*Terminal 1 (Producer):*
```bash
export PYTHONUNBUFFERED=1
./.venv/bin/python src/producer_kafka.py --rate 10 --interval 2.0
```
*Terminal 2 (Spark Ingest):*
```bash
export PYTHONUNBUFFERED=1 && export HADOOP_USER_NAME=hadoop
./.venv/bin/python src/ingest_kafka.py
```
*Let them run for a few seconds to show the batches merging, then stop the producer.*

## 4. Iceberg Metadata & Time Travel (Phase 2)
**What to say:** 
> "Unlike a traditional Data Lake, Apache Iceberg tracks every single transaction in a metadata tree. This gives us ACID compliance and the ability to Time Travel to previous states."
**Action:** 
List the HDFS metadata directory to prove the JSON/Avro snapshot files exist.
```bash
docker exec hadoop hdfs dfs -ls /user/hive/warehouse/orders_kafka/metadata
```

## 5. The Small File Problem & Compaction (Phase 3)
**What to say:** 
> "Streaming creates thousands of tiny files over time, which crashes query performance—this is known as the 'Small File Problem'."
**Action:** 
Open the `docs/figures/e1_planning.png` chart on your screen.
> "I built an automated benchmarking harness. As you can see on this graph, as file counts scale to 5,000, query planning time spikes massively. However, after executing Iceberg's Data Compaction algorithm (the blue line), query times instantly flatline back to optimal speeds."

## 6. Chaos Testing & Exactly-Once Semantics (Phase 4)
**What to say:** 
> "What happens if our Spark server crashes mid-transaction? I built an automated chaos testing harness to constantly terminate the PySpark driver (`kill -9`) while streaming thousands of records."
**Action:** 
Open `results/e3_results.csv` or explain the results.
> "By cross-referencing our final Iceberg table against our ground-truth ledger, we proved that Iceberg's atomic snapshot commits combined with Kafka's offset tracking resulted in 0 duplicates and 0 missing rows across 20 destructive trials."

## 7. Decoupled Compute via ClickHouse (Phase 5)
**What to say:** 
> "The ultimate promise of the Lakehouse is decoupling storage from compute. To prove this, I spun up an entirely different database—ClickHouse. Instead of copying or migrating the data into ClickHouse, I am going to have ClickHouse execute a high-speed aggregation query directly against the raw Parquet data files that Spark just wrote to HDFS."
**Action:** 
Run the ClickHouse query.
```bash
./.venv/bin/python src/query_clickhouse.py
```
> "As you can see, ClickHouse instantly returned our aggregated sales metrics using the same open data, proving true multi-engine interoperability without data lock-in."

---
**End of Demo.**
