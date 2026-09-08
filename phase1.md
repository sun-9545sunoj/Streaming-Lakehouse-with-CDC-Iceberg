# Phase 1 Summary: Streaming Ingestion into Iceberg

## Overview
The goal of Phase 1 was to establish the foundational streaming pipeline for our Streaming Lakehouse. This involved generating mock Change Data Capture (CDC) events and streaming them into an Apache Iceberg table stored on Hadoop Distributed File System (HDFS).

## Architecture & Implementation
1.  **Mock Producer (`src/producer.py`)**:
    *   Generates synthetic e-commerce orders with `c` (create), `u` (update), and `d` (delete) operations.
    *   Maintains the ground truth state in `data/expected_state.json`.
    *   Outputs events as JSON strings. To bypass Docker network routing issues between the Mac host and the containerized HDFS DataNode, it writes temporary files locally and pipes them directly into HDFS via `docker exec`.

2.  **Streaming Ingestion (`src/ingest.py`)**:
    *   Utilizes PySpark Structured Streaming to read new JSON files from the HDFS landing zone (`/cdc/orders/`).
    *   Uses a `foreachBatch` function to process micro-batches.
    *   **Deduplication**: Because multiple updates for the same `order_id` might arrive in a single micro-batch (which causes Iceberg `MERGE INTO` to fail), we implemented an in-memory deduplication step using PySpark Window functions (sorting by `ts_ms` descending to keep the latest event).
    *   **MERGE INTO**: Deduplicated events are registered as a `global_temp.batch_updates` view, which is then used in a SQL `MERGE INTO` statement to atomically apply inserts, updates, and deletes to the `default.orders` Iceberg table.

## Challenges & Solutions

### 1. Hadoop Docker Networking on macOS
*   **Problem**: HDFS DataNodes inside Docker advertise their internal container IP (e.g., `172.17.x.x`). When the PySpark client on the Mac host attempts to write or read data blocks, it fails to connect to these unreachable internal IPs. Additionally, NameNode RPC requests were throwing `EOFException`.
*   **Solution**: We reconfigured the Docker container by mapping all necessary Hadoop ports (`9000`, `9866`, `9864`, `9870`, `9083`) to the Mac host. We updated the HDFS `core-site.xml` so the NameNode binds to `0.0.0.0` instead of `localhost`, allowing external TCP connections. We also set `HADOOP_USER_NAME=hadoop` to bypass `AccessControlException` when PySpark runs as the local Mac user.

### 2. Iceberg Catalog & TempView Visibility
*   **Problem**: Iceberg's strict catalog integration hooks into Spark's SQL resolution. When we tried to use `CREATE TEMP VIEW` for our micro-batch data, the `MERGE INTO` query threw a `TABLE_OR_VIEW_NOT_FOUND` error because Iceberg's catalog intercepted the query and couldn't find the view.
*   **Solution**: We transitioned from `SparkCatalog` to `SparkSessionCatalog`, which correctly falls back to Spark's default catalog for non-Iceberg tables. We also used `createOrReplaceGlobalTempView("batch_updates")` to place the view in the isolated `global_temp` namespace, completely bypassing Iceberg's resolution rules and allowing the `MERGE` to succeed.

### 3. Dependency Management
*   **Problem**: Native macOS lacks Java installations by default, and newer versions of Python (3.12+) can break PySpark serialization.
*   **Solution**: We utilized Conda to create a deterministic environment (`.venv`) locked to Python 3.11 and OpenJDK 17. 

## End Result
The pipeline successfully streams data from HDFS into an Iceberg table, deduplicating records on the fly, and safely updating the table with row-level modifications using Iceberg's v2 format (`copy-on-write`). 
