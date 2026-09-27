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
2. The producer streamed a seeded workload of 10,000 CDC events (creates, 40 % updates, 5 % deletes) into Kafka at 100 events/s.
3. Each time `ingest_kafka.py` committed its first micro-batch, the harness sent `kill -9` to the driver at a random point 0-4 s later, inside the next 5 s trigger window.
4. The ingestion script was immediately restarted, forcing it to recover from the interrupted checkpoint logs.

### Verification & Results

> **Revised 2026-09-26.** An earlier version of this section reported "flawless" results
> across 20 trials, but no results file existed in the repo, and the harness at that
> time could not have produced them. Its drain step called `timeout`, which macOS lacks,
> and its kills mostly landed before Spark had started. The harness was fixed (see
> `DECISIONS.md`) and rerun. The table below is `results/e3/failure_trials.csv`.

Each of the 20 trials streamed 10,000 seeded events. The driver was killed with `kill -9`
11-14 times per trial, **250 kills in total**. Every kill landed after the incarnation's
first committed batch, at a random point inside the next trigger window.

| Metric (all 20 trials) | Result |
|---|---|
| Trials passed | **20 / 20** |
| Duplicates | 0 |
| Missing keys | 0 |
| Mismatched values (status, amount, updated_at) | 0 |
| Deleted keys that came back | 0 |
| Surviving orders checked per trial | 5,525 - 5,696 |
| Final drain after the last kill | 2 - 9 s |

**Mechanism.** Structured Streaming writes each batch's offset range to the checkpoint
before running it, and marks the batch done only after `foreachBatch` returns. A kill
after Iceberg's commit but before that marker replays the batch. The replay is harmless
because the sink is idempotent: `MERGE INTO` keyed on `order_id`, keeping the latest event
per key, produces the same table when the same batch is applied twice. Iceberg's atomic
commit guarantees that a kill during the MERGE leaves no partial result. Exactly-once
here means at-least-once replay plus an idempotent, atomic sink. The control runs, which
drop the checkpoint, are analysed in `docs/REPORT.md` section 6.
