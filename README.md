# Streaming Lakehouse: CDC from Kafka into Apache Iceberg

CDC events stream from Kafka through Spark Structured Streaming into an Apache Iceberg
table on HDFS, and ClickHouse queries that table in place. The pipeline is the apparatus.
The deliverables are three measured experiments:

| | Question | Harness | Output |
|---|---|---|---|
| E1 | How does file count affect planning and query cost, and what does compaction buy back? | `bench/e1_planning.py` | `results/e1/planning_vs_files.csv` |
| E2 | Where does merge-on-read stop paying off against copy-on-write? | `bench/e2_cow_mor.py` | `results/e2/*.csv` |
| E3 | Is delivery exactly-once when the driver is `kill -9`ed mid-batch? | `bench/e3_chaos.sh` + `src/verify.py` | `results/e3/*.csv` |

The write-up is in [`docs/REPORT.md`](docs/REPORT.md), and the figures are in `docs/figures/`.
Every design choice that wasn't obvious is logged in [`DECISIONS.md`](DECISIONS.md).

## Architecture

```
producer_kafka.py --> Kafka (KRaft) topic orders.cdc
      |                        |
      | expected_state_e3.json v
      |               ingest_kafka.py  (Structured Streaming, foreachBatch:
      |                        |        dedupe per order_id by ts_ms -> MERGE INTO)
      v                        v
  verify.py  <----  Iceberg table on HDFS  ---->  ClickHouse (icebergHDFS / Parquet)
                               |
                         maintain.py (compact, expire, orphan cleanup, rollback)
```

## Stack

| Component | Version | Runs on |
|---|---|---|
| Hadoop HDFS | 3.3.6 | Java 11, native |
| Spark (PySpark) | 3.5.0 | Java 17, `local[4]` |
| Iceberg | 1.10.0 (`iceberg-spark-runtime-3.5_2.12`) | inside Spark |
| Kafka | 3.8.0 (`apache/kafka`, KRaft) | Docker |
| ClickHouse | latest OSS | Docker |
| Hive Metastore | 3.1.3 (optional, HiveCatalog path) | Java 8 |
| Python | 3.11 | `.venv` |

The catalog is Iceberg `HadoopCatalog` on HDFS. SPEC section 6.2 allows this as the
fallback, and [`DECISIONS.md`](DECISIONS.md) explains why this repo uses it.
The HiveCatalog path still works through `scripts/start_metastore.sh` and
`E3_CATALOG=hive`.

## Layout

```
src/        producer.py, producer_kafka.py   CDC generators + ground truth
            ingest.py, ingest_kafka.py       streaming MERGE INTO (file / Kafka source)
            maintain.py                      Iceberg maintenance procedures
            verify.py, reset_e3.py           E3 verifier and per-trial reset
            spark_session.py                 shared session / catalog builder
            query_clickhouse.py              Phase 5 reader
bench/      e1_planning.py, e2_cow_mor.py, e3_chaos.sh, plots.py, run_all.sh
scripts/    start_hdfs.sh, start_metastore.sh, start_kafka_docker.sh,
            start_clickhouse.sh, run_e2.sh
conf/       spark-defaults.conf, hive-site.xml, docker-compose.yml
results/    CSVs (committed - they are the evidence)
docs/       REPORT.md, figures/, screenshots/
DECISIONS.md  every non-obvious choice and why
```

## Setup

```bash
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
env -u PYTHONPATH ./.venv/bin/pip install -r requirements.txt
```

PySpark 3.5 does not support Python 3.14. If a Homebrew `apache-spark` is installed, your
shell profile may export `PYTHONPATH` and `SPARK_HOME` pointing at it. That would load
Spark 4.x instead of the pinned 3.5.0, so every runner here unsets both variables.

## Running

```bash
bash scripts/start_hdfs.sh                   # 1. storage

bash bench/run_all.sh                        # 2. E1 (5M rows) + E2 (2M rows) + figures
                                             #    E1_ROWS / E2_ROWS / E2_ROUNDS override

bash scripts/start_kafka_docker.sh           # 3. E3
E3_CATALOG=hadoop bash bench/e3_chaos.sh                         # 20 trials
BROKEN_CONTROL=1 TRIALS=3 E3_CATALOG=hadoop bash bench/e3_chaos.sh  # control arm

docker stop kafka                            # 4. ClickHouse (never alongside E1/E2)
bash scripts/start_clickhouse.sh
./.venv/bin/python src/query_clickhouse.py --mode iceberg

./.venv/bin/python bench/plots.py            # re-render every figure from the CSVs
```

Keep ClickHouse stopped while E1 and E2 run. On a 16 GB machine it takes RAM from the
Spark driver, and the timings stop meaning anything.
