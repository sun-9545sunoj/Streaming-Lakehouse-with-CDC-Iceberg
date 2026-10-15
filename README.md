# Streaming Lakehouse: CDC from Kafka into Apache Iceberg

**Team:** Vivek Gangavarapu (2023BCS0175), Ragiri Sunoj (2023BCS0178), Lella Tejasri (2023BCS0166)

CDC events stream from Kafka through Spark Structured Streaming into an Apache Iceberg
table on HDFS, and Spark, ClickHouse and Hive all query that table in place. The pipeline
is the apparatus. The deliverables are three measured experiments:

| | Question | Harness | Output |
|---|---|---|---|
| E1 | How does file count affect planning and query cost, and what does compaction buy back? | `bench/e1_planning.py` | `results/e1/planning_vs_files.csv` |
| E2 | Where does merge-on-read stop paying off against copy-on-write? | `bench/e2_cow_mor.py` | `results/e2/*.csv` |
| E3 | Is delivery exactly-once when the driver is `kill -9`ed mid-batch? | `bench/e3_chaos.sh` + `src/verify.py` | `results/e3/*.csv` |

Every design choice that wasn't obvious is logged in [`DECISIONS.md`](DECISIONS.md).
The figures are in `docs/figures/`, the demo screenshots in `docs/screenshots/`, and the
demo walkthrough in [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md).

Contents:

1. [Architecture](#1-architecture)
2. [Repository layout](#2-repository-layout)
3. [Prerequisites](#3-prerequisites)
4. [One-time setup](#4-one-time-setup)
5. [Reproduce the demo (about 10 minutes)](#5-reproduce-the-demo-about-10-minutes)
6. [Reproduce the experiments](#6-reproduce-the-experiments)
7. [Expected results](#7-expected-results)
8. [Stopping and cleaning up](#8-stopping-and-cleaning-up)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Architecture

```
producer_kafka.py --> Kafka (KRaft) topic orders.cdc
      |                        |
      | expected_state_e3.json v
      |               ingest_kafka.py  (Structured Streaming, foreachBatch:
      |                        |        dedupe per order_id by ts_ms -> MERGE INTO)
      v                        v
  verify.py  <----  Iceberg table on HDFS  ---->  ClickHouse (icebergHDFS)
                               |           ---->  Hive CLI (iceberg-hive-runtime)
                         maintain.py (compact, expire, orphan cleanup, rollback)
```

1. `src/producer_kafka.py` generates Debezium-shaped CDC events (`op` = `c`/`u`/`d`) for
   an orders table and writes them to Kafka topic `orders.cdc` (3 partitions). It also
   writes the expected final table state to `data/expected_state_e3.json`.
2. `src/ingest_kafka.py` reads the topic with Spark Structured Streaming. In each
   micro-batch it keeps the latest event per `order_id` (by `ts_ms`) and runs one
   `MERGE INTO` against the Iceberg table. The Kafka offsets live in a checkpoint on HDFS.
3. The table is an Iceberg table in a `HadoopCatalog` on HDFS
   (`hdfs://localhost:9000/warehouse/e3/e3/orders_kafka`).
4. `src/maintain.py` runs Iceberg's maintenance procedures (`rewrite_data_files`,
   `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`, `rollback_to_snapshot`).
5. `src/verify.py` diffs the table against the producer's expected state and reports
   duplicates, missing rows, mismatches and resurrected deletes.

## 2. Repository layout

```
src/        producer.py, producer_kafka.py   CDC generators + ground truth
            ingest.py, ingest_kafka.py       streaming MERGE INTO (file / Kafka source)
            maintain.py                      Iceberg maintenance procedures
            verify.py, reset_e3.py           E3 verifier and per-trial reset
            spark_session.py                 shared session / catalog builder
            demo_query.py, demo_cancel.py    helpers used by the demo
            query_clickhouse.py              ClickHouse reader
bench/      e1_planning.py, e2_cow_mor.py, e3_chaos.sh, plots.py, run_all.sh
scripts/    start_hdfs.sh, start_kafka_docker.sh, start_clickhouse.sh,
            start_metastore.sh, run_e2.sh, demo.sh, hive/*.hql
conf/       spark-defaults.conf, hive-site.xml, docker-compose.yml, clickhouse/hdfs.xml
results/    CSVs from the runs reported (committed - they are the evidence)
docs/       figures/, screenshots/, DEMO_SCRIPT.md
DECISIONS.md  every non-obvious choice and why
```

## 3. Prerequisites

Tested on macOS (Apple Silicon, 16 GB RAM) on 2026-10-15. The scripts assume Homebrew's
default paths under `/opt/homebrew`. On Linux or Intel Macs, set the `JAVA_HOME_*`
variables listed below to your own JDK paths.

| Component | Version | Used for | Install |
|---|---|---|---|
| Python | 3.11 | everything (PySpark 3.5 does not support 3.12+) | `brew install python@3.11` |
| JDK 17 | 17 | Spark 3.5.0 | `brew install openjdk@17` |
| JDK 11 | 11 | Hadoop daemons | `brew install openjdk@11` |
| Hadoop | 3.3.6 | HDFS (YARN is not used) | binary tarball from hadoop.apache.org, unpacked to `~/hadoop/hadoop-3.3.6` |
| Docker Desktop | any recent | Kafka and ClickHouse containers | docker.com |
| Kafka | 3.8.0 (`apache/kafka:3.8.0`, KRaft) | CDC topic | pulled by `scripts/start_kafka_docker.sh` |
| ClickHouse | 26.9.2.8 (`clickhouse/clickhouse-server:26.9.2.8`) | second query engine | pulled by `scripts/start_clickhouse.sh` |
| Spark + Iceberg | Spark 3.5.0, Iceberg 1.10.0 | ingest, maintenance, queries | `pip` (PySpark) + Maven jars downloaded on first run |
| Hive (optional) | 3.1.3 on JDK 8 (Zulu 8) | third query engine, demo step 8 only | see section 4.4 |

Other requirements:

- Internet access on the first Spark run: Spark downloads
  `iceberg-spark-runtime-3.5_2.12:1.10.0` and `spark-sql-kafka-0-10_2.12:3.5.0` from Maven
  Central into `~/.ivy2`.
- About 20 GB of free disk for the full-size E1 run. The demo needs well under 1 GB.
- Free ports: 9000, 9866, 9870 (HDFS), 9092 (Kafka), 8123 (ClickHouse).

Overridable paths (defaults in brackets):

| Variable | Used by | Default |
|---|---|---|
| `JAVA_HOME_17` | Spark scripts | `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` |
| `JAVA_HOME_11` | `scripts/start_hdfs.sh` | `/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home` |
| `HADOOP_HOME` | `scripts/start_hdfs.sh` | `~/hadoop/hadoop-3.3.6` |

## 4. One-time setup

### 4.1 Clone and create the Python environment

```bash
git clone <repo-url> streaming-lakehouse-cdc-iceberg
cd streaming-lakehouse-cdc-iceberg

/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
env -u PYTHONPATH ./.venv/bin/pip install -r requirements.txt
```

Check that the venv sees the pinned PySpark:

```bash
env -u PYTHONPATH -u SPARK_HOME ./.venv/bin/python -c "import pyspark; print(pyspark.__version__)"
# expected: 3.5.0
```

If this prints `4.x`, a Homebrew `apache-spark` is exporting `PYTHONPATH`/`SPARK_HOME`
from your shell profile. Every script in this repo unsets both, so the scripts are fine;
only commands you type by hand need the `env -u PYTHONPATH -u SPARK_HOME` prefix.

### 4.2 Configure single-node HDFS

Edit the files in `~/hadoop/hadoop-3.3.6/etc/hadoop/`:

`hadoop-env.sh`:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```

`core-site.xml`:

```xml
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>/Users/YOU/hadoopdata/tmp</value></property>
</configuration>
```

`hdfs-site.xml`:

```xml
<configuration>
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///Users/YOU/hadoopdata/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///Users/YOU/hadoopdata/hdfs/datanode</value></property>
</configuration>
```

`start-dfs.sh` logs in over SSH, so enable passwordless SSH to localhost (on macOS, first
turn on System Settings > General > Sharing > Remote Login):

```bash
ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519      # skip if you already have a key
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
ssh localhost true                                    # must not ask for a password
```

Format the NameNode once:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home \
  ~/hadoop/hadoop-3.3.6/bin/hdfs namenode -format
```

### 4.3 Pull the container images (optional, saves time later)

```bash
docker pull apache/kafka:3.8.0
docker pull clickhouse/clickhouse-server:26.9.2.8
```

### 4.4 Hive 3.1.3 (optional: only for the third engine in demo step 8)

Skip this if you do not need Hive. Steps 0-7 and 9 of the demo, and all three
experiments, run without it. Step 8 will still show Spark and ClickHouse, and print a
Hive error at the end.

The Hive 3.1.3 CLI only runs on Java 8. The demo expects this layout:

```
~/java/zulu8.96.0.205-ca-jdk8.0.504-macosx_aarch64/   Zulu JDK 8 (azul.com)
~/hive/apache-hive-3.1.3-bin/                         Hive 3.1.3 binary tarball
~/hive/hadoop-conf-java8/                             copy of ~/hadoop/hadoop-3.3.6/etc/hadoop
                                                      with JAVA_HOME in hadoop-env.sh set to Zulu 8
~/hive/auxlib/iceberg-hive-runtime-1.6.1.jar          from Maven Central (link below)
~/hive/hive-env.sh                                    environment script (below)
```

Download the Iceberg runtime for Hive. 1.6.1 is the newest release built for Java 8:

```bash
mkdir -p ~/hive/auxlib
curl -L -o ~/hive/auxlib/iceberg-hive-runtime-1.6.1.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.6.1/iceberg-hive-runtime-1.6.1.jar
```

`~/hive/hive-env.sh`:

```bash
export JAVA_HOME=$HOME/java/zulu8.96.0.205-ca-jdk8.0.504-macosx_aarch64/Contents/Home
export HADOOP_HOME=$HOME/hadoop/hadoop-3.3.6
export HADOOP_CONF_DIR=$HOME/hive/hadoop-conf-java8
export HIVE_HOME=$HOME/hive/apache-hive-3.1.3-bin
export HIVE_CONF_DIR=$HIVE_HOME/conf
export PATH=$JAVA_HOME/bin:$HIVE_HOME/bin:$HADOOP_HOME/bin:$PATH
export HADOOP_CLIENT_OPTS="-Xmx4g $HADOOP_CLIENT_OPTS"
cd $HOME/hive
```

Initialise the embedded Derby metastore once:

```bash
source ~/hive/hive-env.sh && schematool -dbType derby -initSchema
```

Hive 3.1.3 ships Guava 19, which clashes with Hadoop 3.3.6. If `hive` fails with a
`NoSuchMethodError` in `com.google.common`, replace `$HIVE_HOME/lib/guava-19.0.jar` with
`$HADOOP_HOME/share/hadoop/common/lib/guava-27.0-jre.jar`.

## 5. Reproduce the demo (about 10 minutes)

`scripts/demo.sh` runs the demo one step per call. Each step clears the screen and prints
a banner with the team names and the date, so each step fits one screenshot. Run the
steps in order: steps 2-6 work on the live table that step 1 creates.

Start Docker Desktop first, then:

```bash
bash scripts/demo.sh 0
```

| Step | Command | What it does | What you should see | Time |
|---|---|---|---|---|
| 0 | `bash scripts/demo.sh 0` | starts HDFS and Kafka, recreates topic `orders.cdc`, drops the table and checkpoint | `HDFS ready at hdfs://localhost:9000`, `Kafka ready on localhost:9092`, `Created topic orders.cdc.`, `Dropped lh.e3.orders_kafka` | ~1 min (longer on first run: image pulls) |
| 1 | `bash scripts/demo.sh 1` | starts the producer (50 events/s) and the streaming ingest job in the background | `Merged batch 0.` and producer lines `Sent N/100000 events to Kafka.` | ~30 s (first run also downloads Spark jars) |
| 2 | `bash scripts/demo.sh 2` | runs the same `GROUP BY status` query twice, 10 s apart | two tables; the counts differ between them (the total grows, and orders move between statuses) | ~20 s |
| 3 | `bash scripts/demo.sh 3` | cancels one order through Kafka and polls the table | `BEFORE` with some status, `SENT op=u ... -> CANCELLED`, `AFTER` with `CANCELLED`, `Row-level update visible in Iceberg after N s` (about 5-10 s) | ~30 s |
| 4 | `bash scripts/demo.sh 4` | queries Iceberg metadata tables `.files`, `.all_data_files`, `.history`, `.snapshots` | a few live data files, many more held by all snapshots; one snapshot per micro-batch, the first `append`, the rest `overwrite` | ~15 s |
| 5 | `bash scripts/demo.sh 5` | stops the stream, runs compaction + manifest rewrite + snapshot expiry | `snapshots_before` > 1, `snapshots_after` = 1, `all_snapshot_files_after` = `live_files_after`; the per-region totals are identical before and after. Opens `docs/figures/e1_planning.png` | ~30 s |
| 6 | `bash scripts/demo.sh 6` | records the current snapshot, deletes `region = 'north'`, rolls back | north disappears from the second table and is back, with the same count, after `rollback_to_snapshot` | ~30 s |
| 7 | `bash scripts/demo.sh 7` | resets the pipeline, streams 3000 events, `kill -9`s the ingest job mid-batch, restarts it from the same checkpoint, drains, verifies | `duplicates 0`, `missing 0`, `mismatched 0`, `resurrected 0`, `passed True` | ~1.5 min |
| 8 | `bash scripts/demo.sh 8` | starts ClickHouse, then runs the same aggregate on Spark, ClickHouse and Hive | three identical `region / total_orders / total_revenue` tables | ~30 s |
| 9 | `bash scripts/demo.sh 9` | prints `results/e2/crossover_summary.csv` | four rows (0.1, 1, 5, 20 %) and the E2 figures open | instant |

Then stop the background jobs:

```bash
bash scripts/demo.sh stop
```

Logs of the background producer and ingest job are in `$TMPDIR/lakehouse-demo/`.
`docs/screenshots/demo_transcript.txt` is the full text output of a reference run, to
compare against.

## 6. Reproduce the experiments

**These scripts overwrite (E1, E2) or append to (E3) the committed CSVs in `results/`
and the figures in `docs/figures/`.** To get the committed versions back afterwards:

```bash
git checkout -- results docs/figures
```

All three need HDFS (`bash scripts/start_hdfs.sh`). Stop ClickHouse while E1 and E2 run
(`docker stop clickhouse-server`). On a 16 GB machine it takes memory from the Spark
driver and the timings stop meaning anything.

### 6.1 Quick check (about 15 minutes in total)

Small sizes, to confirm the harnesses work on your machine. The numbers are too small to
compare with section 7.

```bash
bash scripts/start_hdfs.sh
E1_ROWS=100000 E2_ROWS=50000 E2_ROUNDS=3 bash bench/run_all.sh    # E1 + E2 + figures

bash scripts/start_kafka_docker.sh
TRIALS=1 EVENTS=2000 E3_CATALOG=hadoop bash bench/e3_chaos.sh     # one chaos trial, ~1 min
```

Expected: `run_all.sh` ends with `Done. CSVs in results/, figures in docs/figures/`, and
the E3 trial ends with `trial 1 PASSED`.

### 6.2 Full size (the numbers in section 7)

These take hours. Run them one at a time.

```bash
bash scripts/start_hdfs.sh

# E1 (5M rows, 7 file-count targets) and E2 (2M rows, 20 rounds, 4 update ratios),
# then all figures.
bash bench/run_all.sh

# E3: 20 trials of 10,000 events each, with random kill -9s of the driver.
bash scripts/start_kafka_docker.sh
rm -f results/e3/failure_trials.csv          # the harness appends; start clean
E3_CATALOG=hadoop bash bench/e3_chaos.sh

# E3 control arms: no checkpoint, so each restart starts from the given offset.
rm -f results/e3/failure_trials_broken_*.csv
BROKEN_CONTROL=1 TRIALS=3 STARTING_OFFSETS=earliest E3_CATALOG=hadoop bash bench/e3_chaos.sh
BROKEN_CONTROL=1 TRIALS=3 STARTING_OFFSETS=latest   E3_CATALOG=hadoop bash bench/e3_chaos.sh

# Re-render every figure from the CSVs (also covers E3).
env -u PYTHONPATH ./.venv/bin/python bench/plots.py
```

Each experiment can also run alone:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
env -u PYTHONPATH -u SPARK_HOME ./.venv/bin/python bench/e1_planning.py --rows 5000000 \
    --targets 10,50,100,500,1000,2500,5000 --reps 5
env -u PYTHONPATH -u SPARK_HOME ./.venv/bin/python bench/e2_cow_mor.py --rows 2000000 \
    --rounds 20 --pct 5          # one update ratio only
```

### 6.3 ClickHouse and Hive reads outside the demo

```bash
docker stop kafka                                   # free memory
bash scripts/start_clickhouse.sh
env -u PYTHONPATH ./.venv/bin/python src/query_clickhouse.py --mode iceberg

source ~/hive/hive-env.sh
HIVE_AUX_JARS_PATH=$HOME/hive/auxlib/iceberg-hive-runtime-1.6.1.jar \
  hive -f "$OLDPWD/scripts/hive/read_iceberg.hql"
```

Hive cannot read zstd Parquet on macOS. Before the Hive query, switch the table to gzip and
rewrite it once (demo step 8 does this for you); the exact statements are at the top of
`scripts/hive/read_iceberg.hql`.

## 7. Expected results

From the committed CSVs. Re-running at full size on similar hardware should give the
same shape; absolute timings depend on the machine.

**E1** (`results/e1/planning_vs_files.csv`, 5M rows):

| Actual data files | Iceberg planning (ms) | Full aggregate (ms) | Point lookup (ms) |
|---|---|---|---|
| 44 | 1.89 | 292 | 113 |
| 443 | 2.93 | 506 | 288 |
| 878 | 3.02 | 705 | 430 |
| 2213 | 4.18 | 1700 | 1110 |
| 4389 | 6.60 | 2528 | 1841 |
| 4 (any target, after compaction) | 1.3-1.8 | 546-590 | 177-196 |

**E2** (`results/e2/crossover_summary.csv`, 2M rows, 20 rounds, no compaction):

| Update ratio | MOR total write (s) | COW total write (s) | MOR bytes written | COW bytes written | Round where COW's total cost becomes lower |
|---|---|---|---|---|---|
| 0.1 % | 104.4 | 61.3 | 2.1 MB | 606 MB | 5 |
| 1 % | 184.3 | 64.2 | 12.8 MB | 606 MB | 1 |
| 5 % | 281.7 | 75.7 | 59.5 MB | 574 MB | 3 |
| 20 % | 191.1 | 83.5 | 223 MB | 567 MB | 6 |

**E3** (`results/e3/*.csv`):

| Arm | Trials | Kills | Expected rows | Duplicates | Missing | Mismatched | Resurrected | Passed |
|---|---|---|---|---|---|---|---|---|
| Checkpoint on | 20 | 250 | 112,323 | 0 | 0 | 0 | 0 | 20/20 |
| No checkpoint, replay from earliest | 3 | 33 | 16,866 | 0 | 0 | 0 | 0 | 3/3 |
| No checkpoint, resume at latest | 3 | 33 | 16,866 | 0 | 9,421 | 1,670 | 440 | 0/3 |

**Three engines** (`results/phase5/three_engines.txt`): Spark, ClickHouse and Hive return
identical per-region counts and sums on the same snapshot.

## 8. Stopping and cleaning up

```bash
bash scripts/demo.sh stop                       # producer + ingest job
docker stop kafka clickhouse-server
bash scripts/start_hdfs.sh stop
```

Delete experiment tables from HDFS to reclaim disk space (E1 drops its own tables unless
run with `--keep-tables`):

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home \
  ~/hadoop/hadoop-3.3.6/bin/hdfs dfs -du -s -h '/warehouse/*'
```

## 9. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `pyspark.__version__` prints 4.x instead of 3.5.0 | shell profile exports `PYTHONPATH`/`SPARK_HOME` for a Homebrew Spark | run Python by hand as `env -u PYTHONPATH -u SPARK_HOME ./.venv/bin/python ...` |
| `ingest_kafka.py exited before its first batch - is Kafka up?` | Docker or the `kafka` container is not running | start Docker Desktop, then `bash scripts/start_kafka_docker.sh` |
| `start-dfs.sh`: `localhost: Permission denied` | no passwordless SSH | section 4.2 |
| HDFS commands hang or report safe mode | NameNode still starting | `scripts/start_hdfs.sh` waits for safe mode to end; rerun it |
| `rewrite_manifests` fails with `Deleted manifest ... could not be found in the latest snapshot` | maintenance ran while the stream was committing | stop the stream first (`bash scripts/demo.sh stop`); demo steps 5 and 6 already do |
| Demo step 8: ClickHouse query hangs until `Read timed out` | seen once, during a host-wide stall in which even the DataNode's heartbeat to the NameNode timed out; did not reproduce in three reruns | press Ctrl-C and rerun `bash scripts/demo.sh 8` |
| ClickHouse: `Query failed` with no HDFS support | image without `icebergHDFS` | use the pinned image tag; as a last resort `--mode parquet` (it over-counts: it reads files the current snapshot no longer references) |
| Hive: `ClassCastException ... URLClassLoader` | Hive CLI started on Java 9+ | `source ~/hive/hive-env.sh` first (Java 8) |
| Hive: zstd codec error | Iceberg 1.10 writes zstd by default | switch the table to gzip (section 6.3) |
| E1/E2 timings much slower than section 7 | ClickHouse or other heavy processes running | `docker stop clickhouse-server` and rerun |
