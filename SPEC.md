# Streaming Lakehouse with CDC + Iceberg

**Course:** Big Data
**Student:** AP25110010062
**Machine:** macOS (Apple Silicon), 16 GB RAM, 10 cores
**Status:** specification — no code written yet

---

## 1. One-line summary

A near-real-time lakehouse that ingests change-data-capture events from Kafka into Apache
Iceberg tables via Spark Structured Streaming, catalogued in the Hive Metastore, with
automated table maintenance and multi-engine querying from Hive and ClickHouse — plus three
measured experiments on planning cost, copy-on-write vs merge-on-read, and exactly-once
recovery under induced failure.

---

## 2. The problem this project exists to solve

An e-commerce store keeps `orders` in an operational database. Rows change constantly:
`PLACED → PAID → SHIPPED → DELIVERED`, plus `CANCELLED` and `REFUNDED`. Analytics needs the
**current state** of every order and the **full history** of how it got there, without
querying the production database.

The classic Hadoop/Hive stack is **append-only**. HDFS files are immutable; Hive tables have
no row-level UPDATE or DELETE (outside the ACID/ORC special case, which is Hive-only and slow).
So when an order's status changes, the options were:

- rewrite the whole partition nightly → stale by up to 24 hours, and wasteful
- or never reflect updates → the table is simply wrong

Apache Iceberg fixes exactly this: row-level `MERGE INTO` over immutable files, with ACID
snapshot isolation and a metadata tree that makes it cheap. **This project is the proof, with
numbers.**

---

## 3. What makes this more than a tutorial

A CDC → Iceberg pipeline is the canonical lakehouse blog post. Building only the happy path
produces an average project on a hot topic. The differentiators below are the actual
deliverable; the pipeline is the apparatus that makes them possible.

| ID | Differentiator | Why it signals depth |
|----|----------------|----------------------|
| **E1** | Query planning cost vs data-file count, measured across 3 orders of magnitude, before and after compaction | Everyone says "small files are bad." Almost nobody plots the curve or names the inflection point. |
| **E2** | Copy-on-write vs merge-on-read crossover, measured empirically | Everyone quotes the tradeoff from the docs. Measuring where MOR read cost overtakes COW write cost is a genuine artifact. |
| **E3** | Exactly-once proven by 20 scripted `kill -9` trials mid-batch | Separates "ran a pipeline" from "understands a pipeline." This is the interview story. |

Stretch (only if Phase 5 finishes early — do **not** start these before E1–E3 are complete):

- **S1** — Manifest-level analysis: walk the snapshot → manifest-list → manifest → data-file
  tree by hand; construct one query where partition pruning works and one where it silently
  fails, and explain why.
- **S2** — Iceberg v3 features (deletion vectors, row lineage) benchmarked against v2
  positional deletes.

**Rule for the report:** every claim must be backed by a number produced by this repo, or be
explicitly labelled as cited from documentation. At least one documented *negative* result
(something that got worse, or a limitation found) is required — honest limits read as senior;
uniform praise reads as a summary of someone else's blog.

---

## 4. Architecture

```
  ┌──────────────┐   Debezium-shaped JSON
  │ producer.py  │──────────────────────────► Kafka topic: orders.cdc
  │ (ground      │                              (KRaft, 3 partitions)
  │  truth log)  │                                      │
  └──────────────┘                                      │
         │                                              ▼
         │ writes                            ┌────────────────────────┐
         │ ledger.jsonl                      │ ingest.py              │
         │ (for E3 verification)             │ Spark Structured       │
         │                                   │ Streaming              │
         │                                   │  foreachBatch →        │
         │                                   │  MERGE INTO            │
         │                                   └───────────┬────────────┘
         │                                               │
         │                                               ▼
         │                              ┌─────────────────────────────────┐
         │                              │  Apache Iceberg table           │
         │                              │  db.orders                      │
         │                              │  storage: HDFS /warehouse       │
         │                              │  catalog: Hive Metastore (9083) │
         │                              └───┬──────────┬──────────┬───────┘
         │                                  │          │          │
         ▼                     ┌────────────┘          │          └───────────┐
  ┌────────────┐               ▼                       ▼                      ▼
  │ verify.py  │      ┌────────────────┐    ┌──────────────────┐   ┌──────────────────┐
  │ (E3 diff)  │      │ maintain.py    │    │ bench.py         │   │ ClickHouse       │
  └────────────┘      │ compaction,    │    │ E1 + E2 harness  │   │ iceberg() fn,    │
                      │ expire, orphan │    │ → results/*.csv  │   │ read-only        │
                      └────────────────┘    └──────────────────┘   └──────────────────┘
```

**Key design point:** Kafka is a *source*, swappable in one line. Phase 1 uses a file source
so nothing downstream is blocked on Kafka setup. `MERGE`, compaction, time travel and the
ClickHouse layer are all identical either way.

```python
# Phase 1
spark.readStream.format("json").schema(CDC_SCHEMA).load("hdfs:///cdc/orders")
# Phase 4 — only this line changes
spark.readStream.format("kafka").option("subscribe", "orders.cdc")...
```

---

## 5. Stack and licensing

Everything is Apache-2.0 or equivalent. Total spend: **$0**. No cloud account, no card.

| Component | Version | License | Notes |
|-----------|---------|---------|-------|
| Hadoop / HDFS | 3.3.6 | Apache 2.0 | already installed at `~/hadoop/hadoop-3.3.6` |
| Hive Metastore | 3.1.3 | Apache 2.0 | already installed at `~/hive/apache-hive-3.1.3-bin` |
| Apache Spark | 3.5.x | Apache 2.0 | to install; **not** 4.x — Iceberg 3.5 runtime is the well-trodden path |
| Apache Iceberg | 1.10.x | Apache 2.0 | `iceberg-spark-runtime-3.5_2.12`; **pin the exact version you resolve** |
| Apache Kafka | 4.x (KRaft) | Apache 2.0 | no ZooKeeper; single process |
| ClickHouse server | latest OSS | Apache 2.0 | Docker image; **not** ClickHouse Cloud (paid) |
| Python | 3.11 | PSF | `/opt/homebrew/opt/python@3.11` — see §6 warning |
| Docker Desktop | — | free personal tier | ClickHouse only |

**Deliberately excluded:** Databricks (proprietary), Snowflake, Confluent Cloud, AWS S3,
Redpanda (BSL, not true OSS). MinIO is unnecessary — HDFS is the object store.

---

## 6. Environment facts on this machine

These are verified, not assumed. Getting them wrong is the top cause of lost days.

### 6.1 Java version matrix

Five JDKs are installed. Each service needs a specific one. They are separate processes
communicating over sockets, so mixing is fine — but each launcher must set its own `JAVA_HOME`.

| Service | Required JDK | Path |
|---------|--------------|------|
| HDFS daemons | 11 | `/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home` |
| Hive Metastore + Hive CLI | 8 | `~/java/zulu8.96.0.205-ca-jdk8.0.504-macosx_aarch64/Contents/Home` |
| Spark 3.5 | 17 | `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` |
| Kafka 4.x | 17 | same as Spark |

Hive 3.1.3 requires Java 8 because its CLI casts the app classloader to `URLClassLoader`,
which only works on 8. This is already handled by `~/hive/hive-env.sh`; the new launchers
follow the same pattern.

### 6.2 Metastore must run as a Thrift service

Today the Hive CLI opens the Derby metastore **embedded**. Iceberg's `HiveCatalog` cannot do
that from Spark, and **Derby is single-writer** — two embedded clients corrupt or lock each
other.

Required change: run one standalone metastore process that owns the Derby lock, and have both
Spark and the Hive CLI connect to it over Thrift.

```bash
# owns Derby; must be started from ~/hive (Derby dir is relative)
hive --service metastore -p 9083
```

Then set `hive.metastore.uris=thrift://localhost:9083` in **both** Spark config and
`hive-site.xml`. Phase 0 does not pass until `SHOW DATABASES` works from Spark and from the
Hive CLI *at the same time*.

**Fallback if this fights back:** switch to Iceberg `HadoopCatalog` on HDFS. Cost: Hive is
demoted from catalog to reader, so add Trino (Docker, Apache 2.0) as the third engine to keep
the tool count. Decide by end of Phase 0 — do not let this bleed into Phase 2.

### 6.3 Memory budget — 16 GB total

Do not run everything at once.

| Process | Budget |
|---------|--------|
| HDFS NameNode + DataNode | 2 GB |
| Hive Metastore | 1 GB |
| Kafka broker | 1.5 GB |
| Spark driver (`local[4]`) | 4 GB |
| ClickHouse (Docker) | 2 GB |
| macOS + everything else | ~4 GB |

Consequence: **ClickHouse stays stopped during E1/E2 benchmark runs.** A benchmark competing
for RAM produces numbers that mean nothing. Phase 5 (ClickHouse) and Phase 3 (benchmarks) never
run in the same session.

### 6.4 Python

System default is **3.14**, which PySpark 3.5 does not support. Create the venv explicitly:

```bash
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
```

`requirements.txt`: `pyspark==3.5.*`, `kafka-python`, `pandas`, `matplotlib`, `clickhouse-connect`.

### 6.5 Disk

40 GB free. Budget: keep the generated dataset under 5 GB, and run
`expire_snapshots` + `remove_orphan_files` between experiments. E1 deliberately creates
thousands of small files — clean up after each sweep or the disk fills.

---

## 7. Data model

### 7.1 CDC event (what the producer emits)

Debezium-shaped envelope, so a real Debezium connector could be dropped in later without
changing the consumer.

```json
{
  "op": "c",
  "ts_ms": 1772000000123,
  "before": null,
  "after": {
    "order_id":    "ORD-000123",
    "customer_id": "CUST-0042",
    "status":      "PLACED",
    "amount":      249.50,
    "currency":    "INR",
    "region":      "south",
    "updated_at":  "2026-09-01T12:04:11Z"
  }
}
```

`op`: `c` create · `u` update · `d` delete · `r` snapshot read.
For `u`, `before` carries the prior row. For `d`, `after` is null and `before` carries the row.

### 7.2 Iceberg target table

```sql
CREATE TABLE lakehouse.orders (
    order_id     STRING,
    customer_id  STRING,
    status       STRING,
    amount       DECIMAL(12,2),
    currency     STRING,
    region       STRING,
    updated_at   TIMESTAMP,
    _op          STRING,     -- last operation applied to this row
    _ingest_ts   TIMESTAMP   -- when the pipeline wrote it
)
USING iceberg
PARTITIONED BY (region, days(updated_at))
TBLPROPERTIES (
    'format-version'      = '2',
    'write.update.mode'   = 'copy-on-write',   -- E2 varies this
    'write.delete.mode'   = 'copy-on-write',
    'write.merge.mode'    = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);
```

`order_id` is the merge key. `DECIMAL` not `DOUBLE` — money never uses floating point.

### 7.3 Producer ground-truth ledger

The producer also appends every event to `data/ledger.jsonl` and, on exit, writes
`data/expected_state.json` — the final status of every `order_id` it ever touched. **E3's
verifier diffs the Iceberg table against this file.** Without an independent ground truth,
"exactly-once" is an unverifiable claim.

---

## 8. Components to build

| # | File | Purpose |
|---|------|---------|
| 1 | `src/producer.py` | Generate order lifecycle events → Kafka (or files in Phase 1). Configurable rate, update/delete ratio, key skew. Writes the ground-truth ledger. |
| 2 | `src/ingest.py` | Spark Structured Streaming: read source → dedupe within batch by `(order_id, max(ts_ms))` → `foreachBatch` → `MERGE INTO`. Checkpoint on HDFS. |
| 3 | `src/maintain.py` | `rewrite_data_files`, `rewrite_manifests`, `expire_snapshots`, `remove_orphan_files`. Each callable independently and timed. |
| 4 | `bench/e1_planning.py` | E1 sweep — see §9.1 |
| 5 | `bench/e2_cow_mor.py` | E2 sweep — see §9.2 |
| 6 | `bench/e3_chaos.sh` + `src/verify.py` | E3 kill-and-verify loop — see §9.3 |
| 7 | `bench/plots.py` | CSV → the figures in `docs/figures/` |
| 8 | `scripts/*.sh` | `start_hdfs.sh`, `start_metastore.sh`, `start_kafka.sh`, `spark_submit.sh`, `clickhouse.sh` — each pinning its own `JAVA_HOME` |
| 9 | `conf/` | `spark-defaults.conf`, `hive-site.xml`, ClickHouse `docker-compose.yml` |

**Critical correctness detail for `ingest.py`:** a Spark micro-batch can contain several events
for the same `order_id`. `MERGE INTO` throws on multiple source rows matching one target row.
Deduplicate to the latest `ts_ms` per key *inside* `foreachBatch` before merging. This is the
single most common bug in CDC pipelines and is worth a paragraph in the report.

---

## 9. Experiments

Every experiment writes a CSV to `results/` and a figure to `docs/figures/`. Every run records
its git commit, timestamp, and full config into the CSV so results are reproducible.

**Shared methodology rules:**
- 5 repetitions per data point; report **median and IQR**, never a single run
- discard the first repetition (JVM warm-up)
- clear the OS page cache between reads where feasible; otherwise state that caching is warm
  and keep it consistent across arms
- one variable changes at a time; everything else pinned
- ClickHouse stopped, no other user processes

### 9.1 E1 — Query planning cost vs file count

**Hypothesis:** query planning time and metadata size grow with data-file count, and the growth
is superlinear beyond some threshold; compaction returns them near baseline.

**Method:** hold total row count constant (~5 M rows). Vary the number of data files by
controlling how many separate commits produce them (small trigger interval, tiny batches):

`{10, 50, 100, 500, 1000, 2500, 5000}` files.

At each point, measure:

| Metric | How |
|--------|-----|
| planning time (ms) | Spark listener / `SparkListenerSQLExecutionStart` → first task launch; cross-check with Iceberg scan-planning metrics |
| end-to-end query wall time (ms) | 3 fixed queries: full scan aggregate, single-partition filter, point lookup by `order_id` |
| data-file count | `SELECT count(*) FROM lakehouse.orders.files` |
| metadata bytes | sum of manifest + manifest-list + metadata JSON sizes |
| task count | Spark UI / listener |

Then run `rewrite_data_files` and repeat all measurements.

**Output:** `results/e1/planning_vs_files.csv`, figure `e1_planning.png` (log-x, two series:
before / after compaction), and the identified inflection point with an explanation grounded in
the manifest structure.

### 9.2 E2 — Copy-on-write vs merge-on-read crossover

**Hypothesis:** COW pays on write and is cheap on read; MOR is the reverse. There exists a
number of update rounds, dependent on update ratio, where MOR's accumulated delete files make
reads more expensive than COW's rewrite cost — and a total-cost crossover where COW wins
overall.

**Method:** two identical tables, `orders_cow` and `orders_mor`, differing only in
`write.{update,delete,merge}.mode`. Same seed, same data, same 2 M starting rows.

Run 20 rounds of `MERGE INTO`. Each round updates *p* % of rows, for
p ∈ `{0.1, 1, 5, 20}` (four separate sweeps).

Measure per round, per table:

| Metric | How |
|--------|-----|
| merge wall time (ms) | timed around the statement |
| bytes written this round | delta of `sum(file_size_in_bytes)` from `.files` and `.delete_files` |
| full-scan read time (ms) | fixed aggregate query |
| point-lookup time (ms) | filter by one `order_id` |
| delete-file count (MOR only) | `SELECT count(*) FROM lakehouse.orders_mor.delete_files` |

**Output:** `results/e2/cow_vs_mor_p{0.1,1,5,20}.csv`, figures `e2_write_cost.png`,
`e2_read_cost.png`. Report the round index of the read crossover and of the total-cost
crossover for each *p*, and state the practical rule the data supports.

**Expect a negative result here.** At low update ratios MOR may never cross over within 20
rounds. Report that honestly — it is a finding, not a failure.

### 9.3 E3 — Exactly-once under induced failure

**Hypothesis:** Spark's checkpointed Kafka offsets committed together with Iceberg's atomic
snapshot commit yield exactly-once end-to-end. Killing the driver mid-batch produces neither
duplicates nor loss.

**Method:**

1. Producer emits a fixed, finite workload (e.g. 500 k events over ~200 k distinct
   `order_id`s), writing `ledger.jsonl` and `expected_state.json`.
2. `e3_chaos.sh` starts `ingest.py`, sleeps a random 3–25 s, sends `kill -9` to the driver,
   waits, restarts from the same checkpoint.
3. Repeat until the topic is fully consumed. Record how many kills occurred.
4. `verify.py` asserts, against `expected_state.json`:
   - **no duplicates** — `count(*) == count(distinct order_id)` in the Iceberg table
   - **no loss** — every non-deleted `order_id` in expected state is present
   - **correct final value** — `status`, `amount`, `updated_at` match the last event per key
   - **no partial commits** — Iceberg snapshot count equals the number of batches that
     actually completed; every snapshot is readable via time travel
5. Repeat the whole trial **20 times** with different kill timings and different random seeds.

Also record recovery time: seconds from restart to fully caught up.

**Output:** `results/e3/failure_trials.csv` — one row per trial: seed, kill count, kill points,
duplicates, missing keys, mismatched values, snapshots, recovery seconds. Plus the **exact
config that makes it work**, and — importantly — a deliberately broken variant (e.g. checkpoint
disabled, or merge key wrong) showing what the failure looks like when it *isn't* exactly-once.
The contrast is what proves you understand the mechanism rather than having inherited it from a
default.

---

## 10. Phases and exit criteria

Nothing moves to the next phase until its exit criterion is demonstrably met.

| Phase | Week | Work | Exit criterion |
|-------|------|------|----------------|
| **0** | 1 | Install Spark 3.5 + Iceberg runtime. Convert metastore to Thrift service. Launcher scripts with per-service `JAVA_HOME`. | `SHOW DATABASES` succeeds from Spark **and** Hive CLI concurrently. Decision recorded: HiveCatalog or HadoopCatalog fallback. |
| **1** | 1–2 | `producer.py` → JSON files on HDFS. `ingest.py` with file source + `MERGE INTO`. | Update an order's status in the producer; the Iceberg row changes. Row-level update on HDFS, demonstrated. |
| **2** | 2–3 | `maintain.py`. Time travel and rollback drill. Schema + partition evolution. | 400 files compacted to <10. A dropped partition restored via `rollback_to_snapshot` in under 5 s. |
| **3** | 3–4 | `bench/` harness. **E1 and E2 run to completion.** | Both CSVs populated, both figures rendered, crossover points identified in writing. |
| **4** | 4–5 | Kafka 4.x KRaft. Swap source. **E3 run to completion.** | 20 chaos trials, all pass. Plus one deliberately broken run showing the failure mode. |
| **5** | 5 | ClickHouse container reading the same Iceberg table via `iceberg()`. | Same table queried from Spark, Hive and ClickHouse — no copy, no export. |
| **6** | 6 | Report, figures, screenshots, demo rehearsal. | Full demo run end-to-end twice without touching anything unplanned. |

Buffer: Phase 3 is the highest-value and highest-variance phase. If time is lost, cut Phase 5
to a single ClickHouse query rather than shrinking E1/E2. **Never cut the experiments — they
are the project.**

---

## 11. Live demo script (~10 minutes)

1. Start the producer. Events flow.
2. `SELECT status, count(*) FROM lakehouse.orders GROUP BY status` — run twice, numbers move.
   Table is live.
3. Force one order to `CANCELLED` in the producer. Query that `order_id`. Value changed.
   **Row-level update on immutable HDFS files** — the money shot.
4. `SELECT * FROM lakehouse.orders.files` — hundreds of tiny files. Show `.history` and
   `.snapshots`.
5. Run compaction. File count collapses. Rerun the benchmark query and show the speedup —
   with the E1 figure on screen behind it.
6. Delete a partition "by accident." `rollback_to_snapshot()`. Data back in seconds.
7. Chaos demo: `kill -9` the streaming job live, restart, run `verify.py`, show zero
   duplicates and zero loss.
8. Open ClickHouse. Same table, no export, no copy.
9. Close on the E2 crossover chart: "here is where merge-on-read stops paying off, measured on
   my own cluster."

Rehearse twice. Have the recorded screenshots as a fallback if the live cluster misbehaves.

---

## 12. Deliverables

- **Code repo** — 9 components in §8, all runnable from `scripts/`
- **`results/`** — E1, E2, E3 CSVs with embedded run metadata
- **`docs/figures/`** — 4 figures minimum: E1 planning curve, E2 write cost, E2 read cost,
  E3 trial summary
- **Report** — outline in §14
- **Screenshot set** — one per demo step, each showing the student identity banner
- **`docs/DECISIONS.md`** — running log of every non-obvious choice and why, including the
  catalog decision from Phase 0

---

## 13. Risks and fallbacks

| Risk | Likelihood | Fallback |
|------|-----------|----------|
| Iceberg `HiveCatalog` + Derby metastore fights back | medium | `HadoopCatalog` on HDFS; add Trino as third engine. **Decide in Phase 0.** |
| Scala/Iceberg jar version mismatch | medium | Pin `iceberg-spark-runtime-3.5_2.12:<exact>` in `spark-defaults.conf`. Mismatched Scala version fails with obscure `NoSuchMethodError` — check this first on any classpath error. |
| ClickHouse Iceberg reader lags the spec | medium | Test in Phase 0 with a throwaway table, not Phase 5. If it can't read, substitute Trino. |
| 16 GB RAM exhaustion during benchmarks | high | Enforced §6.3 budget; ClickHouse stopped during E1/E2; `local[4]` not `local[*]`. |
| Disk fills during E1's small-file sweep | high | `expire_snapshots` + `remove_orphan_files` after every sweep point. |
| Python 3.14 breaks PySpark | certain if ignored | venv pinned to 3.11 (§6.4). |
| Benchmarks produce noisy, meaningless numbers | medium | 5 reps, median + IQR, warm-up discarded, single-variable discipline (§9). |

---

## 14. Report outline

1. **Problem** — why append-only storage fails for mutable operational data
2. **Background** — Iceberg's metadata tree: snapshot → manifest list → manifest → data file;
   what makes an atomic commit atomic
3. **System** — architecture, component responsibilities, the in-batch dedupe requirement
4. **Experiment 1** — planning cost vs file count; method, results, inflection point, why
5. **Experiment 2** — COW vs MOR; method, crossover results per update ratio, the practical
   rule the data supports
6. **Experiment 3** — exactly-once; method, 20-trial results, the mechanism that makes it work,
   and the broken variant that shows what it looks like when it doesn't
7. **Limitations and negative results** — required section, not optional
8. **Comparison to Hive ACID** — what Hive 3 can and cannot do here, from the existing Lab 6
   environment
9. **Conclusion**

---

## 15. Resume line this becomes

> Built a streaming lakehouse ingesting CDC events from Kafka into Apache Iceberg via Spark
> Structured Streaming, catalogued in Hive Metastore over HDFS and queried from ClickHouse.
> Measured query-planning cost across three orders of magnitude of file count, established the
> copy-on-write / merge-on-read crossover empirically, and proved exactly-once delivery through
> 20 scripted mid-batch failure-injection trials.

Every clause in that sentence is backed by a CSV in this repo. That is the point.
