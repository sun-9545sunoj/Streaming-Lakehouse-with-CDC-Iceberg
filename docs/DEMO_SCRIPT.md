# Demo Script and Work Division

**Project:** Streaming Lakehouse with CDC + Iceberg
**Team:** Vivek Gangavarapu (2023BCS0175), Ragiri Sunoj (2023BCS0178), Lella Tejasri (2023BCS0166)
**Total time:** about 15 minutes of demo plus Q&A, about 5 minutes per person

---

## Part 1: Work division

Each member owns one third of the system, one third of the experiments and one third of
the write-up. Each person presents the part they own.

| Area | Vivek Gangavarapu | Ragiri Sunoj | Lella Tejasri |
|---|---|---|---|
| **System component** | Ingest path: CDC producer, Kafka topic, Spark Structured Streaming job, `MERGE INTO` upsert logic | Reliability and serving: checkpointing, chaos harness, verifier, ClickHouse and Hive readers | Table management: Iceberg catalog on HDFS, compaction, snapshot expiry, time travel and rollback |
| **Main files** | `src/producer_kafka.py`, `src/ingest_kafka.py`, `src/demo_cancel.py` | `bench/e3_chaos.sh`, `src/verify.py`, `src/query_clickhouse.py`, `scripts/hive/` | `src/maintain.py`, `bench/e1_planning.py`, `conf/` |
| **Experiment** | E2: copy-on-write vs merge-on-read (`bench/e2_cow_mor.py`) | E3: exactly-once under `kill -9` (20 trials, 250 kills, control arms) | E1: planning and query cost vs file count |
| **Infrastructure** | Kafka (KRaft) setup, demo script `scripts/demo.sh` | ClickHouse container, Hive Metastore launchers | HDFS setup, Spark and Iceberg version pinning |
| **Report sections** | 1-3 (problem, architecture, pipeline), section 5 (E2), section 9 (conclusion) | Section 6 (E3), section 8 (Iceberg vs Hive ACID), Q&A prep | Section 4 (E1), section 7 (limitations and negative results), figures |
| **Demo steps presented** | Opening, steps 0-3, step 9, closing | Steps 7-8, limitations | Steps 4-6 |

**Shared by all three:** design decisions (`docs/DECISIONS.md`), debugging the bugs the
experiments found in our own harness, rehearsing the demo, reviewing each other's sections.

> NOTE: Edit this table if it does not match who actually built what. Each person should
> present only what they can explain in detail, because Q&A will follow the same split.

---

## Part 2: Before the demo starts

Run 15 minutes before presenting:

```bash
bash scripts/demo.sh 0      # starts HDFS and Kafka, fresh topic and table
```

Keep open in a second window: `docs/screenshots/demo_transcript.txt` and the
`docs/screenshots/step*.png` images. **If anything hangs live, say "we have the recorded
run from rehearsal" and show the matching screenshot. Do not debug on stage.**

Use a large terminal font. Every step prints a banner with all three names, so every
screenshot is self-labelled.

---

## Part 3: What to say

Words in quotes are what to say. Lines starting with **DO** are actions. Lines starting
with **POINT AT** tell you what to show on screen.

---

### VIVEK: Opening (1 min)

"Good morning. We are Vivek, Sunoj and Tejasri. Our project is a streaming lakehouse:
change events from an orders database flow through Kafka, Spark Structured Streaming
writes them into Apache Iceberg tables on HDFS, and Spark, ClickHouse and Hive can all
query the result."

"The problem it solves: an online store's orders keep changing status: placed, paid,
shipped, delivered, sometimes cancelled or refunded. Classic Hive on HDFS is append-only.
Files are immutable, so there is no cheap way to update one row. Either you rewrite the
whole partition every night and the data is a day old, or the table is simply wrong."

"Iceberg fixes this. It gives row-level MERGE on top of immutable files, using snapshots
and a metadata tree."

"Building that pipeline is a well-known tutorial, so it is not our contribution. Our
contribution is three experiments that test what people usually assume about this
stack. Two of those assumptions turned out to be wrong on our setup. I'll show the
pipeline first, then Tejasri covers table maintenance, then Sunoj shows failure recovery."

---

### VIVEK: Step 1, producer and ingest (1 min)

**DO:** `bash scripts/demo.sh 1`

"This starts two processes. The producer generates order events in the same JSON shape
Debezium uses for CDC: create, update and delete, about 50 events per second, into a
Kafka topic with 3 partitions. The second process is our Spark streaming job. Every 5
seconds it takes a micro-batch from Kafka and merges it into the Iceberg table."

**POINT AT:** "Merged batch 0" and the producer log lines.

"You can see the first batch has merged, and the producer count keeps going up."

---

### VIVEK: Step 2, the table is live (1 min)

**DO:** `bash scripts/demo.sh 2`

"Here we run the same query twice, ten seconds apart: count of orders by status."

**POINT AT:** the two tables. Read out one row, for example PLACED going up.

"The counts changed between the two queries. Orders are moving between statuses, so
these rows are being updated, not just appended. That's the thing plain Hive on HDFS
can't do."

---

### VIVEK: Step 3, one row-level update (1 min)

**DO:** `bash scripts/demo.sh 3`

"Now one order, end to end. The script picks an order that is not already CANCELLED, sends
one update event to Kafka saying it is CANCELLED, and then polls the Iceberg table."

**POINT AT:** BEFORE, SENT, AFTER, and the seconds on the last line.

"Before: [read out the status on the BEFORE line]. We send the cancel. After: CANCELLED, with a new timestamp. It became
visible in Iceberg within a few seconds, about one trigger interval. Inside each batch we
remove duplicate events per order and keep the latest, then run one MERGE INTO: update if
the order exists, insert if new, delete on a delete event."

"Now Tejasri will show what this does to the files underneath."

---

### TEJASRI: Step 4, snapshots and small files (1.5 min)

**DO:** `bash scripts/demo.sh 4`

"Every micro-batch is an Iceberg commit, and every commit creates a new snapshot. These
are Iceberg's metadata tables, which you query with plain SQL."

**POINT AT:** `live_data_files` vs `files_held_by_all_snapshots`.

"The current table has only a few live data files, but the old snapshots still hold many
more. Iceberg keeps them so we can time travel. A streaming job that commits every few
seconds keeps piling up files and snapshots, and that's the small-files problem."

**POINT AT:** the history and snapshots tables.

"Here is the history: one snapshot per batch, each pointing to its parent. The first one
is an append, the rest are overwrites from the MERGE."

---

### TEJASRI: Step 5, compaction and expiry, plus E1 (2 min)

**DO:** `bash scripts/demo.sh 5`

"First we pause the stream. We learned this the hard way: maintenance and a streaming
commit at the same time conflict, and `rewrite_manifests` failed with a deleted-manifest
error in rehearsal."

**POINT AT:** before counts, then the three procedure outputs, then after counts.

"We run three Iceberg procedures: rewrite data files, rewrite manifests and expire old
snapshots. Snapshots drop to one, the files held by old snapshots drop to just the live
ones, and the per-region totals are identical before and after. Maintenance changes the
layout, not the data."

**POINT AT:** the E1 figure that opens (`docs/figures/e1_planning.png`).

"This is our first experiment, E1. Everyone says small files make queries slow, and the
usual explanation is query planning. We measured it on 5 million rows, from 44 files up
to about 4,400 files."

"Finding one: planning is not the problem. Iceberg planning went from about 2 to 6.6
milliseconds for 100 times more files. It never jumped sharply."

"Finding two: execution is the problem. A full GROUP BY got 8.7 times slower and a point
lookup 16 times slower. The slowdown starts between 500 and 1,000 files, which is below
about 10,000 rows per file."

"Finding three, our negative result: compacting too far hurts. Four files became one
scan task, so the query ran on one core and was about 1.9 times slower than the 44-file
layout. Don't compact below one file per core."

---

### TEJASRI: Step 6, accidental delete and rollback (1.5 min)

**DO:** `bash scripts/demo.sh 6`

"Now a mistake someone could really make. We note the current snapshot ID, then delete
the entire north region."

**POINT AT:** the second table, where north is gone.

"North is gone. In a plain HDFS table, that data would be gone unless you had a backup."

**POINT AT:** the rollback output and the final table.

"We call `rollback_to_snapshot` with the ID we noted. North is back with the same row
count. Nothing was copied. Iceberg just moved the table's current pointer back to the
older snapshot, whose files were still there."

"Next, Sunoj will show the most important property: what happens when the job crashes."

---

### SUNOJ: Step 7, kill -9 and exactly-once, plus E3 (2.5 min)

**DO:** `bash scripts/demo.sh 7`

"This resets the table and starts a fresh workload of 3,000 events. While the streaming
job is in the middle of a batch, we send it `kill -9`. That's not a graceful shutdown:
the process dies immediately, possibly halfway through a MERGE."

**POINT AT:** `kill -9`, the restart line, then the verification block.

"We restart it from the same checkpoint, let it finish, and then our verifier compares
the table against the producer's ground truth: expected rows equal actual rows, zero
duplicates, zero missing, zero wrong values, zero deleted orders coming back. Passed."

"One kill on stage is a demo, not proof. So experiment E3 ran this 20 times, with 250
kills in total, 11 to 14 per trial at random moments. All 20 passed with zero errors."

"The part we're most proud of is the control runs, because they show which component
gives which guarantee. We restarted without a checkpoint. When the job resumed from the
latest offset, which is Spark's default for Kafka, every event produced while it was down
was skipped. We lost 50 to 62 percent of the orders, and up to 181 deleted orders came
back. When it replayed the whole topic from the beginning instead, there were still zero
duplicates."

"So exactly-once is really two guarantees. The checkpoint prevents loss. The keyed,
latest-wins MERGE prevents duplicates, because applying the same upsert twice gives the
same table. And Iceberg's atomic commit means a crash mid-MERGE leaves either the whole
batch or none of it."

---

### SUNOJ: Step 8, three engines, one table (1.5 min)

**DO:** `bash scripts/demo.sh 8`

"Last, the open-format point. The same aggregation, orders and revenue per region, run
by three different engines on the same Iceberg files on HDFS."

**POINT AT:** the Spark table, the ClickHouse rows, the Hive rows.

"Spark, ClickHouse and Hive all return the same numbers. ClickHouse reads the files in
place with its icebergHDFS function. There is no copy and no export step."

"One practical lesson: Hive's Parquet reader on macOS couldn't decode zstd compression,
so we rewrite the table with gzip before Hive reads it. An open format only helps if
every engine supports the options you chose."

"Vivek will close with the second experiment."

---

### VIVEK: Step 9, E2 copy-on-write vs merge-on-read (1.5 min)

**DO:** `bash scripts/demo.sh 9`

"Iceberg has two ways to apply updates. Copy-on-write rewrites the data files on every
update. Merge-on-read writes small delete files instead and merges them in at read time.
The docs say merge-on-read is cheaper to write and is the choice for update-heavy tables."

"We tested it: 2 million rows, 20 rounds of MERGE, at 0.1, 1, 5 and 20 percent of rows
updated per round, with no compaction in between."

**POINT AT:** the `total_cost_crossover_round` column.

"Merge-on-read did write faster, but only for about the first commit. After that each
MERGE got slower, because a MERGE has to read the table, including every delete file
built up so far. By round 20 its merges were 3 to 7 times slower than copy-on-write.
Counting write plus read time, merge-on-read lost by round 1 to 6 at every update ratio."

**POINT AT:** the bytes-written columns.

"Its one lasting win is bytes written: up to 290 times fewer. So the rule from our data:
use merge-on-read only if you compact every few commits, or if storage I/O is your real
cost."

---

### SUNOJ: Limitations (0.5 min)

"To be honest about limits: everything ran on one 16 GB laptop with a single HDFS
DataNode, so only the shape of each curve carries over, not the absolute times. We report
medians without spread. Schema and partition evolution were in our plan but not built.
And our experiments found bugs in our own test setup, a timezone mismatch and tied
timestamps, which we fixed before the final runs. The report documents all of it."

---

### VIVEK: Closing (0.5 min)

"To summarise: we built a working CDC lakehouse on Kafka, Spark, Iceberg and HDFS, with
three engines reading one table. Then we measured it. Exactly-once held up across 250
kills, and we showed which part does what. Small files hurt execution, not planning. And
merge-on-read loses to copy-on-write unless you compact often. Every number we showed
comes from a CSV in our repo. Thank you, we're happy to take questions."

---

## Part 4: Q&A, who answers what

| Likely question | Answers | Short answer |
|---|---|---|
| Why Iceberg and not Hive ACID or Delta Lake? | Tejasri | Hive ACID changes are visible only to Hive; Iceberg's metadata is an open spec, so Spark, ClickHouse and Hive all read it (step 8). |
| Why `foreachBatch` and not a direct Iceberg sink? | Vivek | A streaming sink only appends. MERGE needs a batch DataFrame, which `foreachBatch` provides. |
| How do you handle two events for one order in the same batch? | Vivek | Deduplicate per `order_id`, keep the latest by `ts_ms` with a tie-breaker, then one MERGE. |
| Is the checkpoint alone exactly-once? | Sunoj | No. Spark can replay the last batch after a crash. The idempotent MERGE makes the replay harmless. |
| Why pause the stream for maintenance? | Tejasri | Concurrent commits made `rewrite_manifests` fail validation. In production you'd schedule maintenance between commits or retry. |
| Would results hold on a real cluster? | Sunoj | Absolute times no, curve shapes and crossovers likely yes. Stated as a limitation. |
| Why was planning flat in E1? | Tejasri | Each test table was one commit, so all file entries sat in one or two manifests. Planning reads the same few metadata files at any file count. |
| How often should you compact a MOR table? | Vivek | On our data, every 1-6 update commits, from the total-cost crossover. |
| What was the hardest bug? | Sunoj | The old chaos harness's drain used `timeout`, which macOS doesn't have, and `|| true` hid the failure, so earlier passes were not real. |
