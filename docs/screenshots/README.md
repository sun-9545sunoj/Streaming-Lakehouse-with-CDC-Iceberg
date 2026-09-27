# Screenshot set (SPEC section 12)

One screenshot per demo step. Every step prints the team banner (all three members'
names and roll numbers, plus the date) first, so each capture is self-labelled.

```bash
bash scripts/demo.sh 0      # prerequisites: HDFS, Kafka, empty topic and table
bash scripts/demo.sh 1      # screenshot -> step1.png  producer + ingest running
bash scripts/demo.sh 2      # screenshot -> step2.png  counts move between two queries
bash scripts/demo.sh 3      # screenshot -> step3.png  one order CANCELLED via Kafka
bash scripts/demo.sh 4      # screenshot -> step4.png  files, history, snapshots
bash scripts/demo.sh 5      # screenshot -> step5.png  compaction + snapshot expiry
bash scripts/demo.sh 6      # screenshot -> step6.png  partition delete, rollback
bash scripts/demo.sh 7      # screenshot -> step7.png  kill -9, restart, verify
bash scripts/demo.sh 8      # screenshot -> step8.png  Spark, ClickHouse, Hive agree
bash scripts/demo.sh 9      # screenshot -> step9.png  E2 crossover summary
bash scripts/demo.sh stop
```

Steps 2-6 run against the live table from step 1, so run them in order. Step 7 resets
the table.

`demo_transcript.txt` is the full text output of the rehearsal run on 2026-09-27
(steps 0-9, unattended). It is the fallback if the live cluster misbehaves.
