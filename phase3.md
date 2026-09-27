# Phase 3 Summary: Benchmarking (E1 and E2)

## Overview
Phase 3 focused on systematically measuring Iceberg's performance under various conditions to validate the theoretical benefits of the Lakehouse architecture, specifically focusing on the impacts of file fragmentation (E1) and table formats (E2).

## Architecture & Implementation
We implemented a robust benchmarking harness under the `bench/` directory:

1. **E1 (Planning Cost vs. File Count)** `bench/e1_planning.py`:
   - Simulates the "Small File Problem" by creating Iceberg tables with increasing degrees of fragmentation (from 10 files up to 5,000 files).
   - Captures query planning overhead in milliseconds.
   - Triggers native Iceberg Compaction (`rewrite_data_files` and `rewrite_manifests`).
   - Remeasures planning overhead to demonstrate the performance reclaimed via compaction.

2. **E2 (Copy-on-Write vs Merge-on-Read)** `bench/e2_cow_mor.py`:
   - Builds two identical 100,000 row tables (one configured for COW, the other for MOR).
   - Generates update batches of varying percentages (0.1%, 1%, 5%, 20%).
   - Measures Write Amplification (time taken to execute `MERGE INTO`).
   - Measures Read Amplification (time taken to execute a full table scan).

3. **Plotting Framework** `bench/plots.py`:
   - Reads the generated CSV metrics from `results/`.
   - Utilizes `matplotlib` and `pandas` to generate visual graphs of the performance data.

## Execution Results

> **Revised 2026-09-26.** The first version of this section reported E1 numbers from a
> harness that produced only 3-47 files for targets of 10-5,000 and timed Spark `EXPLAIN`
> of a `count(*)`, which Iceberg answers from metadata. It also blamed Derby for E2 not
> completing. The actual cause was a table-name typo: the harness created
> `default.orders_cow22` and then merged into `default.orders_cow2`. Both harnesses were
> fixed and rerun. The numbers below come from `results/e1/` and `results/e2/`, and the
> full analysis is in `docs/REPORT.md` sections 4 and 5.

### Experiment 1: file count at 5,000,000 rows

- File counts reached 9 to 4,389 (targets 10 to 5,000).
- **Iceberg scan planning** (`planTasks`) grew from 1.9 ms to 6.6 ms, roughly linear and
  small. All file entries sit in one or two manifests, so planning is not where small files
  hurt at this scale.
- **Execution is.** A full-scan GROUP BY went from 292 ms to 2,528 ms and a point lookup
  from 113 ms to 1,841 ms. Both curves bend between 500 and 1,000 files (below ~10k rows
  per file).
- **Compaction** to 4 files restored the point lookup (~180 ms), but it left one scan task
  for four cores, so the GROUP BY settled at ~550 ms. That is slower than the 44-file layout.

### Experiment 2: COW vs MOR at 2,000,000 rows, 20 rounds, p = 0.1 / 1 / 5 / 20 %

- **Round 1:** MOR merged 2.4-2.8x faster at 5-20 % and wrote 4-600x fewer bytes.
- **After that:** MOR's MERGE slowed every round, as the join read an ever-larger set of
  delete files, reaching 3-7x COW's time by round 20. MOR scans were slower from round 1
  and ~3.2x slower by round 20.
- **Total-cost crossover:** rounds 1-6. Without compaction every few commits, copy-on-write
  was cheaper overall at every update ratio tested.
- Figures: `docs/figures/e1_planning.png`, `docs/figures/e2_overview.png`.

## Next Steps
We are now ready to progress to Phase 4 (KRaft Kafka and Chaos Testing) and Phase 5 (ClickHouse integration).
