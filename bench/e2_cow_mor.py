#!/usr/bin/env python3
"""E2 - copy-on-write vs merge-on-read crossover (SPEC section 9.2).

Two tables differing only in write.{update,delete,merge}.mode take the same
deterministic update stream for `rounds` rounds. Per round and per table we
record write cost, bytes actually written, full-scan read cost and point-lookup
cost, so the read crossover and the total-cost crossover can be read off the CSV.

Catalog is Iceberg HadoopCatalog on HDFS: E2 is self-contained and does not need
the Hive Metastore (SPEC section 6.2 fallback).
"""
import os
import csv
import time
import argparse
import datetime
import statistics
import subprocess

from pyspark.sql import SparkSession

HDFS_URL = "hdfs://localhost:9000"
CATALOG = "lh"
NAMESPACE = "e2"
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"

# Reads are repeated READ_REPS + 1 times; the first is discarded as JVM warm-up
# and the median of the rest is reported (SPEC section 9 methodology).
READ_REPS = 3
PCT_SCALE = 10000  # resolution of the deterministic row sampler


def get_spark(driver_memory):
    return (SparkSession.builder
        .appName("E2_COW_MOR")
        .master("local[4]")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"{HDFS_URL}/warehouse/e2")
        # This Mac has several active interfaces; left to itself Spark can pick a
        # link-local IPv6 address for the driver and then time out connecting to
        # itself. Pin the loopback.
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", driver_memory)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())


def git_commit():
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def measure(spark, query):
    start = time.time()
    spark.sql(query).collect()
    return (time.time() - start) * 1000.0


def measure_read(spark, query):
    times = [measure(spark, query) for _ in range(READ_REPS + 1)]
    return round(statistics.median(times[1:]), 2)


def table_name(mode, p_pct):
    tag = str(p_pct).replace(".", "_")
    return f"{CATALOG}.{NAMESPACE}.orders_{mode}_p{tag}"


def base_rows(spark, rows):
    """The starting dataset. Deterministic, so both tables are byte-identical."""
    return spark.range(rows).selectExpr(
        "concat('ORD-', cast(id as string)) as order_id",
        "concat('CUST-', cast(id % 1000 as string)) as customer_id",
        "'PLACED' as status",
        "cast(id % 100 as decimal(12,2)) as amount",
        "'INR' as currency",
        "CASE WHEN id % 4 = 0 THEN 'north' WHEN id % 4 = 1 THEN 'south'"
        "     WHEN id % 4 = 2 THEN 'east' ELSE 'west' END as region",
        "timestamp'2026-09-01 00:00:00' as updated_at",
    )


def create_table(spark, mode, p_pct, rows):
    table = table_name(mode, p_pct)
    write_mode = "copy-on-write" if mode == "cow" else "merge-on-read"
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"""
    CREATE TABLE {table} (
        order_id STRING, customer_id STRING, status STRING,
        amount DECIMAL(12,2), currency STRING, region STRING,
        updated_at TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (region)
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='{write_mode}',
        'write.delete.mode'='{write_mode}',
        'write.merge.mode'='{write_mode}',
        'write.target-file-size-bytes'='134217728'
    )
    """)
    base_rows(spark, rows).createOrReplaceTempView("e2_base")
    spark.sql(f"INSERT INTO {table} SELECT * FROM e2_base")
    return table


def stage_updates(spark, rows, p_pct, round_idx):
    """Register the round's update batch.

    Row selection is a hash of (order_id, round), not rand(), so the two tables
    receive exactly the same rows and a rerun reproduces the same sweep.
    """
    threshold = int(p_pct / 100.0 * PCT_SCALE)
    base_rows(spark, rows).createOrReplaceTempView("e2_base")
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW e2_updates AS
    SELECT order_id, customer_id, 'SHIPPED' as status,
           amount + {round_idx}.00 as amount, currency, region,
           updated_at + INTERVAL {round_idx} HOURS as updated_at
    FROM e2_base
    WHERE pmod(crc32(concat(order_id, '#', {round_idx})), {PCT_SCALE}) < {threshold}
    """)
    return spark.sql("SELECT count(*) FROM e2_updates").collect()[0][0]


def last_snapshot_summary(spark, table):
    row = spark.sql(
        f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    return row["summary"] or {}


def file_bytes(spark, table):
    data = spark.sql(
        f"SELECT coalesce(sum(file_size_in_bytes), 0) FROM {table}.files"
    ).collect()[0][0]
    deletes = spark.sql(
        f"SELECT coalesce(sum(file_size_in_bytes), 0), count(*) FROM {table}.delete_files"
    ).collect()[0]
    return int(data), int(deletes[0]), int(deletes[1])


def run_round(spark, table, mode, rows, p_pct, round_idx, update_count, meta):
    merge_sql = f"""
    MERGE INTO {table} t
    USING e2_updates s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET *
    """
    write_ms = measure(spark, merge_sql)

    summary = last_snapshot_summary(spark, table)
    data_bytes, delete_bytes, delete_files = file_bytes(spark, table)

    # count(*) is answered from Iceberg metadata without touching the data, so
    # the read probe has to aggregate a real column to measure anything.
    scan_ms = measure_read(spark, f"""
        SELECT region, count(*), sum(amount) FROM {table} GROUP BY region
    """)
    point_ms = measure_read(spark, f"""
        SELECT * FROM {table} WHERE order_id = 'ORD-{rows // 2}'
    """)

    return {
        "p_pct": p_pct,
        "round": round_idx,
        "table_type": mode,
        "rows": rows,
        "update_count": update_count,
        "write_ms": round(write_ms, 2),
        "added_bytes": int(summary.get("added-files-size", 0)),
        "added_data_files": int(summary.get("added-data-files", 0)),
        "added_delete_files": int(summary.get("added-delete-files", 0)),
        "total_data_bytes": data_bytes,
        "total_delete_bytes": delete_bytes,
        "delete_files": delete_files,
        "scan_ms": scan_ms,
        "point_ms": point_ms,
        "git_commit": meta["git_commit"],
        "run_ts": meta["run_ts"],
    }


def write_csv(path, results):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {path}")


def crossover(results, metric):
    """First round from which MOR stays worse than COW on `metric`."""
    by_round = {}
    for row in results:
        by_round.setdefault(row["round"], {})[row["table_type"]] = row
    rounds = sorted(by_round)
    for i, r in enumerate(rounds):
        if all(by_round[x]["mor"][metric] > by_round[x]["cow"][metric] for x in rounds[i:]):
            return r
    return None


def total_cost_crossover(results):
    by_round = {}
    for row in results:
        by_round.setdefault(row["round"], {})[row["table_type"]] = row
    cum = {"cow": 0.0, "mor": 0.0}
    for r in sorted(by_round):
        for mode in ("cow", "mor"):
            row = by_round[r][mode]
            cum[mode] += row["write_ms"] + row["scan_ms"]
        if cum["mor"] > cum["cow"]:
            return r
    return None


def run_sweep(spark, rows, p_pct, rounds, meta):
    print(f"\n=== sweep p={p_pct}% rows={rows} rounds={rounds} ===")
    tables = {mode: create_table(spark, mode, p_pct, rows) for mode in ("cow", "mor")}

    results = []
    for r in range(1, rounds + 1):
        update_count = stage_updates(spark, rows, p_pct, r)
        line = [f"  round {r:>2}/{rounds} updates={update_count}"]
        for mode in ("cow", "mor"):
            row = run_round(spark, tables[mode], mode, rows, p_pct, r, update_count, meta)
            results.append(row)
            line.append(
                f"{mode}: write={row['write_ms']:.0f}ms scan={row['scan_ms']:.0f}ms "
                f"del_files={row['delete_files']}"
            )
        print(" | ".join(line), flush=True)

    tag = str(p_pct).replace(".", "_")
    write_csv(f"results/e2/cow_vs_mor_p{tag}.csv", results)

    return {
        "p_pct": p_pct,
        "rows": rows,
        "rounds": rounds,
        "read_crossover_round": crossover(results, "scan_ms"),
        "point_crossover_round": crossover(results, "point_ms"),
        "total_cost_crossover_round": total_cost_crossover(results),
        "cow_total_write_ms": round(sum(r["write_ms"] for r in results if r["table_type"] == "cow"), 2),
        "mor_total_write_ms": round(sum(r["write_ms"] for r in results if r["table_type"] == "mor"), 2),
        "cow_total_bytes_written": sum(r["added_bytes"] for r in results if r["table_type"] == "cow"),
        "mor_total_bytes_written": sum(r["added_bytes"] for r in results if r["table_type"] == "mor"),
        "mor_final_delete_files": max(r["delete_files"] for r in results if r["table_type"] == "mor"),
        "git_commit": meta["git_commit"],
        "run_ts": meta["run_ts"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--rounds", type=int, default=20)
    parser.add_argument("--pct", type=float, action="append",
                        help="update ratio in percent; repeatable. Default: 0.1 1 5 20")
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args()

    sweeps = args.pct if args.pct else [0.1, 1, 5, 20]
    meta = {
        "git_commit": git_commit(),
        "run_ts": datetime.datetime.now().isoformat(timespec="seconds"),
    }

    spark = get_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")

    summaries = [run_sweep(spark, args.rows, p, args.rounds, meta) for p in sweeps]
    write_csv("results/e2/crossover_summary.csv", summaries)

    print("\n--- crossover summary ---")
    for s in summaries:
        print(f"p={s['p_pct']}%  read crossover: {s['read_crossover_round']}  "
              f"total-cost crossover: {s['total_cost_crossover_round']}  "
              f"MOR delete files: {s['mor_final_delete_files']}")


if __name__ == "__main__":
    main()
