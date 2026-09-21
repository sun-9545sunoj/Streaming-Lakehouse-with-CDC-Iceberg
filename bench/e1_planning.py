#!/usr/bin/env python3
"""E1 - query planning cost vs data-file count (SPEC section 9.1).

Total row count is held constant; only the number of data files varies. Each
target builds its own table whose row-to-file mapping is forced exactly, then is
measured, compacted and measured again, so the before/after curves come from the
same data at every point.

Catalog is Iceberg HadoopCatalog on HDFS (SPEC section 6.2 fallback) - E1 does
not need the Hive Metastore.
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
NAMESPACE = "e1"
ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
REGIONS = ["north", "south", "east", "west"]


def get_spark(driver_memory):
    return (SparkSession.builder
        .appName("E1_Planning")
        .master("local[4]")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", f"{HDFS_URL}/warehouse/e1")
        # Adaptive execution coalesces the write tasks, which silently collapses
        # the file count the sweep is built to control. This is why the first
        # run of this benchmark never got past ~47 files at a 5000 target.
        .config("spark.sql.adaptive.enabled", "false")
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


def table_name(target):
    return f"{CATALOG}.{NAMESPACE}.orders_e1_f{target}"


def build_table(spark, target, rows):
    """Create a table holding `rows` rows spread over exactly `target` files.

    Each write task must own exactly one partition value, otherwise it emits one
    file per partition it touches and the file count is a multiple of the task
    count. Region is therefore derived from the same bucket that drives the
    repartition, so bucket -> task -> one region -> one file.
    """
    table = table_name(target)
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"""
    CREATE TABLE {table} (
        order_id STRING, customer_id STRING, status STRING,
        amount DECIMAL(12,2), currency STRING, region STRING,
        updated_at TIMESTAMP, _op STRING, _ingest_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (region, days(updated_at))
    TBLPROPERTIES (
        'format-version'='2',
        'write.target-file-size-bytes'='134217728',
        -- Iceberg defaults a partitioned table to distribution-mode=hash, which
        -- reshuffles by partition key after the repartition below and collapses
        -- every target down to one file per partition. 'none' leaves the task
        -- layout alone, which is what makes the file count controllable.
        'write.distribution-mode'='none',
        -- With no distribution the rows reaching a task are not clustered by
        -- partition, so the writer needs fanout to keep several files open.
        'write.spark.fanout.enabled'='true'
    )
    """)

    region_case = " ".join(
        f"WHEN _bucket % 4 = {i} THEN '{r}'" for i, r in enumerate(REGIONS)
    )
    df = (spark.range(rows)
        .selectExpr("id", f"pmod(id, {target}) as _bucket")
        .selectExpr(
            "concat('ORD-', cast(id as string)) as order_id",
            "concat('CUST-', cast(id % 1000 as string)) as customer_id",
            "'PLACED' as status",
            "cast(id % 1000 as decimal(12,2)) as amount",
            "'INR' as currency",
            f"CASE {region_case} END as region",
            "timestamp'2026-09-01 00:00:00' as updated_at",
            "'c' as _op",
            "timestamp'2026-09-01 00:00:00' as _ingest_ts",
            "_bucket",
        )
        .repartition(target, "_bucket")
        .drop("_bucket"))

    df.createOrReplaceTempView("e1_rows")
    spark.sql(f"INSERT INTO {table} SELECT * FROM e1_rows")
    return table


def table_location(spark, table):
    for row in spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect():
        if row[0].strip().lower() == "location":
            return row[1].strip()
    return None


def metadata_bytes(spark, table):
    """Total size of the manifest / manifest-list / metadata JSON tree."""
    location = table_location(spark, table)
    if not location:
        return 0
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{location}/metadata")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return 0
    return int(fs.getContentSummary(path).getLength())


def measure(spark, query, reps):
    """Median wall time over `reps` runs after discarding a warm-up run."""
    times = []
    for _ in range(reps + 1):
        start = time.time()
        spark.sql(query).collect()
        times.append((time.time() - start) * 1000.0)
    return round(statistics.median(times[1:]), 2)


def measure_planning(spark, query, reps):
    times = []
    for _ in range(reps + 1):
        start = time.time()
        spark.sql(f"EXPLAIN {query}").collect()
        times.append((time.time() - start) * 1000.0)
    return round(statistics.median(times[1:]), 2)


def measure_point(spark, table, rows, state, target, reps, meta):
    q_agg = f"SELECT region, count(*), sum(amount) FROM {table} GROUP BY region"
    q_filter = f"SELECT count(*) FROM {table} WHERE region = 'north'"
    q_point = f"SELECT * FROM {table} WHERE order_id = 'ORD-{rows // 2}'"

    files = spark.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0]
    # Split count is the number of tasks the scan plans, which is the cost the
    # planner actually pays for fragmentation.
    splits = spark.sql(q_filter).rdd.getNumPartitions()

    return {
        "target_files": target,
        "actual_files": files,
        "splits": splits,
        "meta_bytes": metadata_bytes(spark, table),
        "state": state,
        "rows": rows,
        "plan_ms": measure_planning(spark, q_filter, reps),
        "agg_ms": measure(spark, q_agg, reps),
        "filter_ms": measure(spark, q_filter, reps),
        "point_ms": measure(spark, q_point, reps),
        "git_commit": meta["git_commit"],
        "run_ts": meta["run_ts"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--targets", default="10,50,100,500,1000,2500,5000")
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--keep-tables", action="store_true",
                        help="do not drop each table after measuring it (needs far more disk)")
    args = parser.parse_args()

    targets = [int(t) for t in args.targets.split(",")]
    meta = {
        "git_commit": git_commit(),
        "run_ts": datetime.datetime.now().isoformat(timespec="seconds"),
    }

    spark = get_spark(args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")

    results = []
    for target in targets:
        print(f"\n--- target {target} files, {args.rows} rows ---", flush=True)
        table = build_table(spark, target, args.rows)

        row = measure_point(spark, table, args.rows, "uncompacted", target, args.reps, meta)
        results.append(row)
        print(f"  uncompacted: files={row['actual_files']} splits={row['splits']} "
              f"plan={row['plan_ms']}ms meta={row['meta_bytes']}B", flush=True)

        compact_start = time.time()
        spark.sql(f"CALL {CATALOG}.system.rewrite_data_files(table => '{NAMESPACE}.orders_e1_f{target}')")
        spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{NAMESPACE}.orders_e1_f{target}')")
        compact_ms = round((time.time() - compact_start) * 1000.0, 2)

        row = measure_point(spark, table, args.rows, "compacted", target, args.reps, meta)
        row["compact_ms"] = compact_ms
        results.append(row)
        print(f"  compacted:   files={row['actual_files']} splits={row['splits']} "
              f"plan={row['plan_ms']}ms meta={row['meta_bytes']}B compaction={compact_ms}ms", flush=True)

        if not args.keep_tables:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")

    os.makedirs("results/e1", exist_ok=True)
    fieldnames = list(results[0].keys()) + ["compact_ms"]
    fieldnames = list(dict.fromkeys(fieldnames))
    with open("results/e1/planning_vs_files.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print("\nSaved results/e1/planning_vs_files.csv")


if __name__ == "__main__":
    main()
