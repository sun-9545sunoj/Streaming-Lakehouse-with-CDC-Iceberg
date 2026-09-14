#!/usr/bin/env python3
import os
import time
import json
import uuid
import random
import csv
from pyspark.sql import SparkSession

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"

def get_spark():
    return (SparkSession.builder
        .appName("E1_Planning")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
        .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())

def get_metadata_bytes():
    import subprocess
    try:
        res = subprocess.check_output(
            ["docker", "exec", "hadoop", "hdfs", "dfs", "-du", "-s", "/warehouse/default.db/orders_e1_final/metadata"],
            stderr=subprocess.DEVNULL
        ).decode().strip()
        if res:
            return int(res.split()[0])
    except:
        pass
    return 0

def measure_planning_time(spark, query):
    start = time.time()
    spark.sql(f"EXPLAIN {query}").collect()
    return (time.time() - start) * 1000.0

def measure_execution_time(spark, query):
    start = time.time()
    spark.sql(query).collect()
    return (time.time() - start) * 1000.0

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    table = "default.orders_e1_final"
    
    q_agg = f"SELECT count(*) FROM {table}"
    q_filter = f"SELECT count(*) FROM {table} WHERE region = 'north'"
    q_point = f"SELECT * FROM {table} WHERE order_id = 'ORD-E1-TEST'"
    
    targets = [10, 50, 100, 500, 1000, 2500, 5000]
    
    print("Setting up E1 table...")
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    spark.sql(f"""
    CREATE TABLE {table} (
        order_id STRING, customer_id STRING, status STRING, 
        amount DECIMAL(12,2), currency STRING, region STRING, 
        updated_at TIMESTAMP, _op STRING, _ingest_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (region)
    TBLPROPERTIES ('write.target-file-size-bytes'='134217728', 'commit.manifest.min-count-to-merge'='100')
    """)
    
    spark.sql(f"""
        INSERT INTO {table} VALUES 
        ('ORD-E1-TEST', 'C-1', 'PLACED', 100.00, 'USD', 'north', current_timestamp(), 'c', current_timestamp())
    """)
    
    results = []
    current_files = 1
    
    def add_files(target_added):
        print(f"  Adding {target_added} files...")
        chunk_size = 250
        remaining = target_added
        while remaining > 0:
            batch = min(chunk_size, remaining)
            df = spark.range(batch).selectExpr(
                "concat('ORD-', cast(id as string)) as order_id",
                "'C-X' as customer_id",
                "'PLACED' as status",
                "cast(id as decimal(12,2)) as amount",
                "'USD' as currency",
                "CASE WHEN id % 2 = 0 THEN 'north' ELSE 'south' END as region",
                "current_timestamp() as updated_at",
                "'c' as _op",
                "current_timestamp() as _ingest_ts"
            ).repartition(batch)
            df.createOrReplaceTempView("batch_data")
            spark.sql(f"INSERT INTO {table} SELECT * FROM batch_data")
            remaining -= batch

    for target in targets:
        needed = target - current_files
        if needed > 0:
            add_files(needed)
            current_files = target
            
        print(f"\\n--- Measuring at {target} files ---")
        actual_files = spark.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0]
        meta_bytes = get_metadata_bytes()
        
        import statistics
        plan_times = [measure_planning_time(spark, q_filter) for _ in range(6)]
        agg_times = [measure_execution_time(spark, q_agg) for _ in range(6)]
        filter_times = [measure_execution_time(spark, q_filter) for _ in range(6)]
        point_times = [measure_execution_time(spark, q_point) for _ in range(6)]
        
        row = {
            "target_files": target,
            "actual_files": actual_files,
            "meta_bytes": meta_bytes,
            "state": "uncompacted",
            "plan_ms": round(statistics.median(plan_times[1:]), 2),
            "agg_ms": round(statistics.median(agg_times[1:]), 2),
            "filter_ms": round(statistics.median(filter_times[1:]), 2),
            "point_ms": round(statistics.median(point_times[1:]), 2)
        }
        results.append(row)
        print(row)
        
    print("\\n--- Running Compaction ---")
    spark.sql(f"CALL spark_catalog.system.rewrite_data_files('{table}')")
    spark.sql(f"CALL spark_catalog.system.rewrite_manifests('{table}')")
    
    actual_files = spark.sql(f"SELECT count(*) FROM {table}.files").collect()[0][0]
    meta_bytes = get_metadata_bytes()
    
    plan_times = [measure_planning_time(spark, q_filter) for _ in range(6)]
    agg_times = [measure_execution_time(spark, q_agg) for _ in range(6)]
    filter_times = [measure_execution_time(spark, q_filter) for _ in range(6)]
    point_times = [measure_execution_time(spark, q_point) for _ in range(6)]
    
    row = {
        "target_files": "compacted",
        "actual_files": actual_files,
        "meta_bytes": meta_bytes,
        "state": "compacted",
        "plan_ms": round(statistics.median(plan_times[1:]), 2),
        "agg_ms": round(statistics.median(agg_times[1:]), 2),
        "filter_ms": round(statistics.median(filter_times[1:]), 2),
        "point_ms": round(statistics.median(point_times[1:]), 2)
    }
    results.append(row)
    print(row)
    
    os.makedirs("results/e1", exist_ok=True)
    keys = results[0].keys()
    with open("results/e1/planning_vs_files.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    print("\\nSaved results to results/e1/planning_vs_files.csv")

if __name__ == "__main__":
    main()
