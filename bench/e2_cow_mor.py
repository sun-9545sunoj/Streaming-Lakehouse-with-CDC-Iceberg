#!/usr/bin/env python3
import os
import time
import csv
import argparse
from pyspark.sql import SparkSession
import statistics

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"

def get_spark():
    return (SparkSession.builder
        .appName("E2_COW_MOR")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
        .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate())

def measure_time(spark, query):
    start = time.time()
    spark.sql(query).collect()
    return (time.time() - start) * 1000.0

def setup_tables(spark, initial_rows):
    print("Setting up base tables...")
    spark.sql("DROP TABLE IF EXISTS default.orders_cow22 PURGE")
    spark.sql("DROP TABLE IF EXISTS default.orders_mor2 PURGE")
    
    schema = """
        order_id STRING, customer_id STRING, status STRING, 
        amount DECIMAL(12,2), currency STRING, region STRING, 
        updated_at TIMESTAMP
    """
    
    spark.sql(f"""
    CREATE TABLE default.orders_cow22 ({schema}) USING iceberg PARTITIONED BY (region)
    TBLPROPERTIES ('write.update.mode'='copy-on-write', 'write.delete.mode'='copy-on-write', 'write.merge.mode'='copy-on-write')
    """)
    
    spark.sql(f"""
    CREATE TABLE default.orders_mor2 ({schema}) USING iceberg PARTITIONED BY (region)
    TBLPROPERTIES ('write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
    """)
    
    # Generate initial data
    df = spark.range(initial_rows).selectExpr(
        "concat('ORD-', cast(id as string)) as order_id",
        "'CUST-1' as customer_id",
        "'PLACED' as status",
        "cast(id % 100 as decimal(12,2)) as amount",
        "'USD' as currency",
        "CASE WHEN id % 4 = 0 THEN 'north' WHEN id % 4 = 1 THEN 'south' WHEN id % 4 = 2 THEN 'east' ELSE 'west' END as region",
        "current_timestamp() as updated_at"
    ).repartition(20) # 20 files
    
    df.createOrReplaceTempView("initial_data")
    
    print(f"Writing {initial_rows} rows to COW table...")
    spark.sql("INSERT INTO default.orders_cow22 SELECT * FROM initial_data")
    
    print(f"Writing {initial_rows} rows to MOR table...")
    spark.sql("INSERT INTO default.orders_mor2 SELECT * FROM initial_data")

def run_sweep(spark, p_pct, rounds=20):
    print(f"\\n=== Starting Sweep for {p_pct}% update ratio ===")
    results = []
    frac = p_pct / 100.0
    
    for r in range(1, rounds + 1):
        print(f"  Round {r}/{rounds}...")
        
        # Create staging updates. We randomly sample `frac` of the COW table (which has the same ids as MOR).
        spark.sql(f"""
        CREATE OR REPLACE TABLE default.batch_updates USING iceberg AS
        SELECT order_id, customer_id, 'SHIPPED' as status, amount + 1.00 as amount, currency, region, current_timestamp() as updated_at
        FROM default.orders_cow2
        WHERE rand() < {frac}
        """)
        update_count = spark.sql("SELECT count(*) FROM default.batch_updates").collect()[0][0]
        print(f"    Selected {update_count} rows for update.")
        
        for table_type in ["cow2", "mor2"]:
            table_name = f"default.orders_{table_type}"
            
            # 1. Measure MERGE time
            merge_query = f"""
            MERGE INTO {table_name} t
            USING default.batch_updates s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET *
            """
            write_time = measure_time(spark, merge_query)
            
            # 2. Measure READ time (full scan)
            read_times = []
            for _ in range(3):
                read_times.append(measure_time(spark, f"SELECT count(*) FROM {table_name}"))
            read_time = statistics.median(read_times)
            
            # 3. Get delete-file count
            # In Iceberg `.files`, content=1 means POSITIONAL DELETE, content=2 means EQUALITY DELETE. 0 is DATA.
            del_files = spark.sql(f"SELECT count(*) FROM {table_name}.files WHERE content IN (1, 2)").collect()[0][0]
            
            results.append({
                "p_pct": p_pct,
                "round": r,
                "table_type": table_type,
                "update_count": update_count,
                "write_ms": round(write_time, 2),
                "read_ms": round(read_time, 2),
                "delete_files": del_files
            })
            
        spark.sql("DROP TABLE IF EXISTS default.batch_updates PURGE")
        
    # Write to CSV
    os.makedirs("results/e2", exist_ok=True)
    keys = results[0].keys()
    out_file = f"results/e2/cow_vs_mor_{p_pct}pct.csv"
    with open(out_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    print(f"Saved {out_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000) # 100k rows default
    parser.add_argument("--pct", type=float, help="Run a specific percentage (e.g. 5)")
    args = parser.parse_args()
    
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    
    setup_tables(spark, args.rows)
    
    if args.pct is not None:
        sweeps = [args.pct]
    else:
        sweeps = [0.1, 1, 5, 20]
        
    for p in sweeps:
        run_sweep(spark, p, rounds=20)
        
    print("E2 completely finished.")

if __name__ == "__main__":
    main()
