import time
import argparse
from pyspark.sql import SparkSession

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"

def get_spark():
    return (SparkSession.builder
        .appName("IcebergMaintenance")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
        .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse")
        .getOrCreate())

def run_query_with_timing(spark, query):
    print(f"\nExecuting: {query}")
    start = time.time()
    try:
        df = spark.sql(query)
        df.show(truncate=False)
        duration = time.time() - start
        print(f"Completed in {duration:.2f} seconds.")
    except Exception as e:
        # A swallowed failure here used to exit 0, so a compaction that never
        # ran still looked like a successful maintenance run.
        print(f"Error executing query: {e}")
        raise

def rewrite_data_files(spark, table="default.orders"):
    run_query_with_timing(spark, f"CALL spark_catalog.system.rewrite_data_files(table => '{table}')")

def rewrite_manifests(spark, table="default.orders"):
    run_query_with_timing(spark, f"CALL spark_catalog.system.rewrite_manifests(table => '{table}')")

def expire_snapshots(spark, table="default.orders", older_than=None, retain_last=1):
    if older_than:
        run_query_with_timing(spark, f"CALL spark_catalog.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{older_than}')")
    else:
        run_query_with_timing(spark, f"CALL spark_catalog.system.expire_snapshots(table => '{table}', retain_last => {retain_last})")

def remove_orphan_files(spark, table="default.orders"):
    run_query_with_timing(spark, f"CALL spark_catalog.system.remove_orphan_files(table => '{table}')")

def rollback_to_snapshot(spark, table="default.orders", snapshot_id=None):
    if snapshot_id is None:
        print("Please provide a snapshot ID to rollback to.")
        return
    run_query_with_timing(spark, f"CALL spark_catalog.system.rollback_to_snapshot(table => '{table}', snapshot_id => {snapshot_id})")

def get_history(spark, table="default.orders"):
    run_query_with_timing(spark, f"SELECT * FROM {table}.history")
    run_query_with_timing(spark, f"SELECT * FROM {table}.snapshots")
    run_query_with_timing(spark, f"SELECT count(*) as file_count FROM {table}.files")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iceberg Table Maintenance")
    parser.add_argument("--compact", action="store_true", help="Rewrite data files and manifests")
    parser.add_argument("--expire", action="store_true", help="Expire snapshots retaining only the last 1")
    parser.add_argument("--orphan", action="store_true", help="Remove orphan files")
    parser.add_argument("--history", action="store_true", help="Show table history, snapshots, and file count")
    parser.add_argument("--rollback", type=int, help="Rollback to a specific snapshot ID")
    parser.add_argument("--table", default="default.orders", help="Target table")
    
    args = parser.parse_args()
    
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    
    if args.history:
        get_history(spark, args.table)
    if args.compact:
        rewrite_data_files(spark, args.table)
        rewrite_manifests(spark, args.table)
    if args.expire:
        expire_snapshots(spark, args.table)
    if args.orphan:
        remove_orphan_files(spark, args.table)
    if args.rollback is not None:
        rollback_to_snapshot(spark, args.table, args.rollback)
