"""Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup, rollback.

The catalog comes from src/spark_session.py (E3_CATALOG=hive|hadoop), so this
works on the same table the ingest job writes. --table is the bare table name.
"""
import time
import argparse
import datetime

from spark_session import build_session, table_id, procedure_catalog, CATALOG_TYPE


def procedure_table_arg(table):
    """Procedures take the identifier without the catalog prefix."""
    return table.split(".", 1)[1] if CATALOG_TYPE == "hadoop" else table

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

def rewrite_data_files(spark, table):
    run_query_with_timing(spark, f"CALL {procedure_catalog()}.system.rewrite_data_files(table => '{procedure_table_arg(table)}')")

def rewrite_manifests(spark, table):
    run_query_with_timing(spark, f"CALL {procedure_catalog()}.system.rewrite_manifests(table => '{procedure_table_arg(table)}')")

def expire_snapshots(spark, table, retain_last=1):
    # retain_last alone expires nothing on a young table: Iceberg still applies
    # its default older_than of now - 5 days. Pass now explicitly.
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_query_with_timing(spark, f"CALL {procedure_catalog()}.system.expire_snapshots("
                                 f"table => '{procedure_table_arg(table)}', "
                                 f"older_than => TIMESTAMP '{now}', retain_last => {retain_last})")

def remove_orphan_files(spark, table):
    run_query_with_timing(spark, f"CALL {procedure_catalog()}.system.remove_orphan_files(table => '{procedure_table_arg(table)}')")

def rollback_to_snapshot(spark, table, snapshot_id=None):
    if snapshot_id is None:
        print("Please provide a snapshot ID to rollback to.")
        return
    run_query_with_timing(spark, f"CALL {procedure_catalog()}.system.rollback_to_snapshot(table => '{procedure_table_arg(table)}', snapshot_id => {snapshot_id})")

def get_history(spark, table):
    run_query_with_timing(spark, f"SELECT * FROM {table}.history")
    run_query_with_timing(spark, f"SELECT * FROM {table}.snapshots")
    run_query_with_timing(spark, f"SELECT count(*) as file_count FROM {table}.files")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iceberg Table Maintenance")
    parser.add_argument("--compact", action="store_true", help="Rewrite data files and manifests")
    parser.add_argument("--expire", action="store_true", help="Expire every snapshot but the current one")
    parser.add_argument("--orphan", action="store_true", help="Remove orphan files")
    parser.add_argument("--history", action="store_true", help="Show table history, snapshots, and file count")
    parser.add_argument("--rollback", type=int, help="Rollback to a specific snapshot ID")
    parser.add_argument("--table", default="orders_kafka", help="Bare table name")

    args = parser.parse_args()

    spark = build_session("IcebergMaintenance")
    spark.sparkContext.setLogLevel("WARN")
    args.table = table_id(args.table)

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
