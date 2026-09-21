#!/usr/bin/env python3
"""Drop the E3 target table and its checkpoint so a trial starts from nothing.

Used by bench/e3_chaos.sh between trials. Previously this was an inline python
-c one-liner in the shell script with its own copy of the Spark config, which
drifted from the ingest job's config.
"""
import argparse

from spark_session import build_session, table_id, HDFS_URL

CHECKPOINT_DIR = f"{HDFS_URL}/checkpoint/orders_ingest_kafka"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="orders_kafka")
    args = parser.parse_args()

    spark = build_session("Reset_E3")
    spark.sparkContext.setLogLevel("ERROR")
    table = table_id(args.table)
    spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    print(f"Dropped {table}")

    # The checkpoint lives outside the table, so dropping the table alone leaves
    # Spark resuming from offsets for data that no longer exists.
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(CHECKPOINT_DIR)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if fs.exists(path):
        fs.delete(path, True)
        print(f"Deleted checkpoint {CHECKPOINT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
