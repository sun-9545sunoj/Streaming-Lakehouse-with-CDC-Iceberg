#!/usr/bin/env python3
"""Run SQL statements against the lakehouse and print the results.

`{t}` in a statement expands to the demo table's full identifier, so the same
command works for either catalog:

    python src/demo_query.py "SELECT status, count(*) FROM {t} GROUP BY status"
"""
import sys
import time
import argparse

from spark_session import build_session, table_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sql", nargs="+", help="statements to run, in order")
    parser.add_argument("--table", default="orders_kafka")
    parser.add_argument("--rows", type=int, default=30, help="max rows to show per result")
    args = parser.parse_args()

    spark = build_session("DemoQuery")
    spark.sparkContext.setLogLevel("ERROR")
    table = table_id(args.table)

    for statement in args.sql:
        sql = statement.replace("{t}", table)
        print(f"\nspark-sql> {sql}", flush=True)
        start = time.time()
        df = spark.sql(sql)
        df.show(args.rows, truncate=False)
        print(f"({time.time() - start:.2f} s)", flush=True)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
