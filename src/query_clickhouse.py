#!/usr/bin/env python3
"""Phase 5 - query the same Iceberg table from ClickHouse, no copy, no export.

Two modes:

  iceberg (default) - ClickHouse's icebergHDFS() table function reads the table
      through its Iceberg metadata, so it sees the current snapshot only.
  parquet           - raw Parquet glob over the warehouse directory.

The parquet mode is a fallback for a ClickHouse build without HDFS/Iceberg
support, and it is NOT equivalent: it reads every Parquet file present on disk,
including files that copy-on-write has already superseded and files that
merge-on-read has logically deleted. Until expire_snapshots and
remove_orphan_files have run, its aggregates over-count. Report numbers from
this mode only with that caveat stated.
"""
import argparse

import clickhouse_connect

DEFAULT_TABLE_PATH = "hdfs://host.docker.internal:9000/warehouse/default.db/orders_kafka"

QUERY_TEMPLATE = """
SELECT region, count(*) as total_orders, sum(amount) as total_revenue
FROM {source}
GROUP BY region
ORDER BY total_revenue DESC
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["iceberg", "parquet"], default="iceberg")
    parser.add_argument("--table-path", default=DEFAULT_TABLE_PATH,
                        help="HDFS URI of the Iceberg table directory (iceberg mode)")
    parser.add_argument("--parquet-glob", default="data/*/*/*.parquet",
                        help="path glob inside the ClickHouse user_files dir (parquet mode)")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8123)
    args = parser.parse_args()

    if args.mode == "iceberg":
        source = f"icebergHDFS('{args.table_path}')"
    else:
        source = f"file('{args.parquet_glob}', 'Parquet')"

    client = clickhouse_connect.get_client(host=args.host, port=args.port)
    query = QUERY_TEMPLATE.format(source=source)
    print(f"Querying the lakehouse from ClickHouse ({args.mode} mode)")
    print(query)

    try:
        result = client.query(query)
    except Exception as e:
        print(f"Query failed: {e}")
        if args.mode == "iceberg":
            print("If this build has no HDFS/Iceberg support, rerun with --mode parquet "
                  "and state the over-counting caveat in the report.")
        raise SystemExit(1)

    print("--- Results ---")
    for row in result.result_rows:
        print(row)


if __name__ == "__main__":
    main()
