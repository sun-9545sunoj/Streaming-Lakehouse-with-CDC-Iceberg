#!/usr/bin/env python3
"""Demo step 3: cancel one order through Kafka and watch the Iceberg row change.

Reads an order from the table, publishes a CDC 'u' event that sets it to
CANCELLED, then polls until the running ingest job has merged it. Needs
ingest_kafka.py running.
"""
import sys
import json
import time
import datetime
import argparse

from kafka import KafkaProducer

from spark_session import build_session, table_id

POLL_SECONDS = 2
TIMEOUT_SECONDS = 90


def row_for(spark, table, order_id):
    rows = spark.sql(
        f"SELECT order_id, customer_id, status, CAST(amount AS STRING) AS amount, currency, "
        f"region, date_format(updated_at, \"yyyy-MM-dd'T'HH:mm:ss'Z'\") AS updated_at "
        f"FROM {table} WHERE order_id = '{order_id}'"
    ).collect()
    return rows[0].asDict() if rows else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="orders_kafka")
    parser.add_argument("--order-id", help="default: any order that is not already CANCELLED")
    args = parser.parse_args()

    spark = build_session("DemoCancel")
    spark.sparkContext.setLogLevel("ERROR")
    table = table_id(args.table)

    order_id = args.order_id or spark.sql(
        f"SELECT order_id FROM {table} WHERE status <> 'CANCELLED' LIMIT 1"
    ).collect()[0][0]
    before = row_for(spark, table, order_id)
    if before is None:
        print(f"{order_id} not found in {table}")
        return 1
    print(f"BEFORE  {before}")

    after = dict(before, status="CANCELLED",
                 updated_at=datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    event = {"op": "u", "ts_ms": int(time.time() * 1000), "before": before, "after": after}

    producer = KafkaProducer(bootstrap_servers=["localhost:9092"],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                             key_serializer=lambda k: k.encode("utf-8"))
    producer.send("orders.cdc", key=order_id, value=event)
    producer.flush()
    print(f"SENT    op=u {order_id} -> CANCELLED")

    start = time.time()
    while time.time() - start < TIMEOUT_SECONDS:
        spark.catalog.clearCache()
        spark.sql(f"REFRESH TABLE {table}")
        current = row_for(spark, table, order_id)
        if current and current["status"] == "CANCELLED":
            print(f"AFTER   {current}")
            print(f"Row-level update visible in Iceberg after {time.time() - start:.1f} s")
            return 0
        time.sleep(POLL_SECONDS)

    print("Timed out waiting for the merge - is ingest_kafka.py running?")
    return 1


if __name__ == "__main__":
    sys.exit(main())
