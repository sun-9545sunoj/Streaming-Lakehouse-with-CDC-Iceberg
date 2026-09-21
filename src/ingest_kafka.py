#!/usr/bin/env python3
"""Phase 4 ingestion: Kafka CDC stream -> Iceberg via foreachBatch MERGE INTO."""
import uuid
import argparse

import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.window import Window

from spark_session import build_session, table_id, HDFS_URL

CHECKPOINT_DIR = f"{HDFS_URL}/checkpoint/orders_ingest_kafka"

# amount is parsed as a string and cast to DECIMAL, never through a double:
# binary floating point cannot represent every two-decimal money value exactly,
# and a cent lost in parsing shows up as a mismatch in the E3 verifier.
ORDER_FIELDS = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("region", StringType(), True),
    StructField("updated_at", StringType(), True),
])

CDC_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", ORDER_FIELDS, True),
    StructField("after", ORDER_FIELDS, True),
])


def process_batch(spark, table):
    def handler(df, batch_id):
        if df.isEmpty():
            return

        parsed = df.selectExpr(
            "op as _op",
            "ts_ms",
            "current_timestamp() as _ingest_ts",
            "CASE WHEN op = 'd' THEN before ELSE after END as payload",
        ).selectExpr(
            "_op", "ts_ms", "_ingest_ts",
            "payload.order_id",
            "payload.customer_id",
            "payload.status",
            "CAST(payload.amount AS DECIMAL(12,2)) as amount",
            "payload.currency",
            "payload.region",
            "CAST(payload.updated_at AS TIMESTAMP) as updated_at",
        )

        # A micro-batch can hold several events for one order_id, and MERGE INTO
        # throws when two source rows match one target row. Keep the latest per
        # key by ts_ms.
        window = Window.partitionBy("order_id").orderBy(F.col("ts_ms").desc())
        deduped = (parsed
            .withColumn("rn", F.row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
            # ts_ms is a dedupe input, not a table column; UPDATE SET * and
            # INSERT * expand to the source columns, so it must not survive.
            .drop("ts_ms"))

        deduped.createOrReplaceGlobalTempView("batch_updates_kafka")

        spark.sql(f"""
            MERGE INTO {table} t
            USING global_temp.batch_updates_kafka s ON t.order_id = s.order_id
            WHEN MATCHED AND s._op = 'd' THEN DELETE
            WHEN MATCHED AND s._op IN ('u', 'c') THEN UPDATE SET *
            WHEN NOT MATCHED AND s._op IN ('u', 'c') THEN INSERT *
        """)
        print(f"Merged batch {batch_id}.", flush=True)

    return handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default="orders_kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="orders.cdc")
    parser.add_argument("--broken-checkpoint", action="store_true",
                        help="SPEC 9.3 control run: start from a throwaway checkpoint every "
                             "time, so a restart replays the topic instead of resuming. "
                             "Produces the failure mode exactly-once is supposed to prevent.")
    args = parser.parse_args()

    spark = build_session("StreamingLakehouseIngestKafka", with_kafka=True)
    spark.sparkContext.setLogLevel("WARN")

    table = table_id(args.table)
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        order_id     STRING, customer_id  STRING, status       STRING,
        amount       DECIMAL(12,2), currency     STRING, region       STRING,
        updated_at   TIMESTAMP, _op          STRING, _ingest_ts   TIMESTAMP
    ) USING iceberg PARTITIONED BY (region, days(updated_at))
    TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='134217728')
    """)

    checkpoint = CHECKPOINT_DIR
    if args.broken_checkpoint:
        checkpoint = f"{HDFS_URL}/checkpoint/broken_{uuid.uuid4().hex[:8]}"
        print(f"BROKEN CONTROL RUN: throwaway checkpoint at {checkpoint}")

    stream = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), CDC_SCHEMA).alias("data"))
        .select("data.*"))

    query = (stream.writeStream
        .foreachBatch(process_batch(spark, table))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="5 seconds")
        .start())

    print(f"Streaming {args.topic} -> {table} ... (Ctrl+C to stop)")
    query.awaitTermination()


if __name__ == "__main__":
    main()
