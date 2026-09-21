#!/usr/bin/env python3
"""
One-file-per-batch ingestion for Phase 2 testing.
Processes one CDC file per micro-batch to create many small Iceberg data files.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"
CHECKPOINT_DIR = f"{HDFS_URL}/checkpoint/orders_ingest_p2"

spark = (SparkSession.builder
    .appName("Phase2Ingest")
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
    .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

CDC_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("region", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True),
    StructField("after", StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("region", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True)
])

spark.sql("CREATE NAMESPACE IF NOT EXISTS default")
spark.sql("""
CREATE TABLE IF NOT EXISTS default.orders (
    order_id     STRING,
    customer_id  STRING,
    status       STRING,
    amount       DECIMAL(12,2),
    currency     STRING,
    region       STRING,
    updated_at   TIMESTAMP,
    _op          STRING,
    _ingest_ts   TIMESTAMP
)
USING iceberg
PARTITIONED BY (region, days(updated_at))
TBLPROPERTIES (
    'format-version'      = '2',
    'write.update.mode'   = 'copy-on-write',
    'write.delete.mode'   = 'copy-on-write',
    'write.merge.mode'    = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
)
""")

batch_counter = [0]

def process_batch(df, batch_id):
    if df.isEmpty():
        return
    count = df.count()
    batch_counter[0] += 1
    
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    parsed_df = df.selectExpr(
        "op as _op", "ts_ms", "current_timestamp() as _ingest_ts",
        "CASE WHEN op = 'd' THEN before ELSE after END as payload"
    ).selectExpr(
        "_op", "ts_ms", "_ingest_ts",
        "payload.order_id", "payload.customer_id", "payload.status",
        "CAST(payload.amount AS DECIMAL(12,2)) as amount",
        "payload.currency", "payload.region",
        "CAST(payload.updated_at AS TIMESTAMP) as updated_at"
    )

    windowSpec = Window.partitionBy("order_id").orderBy(F.col("ts_ms").desc())
    deduped_df = parsed_df.withColumn("rn", F.row_number().over(windowSpec)) \
                          .filter("rn = 1").drop("rn").drop("ts_ms")

    deduped_df.createOrReplaceGlobalTempView("batch_updates")

    spark.sql("""
        MERGE INTO default.orders t
        USING global_temp.batch_updates s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s._op = 'd' THEN DELETE
        WHEN MATCHED AND s._op IN ('u', 'c') THEN UPDATE SET *
        WHEN NOT MATCHED AND s._op IN ('u', 'c') THEN INSERT *
    """)
    
    if batch_counter[0] % 25 == 0:
        print(f"  ... processed {batch_counter[0]} micro-batches")

# KEY: maxFilesPerTrigger=1 ensures one file per micro-batch = many small Iceberg files
stream_df = (spark.readStream
    .format("json")
    .schema(CDC_SCHEMA)
    .option("maxFilesPerTrigger", 1)
    .load(f"{HDFS_URL}/cdc/orders"))

query = (stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="2 seconds")
    .start())

print("Phase 2 ingestion: 1 file per micro-batch... (will auto-stop when all files processed)")
query.awaitTermination()
