import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, DoubleType
from pyspark.sql.window import Window

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"
CHECKPOINT_DIR = f"{HDFS_URL}/checkpoint/orders_ingest"

# Iceberg Catalog Configurations
# Since Hive 3.1.3 embedded Derby has a lock issue, we use the Thrift server.
spark = (SparkSession.builder
    .appName("StreamingLakehouseIngest")
    # Iceberg 1.10.x runtime for Spark 3.5.x
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
    # Catalog setup
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hive")
    .config("spark.sql.catalog.lakehouse.uri", METASTORE_URL)
    .config("spark.sql.catalog.lakehouse.warehouse", f"{HDFS_URL}/warehouse")
    .getOrCreate())

# Make sure we don't spam the console too much
spark.sparkContext.setLogLevel("WARN")

# CDC Envelope Schema
CDC_SCHEMA = StructType([
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("before", StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True), # Staging as Double to parse JSON, cast to Decimal later
        StructField("currency", StringType(), True),
        StructField("region", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True),
    StructField("after", StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("region", StringType(), True),
        StructField("updated_at", StringType(), True),
    ]), True)
])

# Initialize the target Iceberg table if it doesn't exist
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS lakehouse
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS lakehouse.orders (
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

def process_batch(df, batch_id):
    print(f"Processing micro-batch {batch_id} with {df.count()} events...")
    if df.isEmpty():
        return
        
    # Deduplicate within the micro-batch:
    # A single batch might have multiple updates for the same order_id.
    # Iceberg MERGE INTO will throw an error if the source has duplicates.
    # We take the latest event per order_id based on ts_ms.
    
    # We extract the effective payload (after for c/u, before for d)
    parsed_df = df.selectExpr(
        "op as _op",
        "ts_ms",
        "current_timestamp() as _ingest_ts",
        "CASE WHEN op = 'd' THEN before ELSE after END as payload"
    ).selectExpr(
        "_op", "ts_ms", "_ingest_ts",
        "payload.order_id",
        "payload.customer_id",
        "payload.status",
        "CAST(payload.amount AS DECIMAL(12,2)) as amount",
        "payload.currency",
        "payload.region",
        "CAST(payload.updated_at AS TIMESTAMP) as updated_at"
    )

    # Deduplicate: sort by ts_ms descending, pick first per order_id
    # Spark SQL doesn't support Window functions natively in Streaming without watermark, 
    # but inside foreachBatch we are dealing with a static DataFrame!
    parsed_df.createOrReplaceTempView("batch_updates_raw")
    
    deduped_df = spark.sql("""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ts_ms DESC) as rn
            FROM batch_updates_raw
        ) WHERE rn = 1
    """).drop("rn", "ts_ms")
    
    deduped_df.createOrReplaceTempView("batch_updates")

    # Perform MERGE INTO Iceberg
    spark.sql("""
        MERGE INTO lakehouse.orders t
        USING batch_updates s
        ON t.order_id = s.order_id
        WHEN MATCHED AND s._op = 'd' THEN DELETE
        WHEN MATCHED AND s._op IN ('u', 'c') THEN UPDATE SET *
        WHEN NOT MATCHED AND s._op IN ('c', 'u') THEN INSERT *
    """)
    print(f"Successfully merged batch {batch_id}.")

# Streaming Query
stream_df = (spark.readStream
    .format("json")
    .schema(CDC_SCHEMA)
    .load(f"{HDFS_URL}/cdc/orders"))

query = (stream_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="5 seconds")
    .start())

print("Starting streaming ingestion from HDFS... (Press Ctrl+C to stop)")
query.awaitTermination()
