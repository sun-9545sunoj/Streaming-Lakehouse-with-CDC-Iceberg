"""Shared Spark/Iceberg session builder for the E3 pipeline.

The ingestion job, the chaos harness and the verifier must agree on the catalog
or the verifier reads a different table than the one the ingest wrote. They all
build their session here.

Catalog is chosen by the E3_CATALOG environment variable:
  hive   (default) - HiveCatalog over the Thrift metastore, tables as default.<name>
  hadoop           - HadoopCatalog on HDFS, no metastore process needed
"""
import os

from pyspark.sql import SparkSession

HDFS_URL = os.environ.get("E3_HDFS_URL", "hdfs://localhost:9000")
METASTORE_URL = os.environ.get("E3_METASTORE_URL", "thrift://localhost:9083")
CATALOG_TYPE = os.environ.get("E3_CATALOG", "hive")

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0"
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

# Table identifier differs per catalog: the Hive path keeps the original
# default.<name>, the Hadoop path needs its own catalog prefix.
HADOOP_CATALOG = "lh"
HADOOP_NAMESPACE = "e3"


def table_id(name):
    if CATALOG_TYPE == "hadoop":
        return f"{HADOOP_CATALOG}.{HADOOP_NAMESPACE}.{name}"
    return f"default.{name}"


def procedure_catalog():
    """Catalog name to call Iceberg stored procedures against."""
    return HADOOP_CATALOG if CATALOG_TYPE == "hadoop" else "spark_catalog"


def build_session(app_name, with_kafka=False):
    packages = f"{ICEBERG_PKG},{KAFKA_PKG}" if with_kafka else ICEBERG_PKG
    builder = (SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Several interfaces are active on the dev Mac; without this Spark can
        # bind the driver to a link-local IPv6 address and time out talking to
        # itself before the first job starts.
        .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND", "127.0.0.1"))
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
        .config("spark.ui.showConsoleProgress", "false"))

    if CATALOG_TYPE == "hadoop":
        builder = (builder
            .config(f"spark.sql.catalog.{HADOOP_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{HADOOP_CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{HADOOP_CATALOG}.warehouse", f"{HDFS_URL}/warehouse/e3"))
    else:
        builder = (builder
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hive")
            .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
            .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse"))

    return builder.getOrCreate()
