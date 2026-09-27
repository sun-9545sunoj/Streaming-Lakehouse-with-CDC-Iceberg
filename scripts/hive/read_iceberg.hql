-- Third engine: the Hive CLI reads the Iceberg table that Spark wrote, in place.
--
-- The table lives in an Iceberg HadoopCatalog, not in the Hive metastore, so it
-- is mapped as a location-based table: Hive reads the Iceberg metadata at that
-- path and sees the current snapshot, same as Spark and ClickHouse.
--
-- iceberg-hive-runtime 1.6.1 is the newest release built for Java 8, which the
-- Hive 3.1.3 CLI needs (1.7.x is Java 11 bytecode; Hive 3 support ends at 1.7.2).
--
-- Prerequisite: the table's data files must not be zstd. Iceberg 1.10 writes
-- zstd by default, and the Hive reader decodes it through Hadoop's
-- ZStandardCodec, which needs a native libhadoop built with zstd that macOS does
-- not have. Switch the table to gzip (pure Java in Hadoop) and rewrite once:
--   ALTER TABLE lh.e3.orders_kafka SET TBLPROPERTIES ('write.parquet.compression-codec'='gzip');
--   CALL lh.system.rewrite_data_files(table => 'e3.orders_kafka', options => map('rewrite-all','true'));
--
-- The runtime jar goes on HIVE_AUX_JARS_PATH, not ADD JAR: ADD JAR comes too late
-- for the local MapReduce task, which fails with ClassNotFoundException.
--
--   source ~/hive/hive-env.sh
--   HIVE_AUX_JARS_PATH=$HOME/hive/auxlib/iceberg-hive-runtime-1.6.1.jar \
--     hive -f ~/Documents/labs/streaming-lakehouse-cdc-iceberg/scripts/hive/read_iceberg.hql

-- YARN is not running (Spark uses local[4]); run MapReduce inside the CLI JVM.
SET mapreduce.framework.name = local;

SET iceberg.engine.hive.enabled = true;
-- The 1.6.1 vectorised reader NPEs in CompatibilityHiveVectorUtils on Hive 3.1.3.
SET hive.vectorized.execution.enabled = false;

CREATE DATABASE IF NOT EXISTS lakehouse_cmp;
USE lakehouse_cmp;

DROP TABLE IF EXISTS orders_iceberg;
CREATE EXTERNAL TABLE orders_iceberg
STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
LOCATION 'hdfs://localhost:9000/warehouse/e3/e3/orders_kafka'
TBLPROPERTIES ('iceberg.catalog' = 'location_based_table');

SELECT region, count(*) AS total_orders, sum(amount) AS total_revenue
FROM orders_iceberg
GROUP BY region
ORDER BY total_revenue DESC;
