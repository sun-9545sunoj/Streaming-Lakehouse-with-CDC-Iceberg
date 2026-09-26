-- Third engine: the Hive CLI reads the Iceberg table that Spark wrote, in place.
--
-- The table lives in an Iceberg HadoopCatalog, not in the Hive metastore, so it
-- is mapped as a location-based table: Hive reads the Iceberg metadata at that
-- path and sees the current snapshot, same as Spark and ClickHouse.
--
-- iceberg-hive-runtime 1.6.1 is the newest release built for Java 8, which the
-- Hive 3.1.3 CLI needs (1.7.x is Java 11 bytecode; Hive 3 support ends at 1.7.2).
--
--   source ~/hive/hive-env.sh
--   hive -f ~/Documents/labs/streaming-lakehouse-cdc-iceberg/scripts/hive/read_iceberg.hql

ADD JAR ${env:HOME}/hive/auxlib/iceberg-hive-runtime-1.6.1.jar;
SET iceberg.engine.hive.enabled = true;

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
