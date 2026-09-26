-- Report section 8: the same CDC operations on a Hive 3 ACID table.
--
-- Run with the course Hive setup (embedded Derby, Java 8):
--   source ~/hive/hive-env.sh
--   hive -f ~/Documents/labs/streaming-lakehouse-cdc-iceberg/scripts/hive/acid_compare.hql
--
-- What to look at in the output: every UPDATE / DELETE / MERGE adds a
-- delta_ or delete_delta_ directory that readers must merge until a compaction
-- runs, and there is no snapshot to time-travel or roll back to.

CREATE DATABASE IF NOT EXISTS lakehouse_cmp;
USE lakehouse_cmp;

DROP TABLE IF EXISTS orders_acid;
CREATE TABLE orders_acid (
    order_id STRING,
    status   STRING,
    amount   DECIMAL(12,2),
    region   STRING
)
CLUSTERED BY (order_id) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

INSERT INTO orders_acid VALUES
    ('ORD-1', 'PLACED', 100.00, 'north'),
    ('ORD-2', 'PLACED', 200.00, 'south'),
    ('ORD-3', 'PLACED', 300.00, 'east');

UPDATE orders_acid SET status = 'SHIPPED' WHERE order_id = 'ORD-1';
DELETE FROM orders_acid WHERE order_id = 'ORD-2';

-- A CDC micro-batch, already deduplicated to one row per key.
DROP TABLE IF EXISTS cdc_batch;
CREATE TABLE cdc_batch (op STRING, order_id STRING, status STRING, amount DECIMAL(12,2), region STRING);
INSERT INTO cdc_batch VALUES
    ('u', 'ORD-3', 'DELIVERED', 300.00, 'east'),
    ('c', 'ORD-4', 'PLACED',    400.00, 'west'),
    ('d', 'ORD-1', NULL,        NULL,   NULL);

MERGE INTO orders_acid t
USING cdc_batch s ON t.order_id = s.order_id
WHEN MATCHED AND s.op = 'd' THEN DELETE
WHEN MATCHED THEN UPDATE SET status = s.status, amount = s.amount
WHEN NOT MATCHED AND s.op <> 'd' THEN INSERT VALUES (s.order_id, s.status, s.amount, s.region);

SELECT * FROM orders_acid ORDER BY order_id;

-- One base/delta directory per write transaction.
dfs -ls /user/hive/warehouse/lakehouse_cmp.db/orders_acid;

SHOW COMPACTIONS;
