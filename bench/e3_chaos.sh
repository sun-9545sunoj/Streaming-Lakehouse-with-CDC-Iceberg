#!/bin/bash
export PYTHONUNBUFFERED=1
export HADOOP_USER_NAME=hadoop
export JAVA_HOME=$(pwd)/.venv

mkdir -p results
echo "expected,actual,duplicates,missing,mismatched,snapshots,kills" > results/e3_results.csv

echo "Starting E3 Chaos Test (20 trials)..."

for trial in {1..20}; do
    echo "--- Trial $trial ---"
    
    # 1. Clean up HDFS and Kafka data for the new trial
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic orders.cdc --bootstrap-server localhost:9092 || true
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders.cdc --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
    
    rm -rf data/ledger_e3.jsonl data/expected_state_e3.json
    docker exec hadoop hdfs dfs -rm -r -f /checkpoint/orders_ingest_kafka
    ./.venv/bin/python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.appName('drop').config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0').config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions').config('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog').config('spark.sql.catalog.spark_catalog.type', 'hive').config('spark.sql.catalog.spark_catalog.uri', 'thrift://localhost:9083').config('spark.sql.catalog.spark_catalog.warehouse', 'hdfs://localhost:9000/warehouse').getOrCreate(); spark.sql('DROP TABLE IF EXISTS default.orders_kafka PURGE')" > /dev/null 2>&1
    
    # 2. Start Producer in background (1000 events total)
    ./.venv/bin/python src/producer_kafka.py --rate 100 --interval 0.5 --max-events 1000 > /dev/null 2>&1 &
    PROD_PID=$!
    
    KILLS=0
    
    # 3. Chaos loop
    while kill -0 $PROD_PID 2>/dev/null; do
        ./.venv/bin/python src/ingest_kafka.py > ingest.log 2>&1 &
        INGEST_PID=$!
        
        SLEEP_TIME=$(( (RANDOM % 10) + 3 ))
        echo "Letting ingest run for $SLEEP_TIME seconds..."
        sleep $SLEEP_TIME
        
        if kill -0 $INGEST_PID 2>/dev/null; then
            echo "KILLING ingest process!"
            kill -9 $INGEST_PID
            KILLS=$((KILLS + 1))
        fi
    done
    
    # Let ingest finish the rest of the queue
    echo "Producer finished. Running ingest one last time to drain..."
    # We will use timeout of 30 seconds since streaming runs forever
    timeout 30 ./.venv/bin/python src/ingest_kafka.py > ingest.log 2>&1 || true
    
    echo "Verifying..."
    ./.venv/bin/python src/verify.py | tee -a e3_verify.log
    
    # Add kill count to the last line of CSV
    sed -i '' "\$ s/\$/,${KILLS}/" results/e3_results.csv
done

echo "E3 Chaos testing complete."
