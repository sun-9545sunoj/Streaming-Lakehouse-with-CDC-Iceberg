import json
import argparse
from pyspark.sql import SparkSession

HDFS_URL = "hdfs://localhost:9000"
METASTORE_URL = "thrift://localhost:9083"

def main():
    spark = (SparkSession.builder
        .appName("Verify_E3")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.spark_catalog.uri", METASTORE_URL)
        .config("spark.sql.catalog.spark_catalog.warehouse", f"{HDFS_URL}/warehouse")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    
    # Load expected state
    try:
        with open("data/expected_state_e3.json", "r") as f:
            expected = json.load(f)
    except:
        print("Could not load expected_state_e3.json")
        return
        
    print("Loading Iceberg table...")
    df = spark.sql("SELECT order_id, status, amount, updated_at FROM default.orders_kafka")
    records = df.collect()
    
    actual = {}
    duplicates = 0
    
    for row in records:
        oid = row.order_id
        if oid in actual:
            duplicates += 1
        actual[oid] = {
            "status": row.status,
            "amount": float(row.amount),
            "updated_at": str(row.updated_at).replace(" ", "T") + "Z"
        }
        
    missing = 0
    mismatched = 0
    
    for oid, exp_row in expected.items():
        if oid not in actual:
            missing += 1
        else:
            act = actual[oid]
            # Compare status and amount
            if act["status"] != exp_row["status"] or abs(act["amount"] - exp_row["amount"]) > 0.01:
                mismatched += 1
                
    snapshots = spark.sql("SELECT count(*) FROM default.orders_kafka.snapshots").collect()[0][0]
    
    print(f"--- E3 Verification Results ---")
    print(f"Expected Rows: {len(expected)}")
    print(f"Actual Rows:   {len(actual)}")
    print(f"Duplicates:    {duplicates}")
    print(f"Missing:       {missing}")
    print(f"Mismatched:    {mismatched}")
    print(f"Snapshots:     {snapshots}")
    
    # Output CSV format for chaos script
    with open("results/e3_results.csv", "a") as f:
        f.write(f"{len(expected)},{len(actual)},{duplicates},{missing},{mismatched},{snapshots}\\n")

if __name__ == "__main__":
    main()
