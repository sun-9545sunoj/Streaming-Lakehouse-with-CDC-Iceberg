import clickhouse_connect

def main():
    # Connect to local ClickHouse Docker container
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    
    print("Mapping Iceberg table to ClickHouse...")
    # Using the Iceberg table function to read directly from HDFS/Local
    
    # Use the hdfs table function to read parquet files directly since it's inside Docker
    query = """
    SELECT region, count(*) as total_orders, sum(amount) as total_revenue
    FROM file('data/*/*/*.parquet', 'Parquet')
    GROUP BY region
    ORDER BY total_revenue DESC
    """
    
    print("Executing query...")
    try:
        result = client.query(query)
        print("--- Results ---")
        for row in result.result_rows:
            print(row)
    except Exception as e:
        print(f"Error executing query: {e}")

if __name__ == "__main__":
    main()
