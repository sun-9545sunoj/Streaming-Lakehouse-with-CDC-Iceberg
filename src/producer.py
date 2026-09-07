import os
import json
import time
import random
import uuid
import datetime
import argparse
import signal
import sys
from hdfs import InsecureClient

# Configurations
HDFS_URL = "http://localhost:9870"
HDFS_OUTPUT_DIR = "/cdc/orders"
LOCAL_DATA_DIR = "data"

STATUS_TRANSITIONS = {
    "PLACED": ["PAID", "CANCELLED"],
    "PAID": ["SHIPPED", "REFUNDED"],
    "SHIPPED": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": [],
    "REFUNDED": []
}

REGIONS = ["north", "south", "east", "west"]
CURRENCIES = ["INR", "USD", "EUR", "GBP"]

# In-memory tracking of orders for updates/deletes and final state
active_orders = {}
ledger_file = None

def init_hdfs():
    client = InsecureClient(HDFS_URL, user='hadoop')
    # ensure output directory exists
    try:
        client.makedirs(HDFS_OUTPUT_DIR)
    except Exception as e:
        print(f"Warning creating HDFS dir: {e}")
    return client

def get_current_ts_ms():
    return int(time.time() * 1000)

def get_utc_iso_string():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def generate_new_order():
    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    return {
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "status": "PLACED",
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(CURRENCIES),
        "region": random.choice(REGIONS),
        "updated_at": get_utc_iso_string()
    }

def create_event(op, before, after):
    return {
        "op": op,
        "ts_ms": get_current_ts_ms(),
        "before": before,
        "after": after
    }

def process_batch(client, batch_events, batch_id):
    if not batch_events:
        return
        
    # Write to local ledger
    for event in batch_events:
        ledger_file.write(json.dumps(event) + "\n")
    ledger_file.flush()

    # Write to HDFS as a JSON file
    file_path = f"{HDFS_OUTPUT_DIR}/batch_{batch_id}_{int(time.time())}.json"
    content = "\n".join([json.dumps(e) for e in batch_events])
    
    # We use overwrite=True just in case, but batch names are unique
    client.write(file_path, data=content.encode('utf-8'), overwrite=True)
    print(f"[{get_utc_iso_string()}] Wrote batch {batch_id} with {len(batch_events)} events to HDFS.")

def shutdown_handler(signum, frame):
    print("\nShutting down gracefully...")
    write_expected_state()
    if ledger_file:
        ledger_file.close()
    sys.exit(0)

def write_expected_state():
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    out_path = os.path.join(LOCAL_DATA_DIR, "expected_state.json")
    with open(out_path, "w") as f:
        json.dump(active_orders, f, indent=2)
    print(f"Wrote final state of {len(active_orders)} active orders to {out_path}.")

def main():
    parser = argparse.ArgumentParser(description="Mock CDC Producer")
    parser.add_argument("--rate", type=int, default=10, help="Events per batch")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between batches")
    parser.add_argument("--update-ratio", type=float, default=0.4, help="Probability of an update vs insert")
    parser.add_argument("--delete-ratio", type=float, default=0.05, help="Probability of a delete vs insert")
    args = parser.parse_args()

    # Handle graceful exit
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    global ledger_file
    ledger_file = open(os.path.join(LOCAL_DATA_DIR, "ledger.jsonl"), "a")

    try:
        client = init_hdfs()
    except Exception as e:
        print(f"Failed to connect to HDFS. Is the container running? Error: {e}")
        sys.exit(1)

    batch_id = 0
    print(f"Starting producer... (Press Ctrl+C to stop)")
    
    while True:
        batch_events = []
        for _ in range(args.rate):
            # Decide operation: insert (c), update (u), delete (d)
            # If no active orders, force insert.
            rand_val = random.random()
            
            if active_orders and rand_val < args.delete_ratio:
                # DELETE
                order_id = random.choice(list(active_orders.keys()))
                before = active_orders.pop(order_id)
                event = create_event("d", before, None)
                batch_events.append(event)
                
            elif active_orders and rand_val < (args.update_ratio + args.delete_ratio):
                # UPDATE
                order_id = random.choice(list(active_orders.keys()))
                before = active_orders[order_id].copy()
                
                possible_next = STATUS_TRANSITIONS[before["status"]]
                if not possible_next:
                    continue # Terminal state, skip update
                    
                after = before.copy()
                after["status"] = random.choice(possible_next)
                after["updated_at"] = get_utc_iso_string()
                
                active_orders[order_id] = after
                event = create_event("u", before, after)
                batch_events.append(event)
                
            else:
                # INSERT
                new_order = generate_new_order()
                active_orders[new_order["order_id"]] = new_order
                event = create_event("c", None, new_order)
                batch_events.append(event)

        if batch_events:
            process_batch(client, batch_events, batch_id)
            batch_id += 1

        time.sleep(args.interval)

if __name__ == "__main__":
    main()
