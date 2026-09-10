#!/usr/bin/env python3
"""
Rapid batch generator for Phase 2 testing.
Creates many small CDC batch files on HDFS to simulate a streaming workload
that produces hundreds of tiny Iceberg data files after ingestion.
"""
import os
import json
import random
import uuid
import datetime
import time
import argparse

HDFS_OUTPUT_DIR = "/cdc/orders"
LOCAL_DATA_DIR = "data/raw"

REGIONS = ["north", "south", "east", "west"]
CURRENCIES = ["INR", "USD", "EUR", "GBP"]
STATUS_TRANSITIONS = {
    "PLACED": ["PAID", "CANCELLED"],
    "PAID": ["SHIPPED", "REFUNDED"],
    "SHIPPED": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": [],
    "REFUNDED": []
}

active_orders = {}

def ts_iso():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ts_ms():
    return int(time.time() * 1000)

def new_order():
    oid = f"ORD-{uuid.uuid4().hex[:8].upper()}"
    return {
        "order_id": oid,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "status": "PLACED",
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": random.choice(CURRENCIES),
        "region": random.choice(REGIONS),
        "updated_at": ts_iso()
    }

def make_event(op, before, after):
    return {"op": op, "ts_ms": ts_ms(), "before": before, "after": after}

def generate_batch(batch_id, events_per_batch=5):
    events = []
    for _ in range(events_per_batch):
        r = random.random()
        if active_orders and r < 0.05:
            oid = random.choice(list(active_orders.keys()))
            before = active_orders.pop(oid)
            events.append(make_event("d", before, None))
        elif active_orders and r < 0.4:
            oid = random.choice(list(active_orders.keys()))
            before = active_orders[oid].copy()
            nexts = STATUS_TRANSITIONS[before["status"]]
            if not nexts:
                continue
            after = before.copy()
            after["status"] = random.choice(nexts)
            after["updated_at"] = ts_iso()
            active_orders[oid] = after
            events.append(make_event("u", before, after))
        else:
            o = new_order()
            active_orders[o["order_id"]] = o
            events.append(make_event("c", None, o))
    return events

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batches", type=int, default=200, help="Number of batches to generate")
    parser.add_argument("--events", type=int, default=5, help="Events per batch")
    args = parser.parse_args()

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    os.system(f"docker exec hadoop hdfs dfs -mkdir -p {HDFS_OUTPUT_DIR} >/dev/null 2>&1")

    print(f"Generating {args.batches} batches of {args.events} events each...")
    for i in range(args.batches):
        events = generate_batch(i, args.events)
        if not events:
            continue
        
        fname = f"batch_rapid_{i}_{int(time.time())}.json"
        local_path = os.path.join(LOCAL_DATA_DIR, fname)
        hdfs_path = f"{HDFS_OUTPUT_DIR}/{fname}"

        with open(local_path, "w") as f:
            f.write("\n".join(json.dumps(e) for e in events))

        os.system(f"cat {local_path} | docker exec -i hadoop hdfs dfs -put - {hdfs_path}")

        if (i + 1) % 50 == 0:
            print(f"  ... {i + 1}/{args.batches} batches uploaded")

    # Save expected state
    os.makedirs("data", exist_ok=True)
    with open("data/expected_state.json", "w") as f:
        json.dump(active_orders, f, indent=2)
    
    print(f"Done! Generated {args.batches} batches. {len(active_orders)} active orders tracked.")

if __name__ == "__main__":
    main()
