import os
import json
import time
import random
import uuid
import datetime
import argparse
import signal
import sys
from kafka import KafkaProducer

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

active_orders = {}
deleted_orders = []
ledger_file = None

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

def shutdown_handler(signum, frame):
    print("\nShutting down gracefully...")
    write_expected_state()
    if ledger_file:
        ledger_file.close()
    sys.exit(0)

def write_expected_state():
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    out_path = os.path.join(LOCAL_DATA_DIR, "expected_state_e3.json")
    # The deleted ids are ground truth too: without them the verifier cannot
    # tell a correctly applied delete from a delete that was lost on recovery.
    state = {"active": active_orders, "deleted": deleted_orders}
    with open(out_path, "w") as f:
        json.dump(state, f, indent=2)
    print(f"Wrote {len(active_orders)} active and {len(deleted_orders)} deleted orders to {out_path}.")

def main():
    parser = argparse.ArgumentParser(description="Mock CDC Producer to Kafka (E3)")
    parser.add_argument("--rate", type=int, default=1000, help="Events per batch")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between batches")
    parser.add_argument("--update-ratio", type=float, default=0.4, help="Probability of an update vs insert")
    parser.add_argument("--delete-ratio", type=float, default=0.05, help="Probability of a delete vs insert")
    parser.add_argument("--max-events", type=int, default=10000, help="Stop after this many events")
    parser.add_argument("--seed", type=int, help="Seed the generator so a trial can be replayed")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    global ledger_file
    ledger_file = open(os.path.join(LOCAL_DATA_DIR, "ledger_e3.jsonl"), "a")

    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8')
        )
    except Exception as e:
        print(f"Failed to connect to Kafka. Error: {e}")
        sys.exit(1)

    batch_id = 0
    total_events = 0
    print(f"Starting producer... (Press Ctrl+C to stop)")
    
    while total_events < args.max_events:
        batch_events = []
        # Calculate how many to generate in this batch
        to_generate = min(args.rate, args.max_events - total_events)
        
        for _ in range(to_generate):
            rand_val = random.random()
            
            if active_orders and rand_val < args.delete_ratio:
                order_id = random.choice(list(active_orders.keys()))
                before = active_orders.pop(order_id)
                deleted_orders.append(order_id)
                event = create_event("d", before, None)
                batch_events.append(event)
                
            elif active_orders and rand_val < (args.update_ratio + args.delete_ratio):
                order_id = random.choice(list(active_orders.keys()))
                before = active_orders[order_id].copy()
                
                possible_next = STATUS_TRANSITIONS[before["status"]]
                if not possible_next:
                    continue 
                    
                after = before.copy()
                after["status"] = random.choice(possible_next)
                after["updated_at"] = get_utc_iso_string()
                
                active_orders[order_id] = after
                event = create_event("u", before, after)
                batch_events.append(event)
                
            else:
                new_order = generate_new_order()
                active_orders[new_order["order_id"]] = new_order
                event = create_event("c", None, new_order)
                batch_events.append(event)

        if batch_events:
            for event in batch_events:
                # Key by order_id. The topic has 3 partitions and Kafka only
                # orders within a partition, so unkeyed events let an update
                # overtake its own create and the merge applies stale state.
                payload = event["after"] or event["before"]
                producer.send('orders.cdc', key=payload["order_id"], value=event)
                ledger_file.write(json.dumps(event) + "\n")
            producer.flush()
            ledger_file.flush()
            total_events += len(batch_events)
            print(f"[{get_utc_iso_string()}] Sent {total_events}/{args.max_events} events to Kafka.")
            batch_id += 1

        time.sleep(args.interval)
        
    print(f"Finished generating {total_events} events.")
    write_expected_state()
    producer.close()
    ledger_file.close()

if __name__ == "__main__":
    main()
