#!/usr/bin/env python3
"""E3 verifier - diffs the Iceberg table against the producer's ground truth.

Checks the four properties SPEC section 9.3 asks for: no duplicates, no loss,
correct final value per key, and that deleted keys really are gone. One CSV row
per trial is appended to results/e3/failure_trials.csv.
"""
import os
import csv
import json
import argparse

from spark_session import build_session, table_id

RESULT_FIELDS = [
    "trial", "seed", "kills", "expected_rows", "actual_rows", "duplicates",
    "missing", "mismatched", "resurrected", "snapshots", "recovery_seconds",
    "checkpoint_enabled", "passed",
]


def load_expected(path):
    """Ground truth written by producer_kafka.py.

    Accepts the flat {order_id: row} shape written by earlier runs as well as
    the current {"active": {...}, "deleted": [...]} shape.
    """
    with open(path, "r") as f:
        data = json.load(f)
    if "active" in data and "deleted" in data:
        return data["active"], set(data["deleted"])
    return data, set()


def normalise_ts(value):
    return str(value).replace(" ", "T").split(".")[0].rstrip("Z") + "Z"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trial", type=int, default=0)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--kills", type=int, default=0)
    parser.add_argument("--recovery-seconds", type=float, default=0.0)
    parser.add_argument("--checkpoint-enabled", default="true",
                        help="false for the deliberately broken control run")
    parser.add_argument("--table", default="orders_kafka")
    parser.add_argument("--expected", default="data/expected_state_e3.json")
    parser.add_argument("--out", default="results/e3/failure_trials.csv")
    args = parser.parse_args()

    if not os.path.exists(args.expected):
        raise SystemExit(f"ground truth {args.expected} not found - did the producer finish?")

    expected, deleted_ids = load_expected(args.expected)

    spark = build_session("Verify_E3")
    spark.sparkContext.setLogLevel("ERROR")
    table = table_id(args.table)

    rows = spark.sql(
        f"SELECT order_id, status, amount, updated_at FROM {table}"
    ).collect()

    actual = {}
    duplicates = 0
    for row in rows:
        if row.order_id in actual:
            duplicates += 1
        actual[row.order_id] = {
            "status": row.status,
            "amount": float(row.amount),
            "updated_at": normalise_ts(row.updated_at),
        }

    missing = 0
    mismatched = 0
    for order_id, exp in expected.items():
        act = actual.get(order_id)
        if act is None:
            missing += 1
            continue
        if (act["status"] != exp["status"]
                or abs(act["amount"] - float(exp["amount"])) > 0.01
                or act["updated_at"] != normalise_ts(exp["updated_at"])):
            mismatched += 1

    # A delete that was lost on recovery leaves the row behind. Without this
    # check a pipeline that drops every 'd' event still reports a clean run.
    resurrected = sum(1 for order_id in deleted_ids if order_id in actual)

    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").collect()[0][0]
    passed = (duplicates == 0 and missing == 0 and mismatched == 0 and resurrected == 0)

    result = {
        "trial": args.trial,
        "seed": args.seed,
        "kills": args.kills,
        "expected_rows": len(expected),
        "actual_rows": len(actual),
        "duplicates": duplicates,
        "missing": missing,
        "mismatched": mismatched,
        "resurrected": resurrected,
        "snapshots": snapshots,
        "recovery_seconds": round(args.recovery_seconds, 2),
        "checkpoint_enabled": args.checkpoint_enabled,
        "passed": passed,
    }

    print("--- E3 Verification Results ---")
    for key, value in result.items():
        print(f"{key:20} {value}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    write_header = not os.path.exists(args.out)
    with open(args.out, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RESULT_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(result)
    print(f"Appended trial {args.trial} to {args.out}")

    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    main()
