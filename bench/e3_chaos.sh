#!/usr/bin/env bash
# E3 - exactly-once under induced failure (SPEC section 9.3).
#
# Each trial wipes the topic, the checkpoint and the target table, streams a
# fixed workload, and kills the Spark driver at random points while it merges.
# verify.py then diffs the table against the producer's ground truth and appends
# one row per trial to results/e3/failure_trials.csv.
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Spark needs Java 17. The previous version pointed JAVA_HOME at ./.venv, which
# is a Python virtualenv and contains no JVM at all.
export JAVA_HOME="${JAVA_HOME_17:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYSPARK_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-$(whoami)}"
unset SPARK_HOME SPARK_CONF_DIR

PYTHON="$PROJECT_DIR/.venv/bin/python"
TRIALS="${TRIALS:-20}"
EVENTS="${EVENTS:-10000}"
TABLE="${TABLE:-orders_kafka}"
RESULTS="results/e3/failure_trials.csv"
BROKEN_FLAG=""

if [ "${BROKEN_CONTROL:-0}" = "1" ]; then
    # SPEC 9.3 requires showing what the failure looks like when it is NOT
    # exactly-once. This arm restarts from a throwaway checkpoint every time.
    BROKEN_FLAG="--broken-checkpoint"
    RESULTS="results/e3/failure_trials_broken.csv"
    echo "RUNNING THE BROKEN CONTROL ARM - duplicates here are the expected result"
fi

mkdir -p results/e3

echo "Starting E3 chaos test: $TRIALS trials, $EVENTS events each"

for trial in $(seq 1 "$TRIALS"); do
    echo "--- Trial $trial/$TRIALS ---"
    SEED=$((1000 + trial))

    # 1. Clean slate: topic, ground truth, checkpoint, target table.
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic orders.cdc \
        --bootstrap-server localhost:9092 >/dev/null 2>&1 || true
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders.cdc \
        --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 >/dev/null 2>&1

    rm -f data/ledger_e3.jsonl data/expected_state_e3.json
    "$PYTHON" src/reset_e3.py --table "$TABLE" || {
        echo "reset failed, aborting trial $trial"; continue;
    }

    # 2. Producer streams a fixed, seeded workload in the background.
    "$PYTHON" src/producer_kafka.py --rate 100 --interval 0.5 \
        --max-events "$EVENTS" --seed "$SEED" > /tmp/e3_producer.log 2>&1 &
    PROD_PID=$!

    KILLS=0
    RECOVERY_START=$(date +%s)

    # 3. Chaos loop: run the ingest, kill it mid-batch, restart from checkpoint.
    while kill -0 $PROD_PID 2>/dev/null; do
        "$PYTHON" src/ingest_kafka.py --table "$TABLE" $BROKEN_FLAG > /tmp/e3_ingest.log 2>&1 &
        INGEST_PID=$!

        SLEEP_TIME=$(( (RANDOM % 10) + 3 ))
        echo "  ingest running for ${SLEEP_TIME}s..."
        sleep "$SLEEP_TIME"

        if kill -0 $INGEST_PID 2>/dev/null; then
            echo "  kill -9 the driver"
            kill -9 $INGEST_PID 2>/dev/null
            wait $INGEST_PID 2>/dev/null
            KILLS=$((KILLS + 1))
        fi
    done

    # 4. Drain whatever the kills left behind. Streaming never exits on its own,
    #    so this last pass is time-boxed.
    echo "  producer done, draining the topic..."
    DRAIN_START=$(date +%s)
    timeout "${DRAIN_TIMEOUT:-60}" "$PYTHON" src/ingest_kafka.py --table "$TABLE" $BROKEN_FLAG \
        > /tmp/e3_ingest.log 2>&1 || true
    RECOVERY=$(( $(date +%s) - DRAIN_START ))

    # 5. Verify against ground truth. Exit code is the pass/fail for this trial.
    "$PYTHON" src/verify.py --trial "$trial" --seed "$SEED" --kills "$KILLS" \
        --recovery-seconds "$RECOVERY" --table "$TABLE" --out "$RESULTS" \
        --checkpoint-enabled "$([ -n "$BROKEN_FLAG" ] && echo false || echo true)" \
        | tee -a /tmp/e3_verify.log
    if [ "${PIPESTATUS[0]}" -eq 0 ]; then
        echo "  trial $trial PASSED ($KILLS kills, ${RECOVERY}s recovery)"
    else
        echo "  trial $trial FAILED ($KILLS kills)"
    fi
done

echo "E3 chaos testing complete. Results in $RESULTS"
