#!/usr/bin/env bash
# Live demo (SPEC section 11), one step per invocation, each under the identity
# banner so every screenshot is self-labelled.
#
#   bash scripts/demo.sh 0     prerequisites: HDFS, Kafka, fresh table, ingest running
#   bash scripts/demo.sh 1-9   the demo steps
#   bash scripts/demo.sh stop  stop the background producer and ingest job
#
# Uses the HadoopCatalog table lh.e3.orders_kafka (E3_CATALOG=hadoop).
set -uo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

export JAVA_HOME="${JAVA_HOME_17:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYSPARK_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP=127.0.0.1
export E3_CATALOG=hadoop
# kafka-python warns about lambda serializers on every send.
export PYTHONWARNINGS=ignore::DeprecationWarning
unset SPARK_HOME SPARK_CONF_DIR PYTHONPATH

PY="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="${TMPDIR:-/tmp}/lakehouse-demo"
mkdir -p "$LOG_DIR"

STEP="${1:-}"
TITLES=(
    "Prerequisites"
    "Producer streams CDC events into Kafka"
    "Live table: status counts move between two queries"
    "Row-level update: cancel one order via Kafka"
    "Small files, history and snapshots"
    "Compaction collapses the file count"
    "Accidental partition delete, then rollback_to_snapshot"
    "Chaos: kill -9 the streaming job, restart, verify"
    "ClickHouse reads the same Iceberg table"
    "E2: where merge-on-read stops paying off"
)

banner() {
    clear
    echo "======================================================================"
    echo " Streaming Lakehouse (CDC -> Kafka -> Spark -> Iceberg on HDFS)"
    echo " Demo step $STEP: ${TITLES[$STEP]}"
    echo " Name: Vivek Gangavarapu      Roll No: 2023BCS0175"
    echo " Date: $(date '+%d-%m-%Y %H:%M:%S')"
    echo "======================================================================"
    echo
}

# Spark/Ivy startup chatter that would otherwise fill every screenshot.
NOISE='Ivy|^\s*::|confs:|artifacts|found org|from central|^\s*-{20,}$|^\s*\|  +(conf|default) |jars for the packages|added as a dependency|NativeCodeLoader|Setting default log level|To adjust logging|WARN|^\s*$'

run() {
    echo "\$ $*"
    "$@" 2>&1 | grep -vE "$NOISE"
    echo
}

sql() {
    # Keep only the statements, their result tables and timings.
    "$PY" src/demo_query.py "$@" 2>&1 | grep -E "^spark-sql>|^[+|]|^\([0-9.]+ s\)"
}

start_ingest() {
    nohup "$PY" src/ingest_kafka.py > "$LOG_DIR/ingest.log" 2>&1 &
    echo $! > "$LOG_DIR/ingest.pid"
    echo "ingest_kafka.py started (pid $(cat "$LOG_DIR/ingest.pid")), waiting for its first batch..."
    until grep -aq "Merged batch" "$LOG_DIR/ingest.log" 2>/dev/null; do sleep 1; done
    grep -a "Merged batch" "$LOG_DIR/ingest.log" | tail -1
}

# Empty topic, table, checkpoint and ground truth. Leaving the topic in place
# makes the new table re-ingest whatever the previous run left behind.
reset_pipeline() {
    echo "\$ recreate topic orders.cdc; python src/reset_e3.py --table orders_kafka"
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --delete --topic orders.cdc \
        --bootstrap-server localhost:9092 >/dev/null 2>&1 || true
    while docker exec kafka /opt/kafka/bin/kafka-topics.sh --list \
            --bootstrap-server localhost:9092 2>/dev/null | grep -qx orders.cdc; do sleep 1; done
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders.cdc --partitions 3 \
        --replication-factor 1 --bootstrap-server localhost:9092
    "$PY" src/reset_e3.py --table orders_kafka 2>&1 | grep -E "^Dropped|^Deleted"
    rm -f data/expected_state_e3.json data/ledger_e3.jsonl
}

case "$STEP" in
0)
    banner
    run bash scripts/start_hdfs.sh
    run bash scripts/start_kafka_docker.sh
    reset_pipeline
    ;;
1)
    banner
    echo "\$ python src/producer_kafka.py --rate 50 --interval 1 --max-events 100000 &"
    nohup "$PY" src/producer_kafka.py --rate 50 --interval 1 --max-events 100000 \
        > "$LOG_DIR/producer.log" 2>&1 &
    echo $! > "$LOG_DIR/producer.pid"
    echo "\$ python src/ingest_kafka.py &"
    start_ingest
    sleep 5
    echo
    echo "\$ tail -5 producer.log"
    tail -5 "$LOG_DIR/producer.log"
    ;;
2)
    banner
    sql "SELECT status, count(*) AS orders FROM {t} GROUP BY status ORDER BY status"
    echo "... 10 seconds later ..."
    sleep 10
    sql "SELECT status, count(*) AS orders FROM {t} GROUP BY status ORDER BY status"
    ;;
3)
    banner
    run "$PY" src/demo_cancel.py
    ;;
4)
    banner
    # Copy-on-write keeps the live file count small; the build-up is in the
    # files every retained snapshot still references.
    sql "SELECT count(*) AS live_data_files, sum(record_count) AS rows, round(avg(file_size_in_bytes)) AS avg_file_bytes FROM {t}.files" \
        "SELECT count(*) AS files_held_by_all_snapshots FROM {t}.all_data_files" \
        "SELECT made_current_at, snapshot_id, parent_id FROM {t}.history ORDER BY made_current_at DESC" \
        "SELECT committed_at, snapshot_id, operation, summary['added-data-files'] AS added_files FROM {t}.snapshots ORDER BY committed_at DESC" --rows 10
    ;;
5)
    banner
    sql "SELECT count(*) AS live_files_before FROM {t}.files" \
        "SELECT count(*) AS all_snapshot_files_before FROM {t}.all_data_files" \
        "SELECT count(*) AS snapshots_before FROM {t}.snapshots" \
        "SELECT region, count(*), sum(amount) FROM {t} GROUP BY region ORDER BY region"
    run "$PY" src/maintain.py --table orders_kafka --compact --expire
    sql "SELECT count(*) AS live_files_after FROM {t}.files" \
        "SELECT count(*) AS all_snapshot_files_after FROM {t}.all_data_files" \
        "SELECT count(*) AS snapshots_after FROM {t}.snapshots" \
        "SELECT region, count(*), sum(amount) FROM {t} GROUP BY region ORDER BY region"
    echo "Measured curve: docs/figures/e1_planning.png"
    open docs/figures/e1_planning.png 2>/dev/null || true
    ;;
6)
    banner
    # Stop ingest first so the only commits in play are the delete and the rollback.
    [ -f "$LOG_DIR/ingest.pid" ] && kill "$(cat "$LOG_DIR/ingest.pid")" 2>/dev/null
    [ -f "$LOG_DIR/producer.pid" ] && kill "$(cat "$LOG_DIR/producer.pid")" 2>/dev/null
    sleep 3
    "$PY" src/demo_query.py "SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC LIMIT 1" 2>/dev/null \
        | grep -oE "[0-9]{12,}" | head -1 > "$LOG_DIR/good_snapshot"
    GOOD=$(cat "$LOG_DIR/good_snapshot")
    echo "Current snapshot before the accident: $GOOD"
    sql "SELECT region, count(*) FROM {t} GROUP BY region ORDER BY region" \
        "DELETE FROM {t} WHERE region = 'north'" \
        "SELECT region, count(*) FROM {t} GROUP BY region ORDER BY region"
    run "$PY" src/maintain.py --table orders_kafka --rollback "$GOOD"
    sql "SELECT region, count(*) FROM {t} GROUP BY region ORDER BY region"
    ;;
7)
    banner
    echo "\$ python src/producer_kafka.py --rate 50 --interval 0.5 --max-events 3000 --seed 7 &"
    reset_pipeline > /dev/null
    "$PY" src/producer_kafka.py --rate 50 --interval 0.5 --max-events 3000 --seed 7 \
        > "$LOG_DIR/producer.log" 2>&1 &
    PROD=$!
    echo "\$ python src/ingest_kafka.py &"
    start_ingest
    sleep 2
    echo "\$ kill -9 $(cat "$LOG_DIR/ingest.pid")      # mid-batch"
    kill -9 "$(cat "$LOG_DIR/ingest.pid")"
    echo "\$ python src/ingest_kafka.py &              # restart from the same checkpoint"
    start_ingest
    wait $PROD
    kill "$(cat "$LOG_DIR/ingest.pid")" 2>/dev/null
    sleep 3
    echo "\$ python src/ingest_kafka.py --drain"
    "$PY" src/ingest_kafka.py --drain > "$LOG_DIR/ingest.log" 2>&1
    run "$PY" src/verify.py --trial 0 --kills 1 --out "$LOG_DIR/demo_verify.csv"
    ;;
8)
    banner
    docker ps --format '{{.Names}}' | grep -qx clickhouse-server || bash scripts/start_clickhouse.sh
    [ -f "$LOG_DIR/ingest.pid" ] && kill "$(cat "$LOG_DIR/ingest.pid")" 2>/dev/null
    [ -f "$LOG_DIR/producer.pid" ] && kill "$(cat "$LOG_DIR/producer.pid")" 2>/dev/null
    # Hive's reader cannot decode zstd on macOS (see scripts/hive/read_iceberg.hql).
    "$PY" src/demo_query.py "ALTER TABLE {t} SET TBLPROPERTIES ('write.parquet.compression-codec'='gzip')" \
        "CALL lh.system.rewrite_data_files(table => 'e3.orders_kafka', options => map('rewrite-all','true'))" > /dev/null 2>&1
    echo "--- Engine 1: Spark ---"
    sql "SELECT region, count(*) AS total_orders, sum(amount) AS total_revenue FROM {t} GROUP BY region ORDER BY total_revenue DESC"
    echo "--- Engine 2: ClickHouse (icebergHDFS, no copy) ---"
    run "$PY" src/query_clickhouse.py --mode "${CH_MODE:-iceberg}"
    echo "--- Engine 3: Hive CLI (location-based Iceberg table) ---"
    docker stop clickhouse-server > /dev/null
    ( set +u  # hive-env.sh expands variables that may be unset
      source "$HOME/hive/hive-env.sh" \
      && HIVE_AUX_JARS_PATH="$HOME/hive/auxlib/iceberg-hive-runtime-1.6.1.jar" \
         hive -f "$PROJECT_DIR/scripts/hive/read_iceberg.hql" 2>&1 \
      | grep -vE "^SLF4J|WARN|log4j|^$|^Time taken|^OK$" | tail -4 )
    ;;
9)
    banner
    echo "\$ results/e2/crossover_summary.csv (2M rows, 20 rounds, no compaction)"
    cut -d, -f1,4,6,7,8,9,10,11 results/e2/crossover_summary.csv | column -t -s,
    echo
    open docs/figures/e2_*p20*.png 2>/dev/null || true
    ;;
stop)
    for f in ingest producer; do
        [ -f "$LOG_DIR/$f.pid" ] && kill "$(cat "$LOG_DIR/$f.pid")" 2>/dev/null && echo "stopped $f"
    done
    ;;
*)
    echo "usage: bash scripts/demo.sh 0|1|2|3|4|5|6|7|8|9|stop"
    exit 1
    ;;
esac
