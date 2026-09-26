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

run() {
    echo "\$ $*"
    "$@"
    echo
}

sql() {
    "$PY" src/demo_query.py "$@" 2>&1 | grep -vE "WARN|Ivy|::|resolving|found |downloading|artifacts|confs|^\s*$|\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-\-|\|  *(default|conf)|modules in use|jars for the packages|added as a dependency|retrieving"
}

start_ingest() {
    nohup "$PY" src/ingest_kafka.py > "$LOG_DIR/ingest.log" 2>&1 &
    echo $! > "$LOG_DIR/ingest.pid"
    echo "ingest_kafka.py started (pid $(cat "$LOG_DIR/ingest.pid")), waiting for its first batch..."
    until grep -q "Merged batch" "$LOG_DIR/ingest.log" 2>/dev/null; do sleep 1; done
    grep "Merged batch" "$LOG_DIR/ingest.log" | tail -1
}

case "$STEP" in
0)
    banner
    run bash scripts/start_hdfs.sh
    run bash scripts/start_kafka_docker.sh
    run "$PY" src/reset_e3.py --table orders_kafka
    rm -f data/expected_state_e3.json data/ledger_e3.jsonl
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
    sql "SELECT count(*) AS data_files, sum(record_count) AS rows, round(avg(file_size_in_bytes)) AS avg_file_bytes FROM {t}.files" \
        "SELECT made_current_at, snapshot_id, parent_id FROM {t}.history ORDER BY made_current_at DESC" \
        "SELECT committed_at, snapshot_id, operation, summary['added-data-files'] AS added_files FROM {t}.snapshots ORDER BY committed_at DESC" --rows 10
    ;;
5)
    banner
    sql "SELECT count(*) AS data_files_before FROM {t}.files" \
        "SELECT region, count(*), sum(amount) FROM {t} GROUP BY region"
    run "$PY" src/maintain.py --table orders_kafka --compact
    sql "SELECT count(*) AS data_files_after FROM {t}.files" \
        "SELECT region, count(*), sum(amount) FROM {t} GROUP BY region"
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
    "$PY" src/reset_e3.py --table orders_kafka > /dev/null 2>&1
    rm -f data/expected_state_e3.json data/ledger_e3.jsonl
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
    ( source "$HOME/hive/hive-env.sh" \
      && HIVE_AUX_JARS_PATH="$HOME/hive/auxlib/iceberg-hive-runtime-1.6.1.jar" \
         hive -f "$PROJECT_DIR/scripts/hive/read_iceberg.hql" 2>&1 \
      | grep -vE "^SLF4J|WARN|log4j|^$|^Time taken|^OK$" | tail -4 )
    ;;
9)
    banner
    run cat results/e2/crossover_summary.csv
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
