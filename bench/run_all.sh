#!/usr/bin/env bash
# Runs E1, then E2, then renders every figure.
#
# E3 is deliberately not in here: it needs Kafka running and takes hours of
# kill-restart cycles, so it is launched on its own with bench/e3_chaos.sh.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Spark needs Java 17. The previous version set JAVA_HOME to ./.venv, a Python
# virtualenv with no JVM in it, and tried to `wait` on a pgrep result, which
# only works for child processes of this shell.
export JAVA_HOME="${JAVA_HOME_17:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"
export PYSPARK_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
unset SPARK_HOME SPARK_CONF_DIR

PYTHON="$PROJECT_DIR/.venv/bin/python"
E1_ROWS="${E1_ROWS:-5000000}"
E2_ROWS="${E2_ROWS:-100000}"
E2_ROUNDS="${E2_ROUNDS:-20}"

echo "=== E1: planning cost vs file count (rows=$E1_ROWS) ==="
"$PYTHON" bench/e1_planning.py --rows "$E1_ROWS" 2>&1 | tee /tmp/e1_run.log

echo "=== E2: COW vs MOR (rows=$E2_ROWS, rounds=$E2_ROUNDS) ==="
"$PYTHON" bench/e2_cow_mor.py --rows "$E2_ROWS" --rounds "$E2_ROUNDS" 2>&1 | tee /tmp/e2_run.log

echo "=== Figures ==="
"$PYTHON" bench/plots.py

echo "Done. CSVs in results/, figures in docs/figures/"
