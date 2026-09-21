#!/usr/bin/env bash
# E2 runner. Spark 3.5 needs Java 17; the HDFS daemons stay on Java 11 and the
# Hive client on Java 8, so this pins its own JAVA_HOME like every other
# launcher in this project.
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

export JAVA_HOME="${JAVA_HOME_17:-/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home}"

# A Spark source checkout also lives on this machine. If its SPARK_HOME leaks in
# from the shell, pyspark loads that build instead of the pinned 3.5 wheel and
# the Iceberg 3.5_2.12 runtime fails to resolve.
unset SPARK_HOME SPARK_CONF_DIR
export PYSPARK_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$PROJECT_DIR/.venv/bin/python"
export PYTHONUNBUFFERED=1
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

exec ./.venv/bin/python bench/e2_cow_mor.py "$@"
