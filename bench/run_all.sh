#!/bin/bash
export PYTHONUNBUFFERED=1
export HADOOP_USER_NAME=hadoop
export JAVA_HOME=$(pwd)/.venv

echo "Waiting for E1 to finish..."
wait $(pgrep -f e1_planning.py) || true

echo "Starting E2..."
./.venv/bin/python bench/e2_cow_mor.py > e2_log.txt 2>&1

echo "Plotting..."
./.venv/bin/python bench/plots.py > plot_log.txt 2>&1
