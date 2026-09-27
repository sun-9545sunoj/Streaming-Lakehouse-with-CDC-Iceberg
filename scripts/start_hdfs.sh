#!/bin/bash
# Start HDFS (NameNode, DataNode, SecondaryNameNode) on Java 11 and wait until
# it leaves safe mode. YARN is not needed: Spark runs local[4].
#
#   bash scripts/start_hdfs.sh          # start
#   bash scripts/start_hdfs.sh stop     # stop
set -e

export JAVA_HOME="${JAVA_HOME_11:-/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop/hadoop-3.3.6}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

if [ "${1:-start}" = "stop" ]; then
    "$HADOOP_HOME/sbin/stop-dfs.sh"
    exit 0
fi

"$HADOOP_HOME/sbin/start-dfs.sh"
"$HADOOP_HOME/bin/hdfs" dfsadmin -safemode wait
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /warehouse /checkpoint
"$JAVA_HOME/bin/jps" | grep -E "NameNode|DataNode"
echo "HDFS ready at hdfs://localhost:9000"
