#!/bin/bash
# Start the Hive 3.1.3 metastore as a standalone Thrift service on 9083
# (SPEC section 6.2), using conf/hive-site.xml.
#
# Hive 3.1.3 needs Java 8. The metastore owns the Derby lock, so no embedded
# Hive CLI session may be open while it runs.
#
# Only the HiveCatalog path needs this (E3 without E3_CATALOG=hadoop). E1, E2
# and the Hadoop-catalog E3 run without it.
set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export JAVA_HOME="${JAVA_HOME_8:-$HOME/java/zulu8.96.0.205-ca-jdk8.0.504-macosx_aarch64/Contents/Home}"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop/hadoop-3.3.6}"
# hadoop-env.sh hard-codes Java 11; the Java 8 copy of the config avoids that.
export HADOOP_CONF_DIR="$HOME/hive/hadoop-conf-java8"
export HIVE_HOME="${HIVE_HOME:-$HOME/hive/apache-hive-3.1.3-bin}"
export HIVE_CONF_DIR="$PROJECT_DIR/conf"

if lsof -iTCP:9083 -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Something is already listening on 9083"
    exit 0
fi

# Derby's database path is absolute in conf/hive-site.xml, but derby.log lands
# in the working directory.
cd "$HOME/hive"
nohup "$HIVE_HOME/bin/hive" --service metastore -p 9083 > "$HOME/hive/tmp/metastore.log" 2>&1 &
echo "Metastore starting (pid $!), log at ~/hive/tmp/metastore.log"

until lsof -iTCP:9083 -sTCP:LISTEN >/dev/null 2>&1; do
    sleep 2
done
echo "Metastore listening on thrift://localhost:9083"
