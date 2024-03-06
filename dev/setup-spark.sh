#!/bin/sh
set -x
SPARK_VERSION=3.5.1
SPARK_HOME=/opt/spark

if [ -d $SPARK_HOME ]; then
    rm -rf $SPARK_HOME
fi

mkdir -p ${SPARK_HOME} && mkdir -p $SPARK_HOME/spark-events

# Download spark
curl --retry 3 "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Setup default spark config
cp spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf

# This is needed as /opt/ folder is protected by MacOS
chmod -R 777 ${SPARK_HOME}