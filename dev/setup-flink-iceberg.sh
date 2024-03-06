#!/bin/sh

set -x
FLINK_MAJOR_VERSION=1.17
ICEBERG_VERSION=1.4.3
HADOOP_VERSION=2.8.5
FLINK_HOME=/opt/flink

# Download Iceberg dependencies
wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-${FLINK_MAJOR_VERSION}/${ICEBERG_VERSION}/iceberg-flink-runtime-${FLINK_MAJOR_VERSION}-${ICEBERG_VERSION}.jar" -o iceberg-flink-runtime-${FLINK_MAJOR_VERSION}-${ICEBERG_VERSION}.jar \
 -P $FLINK_HOME/lib/ \
 && rm -rf iceberg-flink-runtime-${FLINK_MAJOR_VERSION}-${ICEBERG_VERSION}.jar

wget "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" -o iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 -P $FLINK_HOME/lib/ \
 && rm -rf iceberg-aws-bundle-${ICEBERG_VERSION}.jar

wget "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar" -o hadoop-common-${HADOOP_VERSION}.jar \
 -P $FLINK_HOME/lib/ \
 && rm -rf hadoop-common-${HADOOP_VERSION}.jar
