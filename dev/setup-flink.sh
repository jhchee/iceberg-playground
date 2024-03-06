#!/bin/sh

set -x
FLINK_VERSION=1.17.2
FLINK_HOME=/opt/flink

if [ -d $FLINK_HOME ]; then
    rm -rf $FLINK_HOME
fi

mkdir -p ${FLINK_HOME}

# Download Flink
curl --retry 3 "https://dlcdn.apache.org/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz" -o flink-${FLINK_VERSION}-bin-scala_2.12.tgz \
 && tar xzf flink-${FLINK_VERSION}-bin-scala_2.12.tgz --directory /opt/flink --strip-components 1 \
 && rm -rf flink-${FLINK_VERSION}-bin-scala_2.12.tgz

# Download Iceberg dependencies

# This is needed as /opt/ folder is protected by MacOS
chmod -R 777 ${FLINK_HOME}