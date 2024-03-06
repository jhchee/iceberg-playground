#!/bin/bash

# Check if the class name argument is provided
if [ -z "$1" ]; then
  echo "Error: Class name not provided."
  echo "Usage: $0 <class-name>"
  exit 1
fi

# Check SPARK HOME is set
if [ -z "$SPARK_HOME" ]; then
  echo "Error: SPARK_HOME not set."
  exit 1
fi

CLASS_NAME="$1"
ARGS="$2"
WORK_DIR="/tmp/spark/iceberg/"
JAR_DIR="$WORK_DIR/spark-java-1.0-SNAPSHOT.jar"
SPARK_VERSION=3.5
ICEBERG_VERSION=1.4.3

# All the packages required for the application
PACKAGES=(
  "org.apache.iceberg:iceberg-spark-runtime-${SPARK_VERSION}_2.12:${ICEBERG_VERSION}"
  "org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}"
  "com.github.javafaker:javafaker:1.0.2"
)
DEPENDENCIES=$(IFS=, ; echo "${PACKAGES[*]}")

rm -rf $WORK_DIR
mkdir -p $WORK_DIR
mvn -q package -DskipTests
cp ./target/spark-java-1.0-SNAPSHOT.jar $JAR_DIR

$SPARK_HOME/bin/spark-submit \
  --master spark://localhost:7077 \
  --deploy-mode client \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=4747 \
  --class $CLASS_NAME \
  --packages $DEPENDENCIES \
  $JAR_DIR \
  $ARGS