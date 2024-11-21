#!/bin/bash
#
# This script installs and runs Kafka instance on Debian like distributions.
# Dedicated to be run by root in Docker containers.
#
set -ex

# latest stable version
KAFKA_VERSION=3.8.0
KAFKA_ARCHIVE=kafka_2.13-${KAFKA_VERSION}.tgz
KAFKA_BIN_DIR=~/kafka

rm -rf ${KAFKA_BIN_DIR}
mkdir ${KAFKA_BIN_DIR}

DIST=$(cat /etc/os-release | grep ^ID= | sed s/ID=//)

echo

# # Download Apache Kafka
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}
tar -xzf ${KAFKA_ARCHIVE} -C ${KAFKA_BIN_DIR} --strip-components=1
export PATH="${KAFKA_BIN_DIR}/bin/:$PATH"

# Configuration
echo "advertised.listeners=PLAINTEXT://localhost:9092" >> ${KAFKA_BIN_DIR}/config/server.properties

# Start Zookeeper and Kafka
zookeeper-server-start.sh ${KAFKA_BIN_DIR}/config/zookeeper.properties > /tmp/zookeeper.log &
kafka-server-start.sh ${KAFKA_BIN_DIR}/config/server.properties > /tmp/kafka.log &
