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
# downloads.apache.org only keeps the current releases; older versions are
# moved to archive.apache.org. Try the live mirror first, then fall back to
# the archive so the download keeps working as versions age out.
if [ ! -f "${KAFKA_ARCHIVE}" ]; then
    # Always write to the same target with -O so a partial/aborted first
    # attempt is overwritten rather than leaving a corrupt archive behind
    # (a plain retry would create ${KAFKA_ARCHIVE}.1 and tar would still
    # consume the broken first copy).
    wget -O "${KAFKA_ARCHIVE}" "https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}" || \
    wget -O "${KAFKA_ARCHIVE}" "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"
fi
tar -xzf ${KAFKA_ARCHIVE} -C ${KAFKA_BIN_DIR} --strip-components=1
export PATH="${KAFKA_BIN_DIR}/bin/:$PATH"

# Configuration
echo "advertised.listeners=PLAINTEXT://localhost:9092" >> ${KAFKA_BIN_DIR}/config/server.properties

# Start Zookeeper and Kafka.
#
# Fully detach the daemons from the calling shell's stdin/stdout/stderr.
# If any of these stay connected to the CI step's output pipe, GitHub
# Actions keeps waiting for that pipe to close and the step hangs until it
# is cancelled, even though this script has logically finished. Redirecting
# all three descriptors (and disowning) lets the step complete while the
# brokers keep running in the background.
nohup zookeeper-server-start.sh ${KAFKA_BIN_DIR}/config/zookeeper.properties > /tmp/zookeeper.log 2>&1 < /dev/null &
disown || true
nohup kafka-server-start.sh ${KAFKA_BIN_DIR}/config/server.properties > /tmp/kafka.log 2>&1 < /dev/null &
disown || true
