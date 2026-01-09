#!/usr/bin/env bash

set -e

KAFKA_BIN_DIR=~/kafka

# stop any running kafka/zookeeper instances
if [ -d "${KAFKA_BIN_DIR}" ]; then
	${KAFKA_BIN_DIR}/bin/kafka-server-stop.sh || true
	${KAFKA_BIN_DIR}/bin/zookeeper-server-stop.sh || true
fi

rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper*

rm -rf ${KAFKA_BIN_DIR}
mkdir ${KAFKA_BIN_DIR}

# regress test
PGOPTIONS='-c optimizer=off' make installcheck

# auth test
PGOPTIONS='-c optimizer=off' make installcheck-auth
