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

SERVER_JAAS_FILE=${KAFKA_BIN_DIR}/config/kafka_server_jaas.conf
ADMIN_CLIENT_FILE=${KAFKA_BIN_DIR}/config/admin-client.properties
CLIENT_JAAS_FILE=${KAFKA_BIN_DIR}/config/admin-client-jaas.conf

BROKER_USER=kafka_fdw_user
BROKER_PASS=kafka_fdw_pass

# stop any running kafka/zookeeper instances
if [ -d "${KAFKA_BIN_DIR}" ]; then
	${KAFKA_BIN_DIR}/bin/kafka-server-stop.sh || true
	${KAFKA_BIN_DIR}/bin/zookeeper-server-stop.sh || true
fi

rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper*

rm -rf ${KAFKA_BIN_DIR}
mkdir ${KAFKA_BIN_DIR}

DIST=$(cat /etc/os-release | grep ^ID= | sed s/ID=//)

echo

# # Download Apache Kafka
if [ ! -f "${KAFKA_ARCHIVE}" ]; then
    wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}
fi
tar -xzf ${KAFKA_ARCHIVE} -C ${KAFKA_BIN_DIR} --strip-components=1
export PATH="${KAFKA_BIN_DIR}/bin/:$PATH"

# Configuration

# server.properties
cat >> ${KAFKA_BIN_DIR}/config/server.properties <<EOF
listeners=SASL_PLAINTEXT://0.0.0.0:9092
advertised.listeners=SASL_PLAINTEXT://localhost:9092

security.inter.broker.protocol=SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-256
sasl.enabled.mechanisms=SCRAM-SHA-256
EOF

# server JAAS configuration
cat > ${SERVER_JAAS_FILE} <<EOF
sasl_plaintext.KafkaServer {
	org.apache.kafka.common.security.scram.ScramLoginModule required
	username="${BROKER_USER}"
	password="${BROKER_PASS}";
};
EOF

# Start Zookeeper
zookeeper-server-start.sh ${KAFKA_BIN_DIR}/config/zookeeper.properties > /tmp/zookeeper.log &
sleep 10

# Create scram user
export KAFKA_OPTS="-Djava.security.auth.login.config=${SERVER_JAAS_FILE}"
${KAFKA_BIN_DIR}/bin/kafka-configs.sh --zookeeper localhost:2181 --alter --add-config "SCRAM-SHA-256=[password=${BROKER_PASS}]" --entity-type users --entity-name ${BROKER_USER}

# Start Kafka Server
kafka-server-start.sh ${KAFKA_BIN_DIR}/config/server.properties > /tmp/kafka.log &
sleep 10

# client configuration
cat > ${ADMIN_CLIENT_FILE} <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256

sasl.username=kafka_fdw_user
sasl.password=kafka_fdw_pass
EOF

# client JAAS
cat > ${CLIENT_JAAS_FILE} <<EOF
KafkaClient {
	org.apache.kafka.common.security.scram.ScramLoginModule required
	username="kafka_fdw_user"
	password="kafka_fdw_pass";
};
EOF

unset KAFKA_OPTS
export KAFKA_OPTS="-Djava.security.auth.login.config=${CLIENT_JAAS_FILE}"
${KAFKA_BIN_DIR}/bin/kafka-configs.sh --bootstrap-server localhost:9092 --command-config ${ADMIN_CLIENT_FILE} --describe --entity-type users --entity-name ${BROKER_USER}
