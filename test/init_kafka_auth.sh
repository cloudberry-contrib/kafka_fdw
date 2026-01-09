#!/bin/bash

KAFKA_BIN_DIR=~/kafka
ADMIN_CLIENT_FILE=${KAFKA_BIN_DIR}/config/admin-client.properties
CLIENT_JAAS_FILE=${KAFKA_BIN_DIR}/config/admin-client-jaas.conf
PRODUCER_CONFIG_FILE=${KAFKA_BIN_DIR}/config/producer.properties
BROKER_USER=kafka_fdw_user
BROKER_PASS=kafka_fdw_pass

cat >> ${KAFKA_BIN_DIR}/config/producer.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-256

sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
	username="${BROKER_USER}" \
	password="${BROKER_PASS}";
EOF

: ${KAFKA_PRODUCER:="${KAFKA_BIN_DIR}/bin/kafka-console-producer.sh --producer.config ${PRODUCER_CONFIG_FILE}"}
: ${KAFKA_TOPICS:="${KAFKA_BIN_DIR}/bin/kafka-topics.sh --command-config ${ADMIN_CLIENT_FILE}"}

kafka_cmd="$KAFKA_PRODUCER --bootstrap-server localhost:9092 --topic"

export KAFKA_OPTS="-Djava.security.auth.login.config=${CLIENT_JAAS_FILE}"

topics=( auth_basic )

# create topics with partitions
for t in "${topics[@]}"; do $KAFKA_TOPICS --bootstrap-server localhost:9092 --create --topic ${t} & done; wait

$kafka_cmd auth_basic <<-EOF
{"int_val" : 8893920, "text_val" : "correct line", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:42:21"}
EOF
