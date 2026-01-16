-- start_ignore
CREATE SERVER IF NOT EXISTS kafka_auth_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (mpp_execute 'all segments', brokers 'localhost:9092', security_protocol 'SASL_PLAINTEXT', sasl_mechanisms 'SCRAM-SHA-256', sasl_username 'broker', sasl_password 'broker-pass' );

CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER kafka_auth_server;

-- set right username and password
alter server kafka_auth_server options (set sasl_username 'kafka_fdw_user');
alter server kafka_auth_server options (set sasl_password 'kafka_fdw_pass');
-- end_ignore

CREATE FOREIGN TABLE kafka_encoded_data (
	part int OPTIONS (partition 'true'),
	offs bigint OPTIONS (offset 'true'),
	kefkakey text OPTIONS (json 'Kafka-Key'),
	kefkaval jsonb OPTIONS (json 'Kafka-Value')
)
SERVER kafka_auth_server OPTIONS
(format 'json', topic 'encoded_data', batch_size '1000', buffer_delay '1000', hex_decode 'true');

select kefkakey, kefkaval from kafka_encoded_data;

select
	(kefkaval -> 'data' ->> 'id')::int                     as id,
	kefkaval -> 'data' ->> 'train_id'                      as train_id,
	kefkaval -> 'data' ->> 'trip_number'                   as trip_number,
	(kefkaval -> 'data' ->> 'operation_date')::date        as operation_date,
	(kefkaval -> 'data' ->> 'actual_departure_time')::timestamp as actual_departure_time,
	(kefkaval -> 'data' ->> 'actual_arrival_time')::timestamp   as actual_arrival_time,
	(kefkaval -> 'data' ->> 'actual_duration')::int        as actual_duration,
	(kefkaval -> 'data' ->> 'delay_departure')::int        as delay_departure,
	(kefkaval -> 'data' ->> 'delay_arrival')::int          as delay_arrival,
	(kefkaval -> 'data' ->> 'actual_distance')::int        as actual_distance,
	(kefkaval -> 'data' ->> 'avg_speed')::numeric          as avg_speed,
	(kefkaval -> 'data' ->> 'max_speed')::numeric          as max_speed,
	(kefkaval -> 'data' ->> 'energy_consumption')::numeric as energy_consumption,
	(kefkaval -> 'data' ->> 'passenger_count')::int        as passenger_count,
	(kefkaval -> 'data' ->> 'load_factor')::numeric        as load_factor,
	kefkaval -> 'data' ->> 'equipment_status'              as equipment_status,
	(kefkaval -> 'data' ->> 'fault_count')::int            as fault_count,
	kefkaval -> 'data' ->> 'weather_condition'             as weather_condition,
	(kefkaval -> 'data' ->> 'temperature')::numeric        as temperature,
	kefkaval -> 'data' ->> 'driver_id'                     as driver_id,
	(kefkaval -> 'data' ->> 'record_time')::timestamp      as record_time
from kafka_encoded_data
order by (kefkaval -> 'data' ->> 'id')::int;
