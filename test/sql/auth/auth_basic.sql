-- start_ignore
drop server if exists kafka_auth_server cascade;
-- end_ignore

CREATE SERVER kafka_auth_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (mpp_execute 'all segments', brokers 'localhost:9092', security_protocol 'SASL_PLAINTEXT', sasl_mechanisms 'SCRAM-SHA-256', sasl_username 'broker', sasl_password 'broker-pass' );

CREATE USER MAPPING FOR PUBLIC SERVER kafka_auth_server;

CREATE FOREIGN TABLE auth_basic_tab1 (
	part int OPTIONS (partition 'true'),
	offs bigint OPTIONS (offset 'true'),
	some_int int OPTIONS (json 'int_val'),
	some_text text OPTIONS (json 'text_val'),
	some_date date OPTIONS (json 'date_val'),
	some_time timestamp OPTIONS (json 'time_val')
)
SERVER kafka_auth_server OPTIONS
(format 'json', topic 'auth_basic', batch_size '30', buffer_delay '500');

select * from auth_basic_tab1;

-- set right username and password
alter server kafka_auth_server options (set sasl_username 'kafka_fdw_user');
alter server kafka_auth_server options (set sasl_password 'kafka_fdw_pass');

select * from auth_basic_tab1;
