\i test/sql/setup.inc

-- This file contains extra tests for Apache Cloudberry

-- 1.column options 'partition' and 'offset ' are essential
CREATE FOREIGN TABLE invalid_kafka_table (
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_cloudberry', batch_size '30', buffer_delay '500');

select * from invalid_kafka_table;

drop foreign table invalid_kafka_table;

CREATE FOREIGN TABLE invalid_kafka_table (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'false'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_cloudberry', batch_size '30', buffer_delay '500');

select * from invalid_kafka_table;
drop foreign table invalid_kafka_table;

-- 2. insert into a non-exist partition
CREATE FOREIGN TABLE normal_kafka_table (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_cloudberry', batch_size '30', buffer_delay '500');

-- invalid partition
insert into normal_kafka_table(part, some_int, some_text, some_date, some_time) values (10, 1, 'jack', now(), now());

-- valid partition
insert into normal_kafka_table(part, some_int, some_text, some_date, some_time) values (0, 1, 'jack', now(), now());

select count(*) from normal_kafka_table;

drop foreign table normal_kafka_table;
