# Kafka Foreign Data Wrapper for PostgreSQL

[![build](https://github.com/adjust/kafka_fdw/actions/workflows/ci_dockerfile.yml/badge.svg)](https://github.com/adjust/kafka_fdw/actions/workflows/ci_dockerfile.yml)

At this point the project is not yet production ready.
Use with care. Pull requests welcome


A simple  foreign data wrapper for Kafka which allows it to be treated as
a table.

Currently kafka_fdw allows message parsing in csv, json and protobuf format.
More might come in a future release.


## Build

The FDW uses the librdkafka C client library. https://github.com/edenhill/librdkafka
to build against installed librdkafka and postgres run
`make && make install`

### Protobuf support dependency (upb)

The `format 'protobuf'` decoder is built on top of **upb**, the small C
runtime that ships *inside* Protocol Buffers.  upb and its `utf8_range`
dependency are vendored as git submodules under `thirdparty/` and compiled
**straight into `kafka_fdw.so`**, so the extension is fully self-contained:
it has **no `libupb*.so` / `libprotobuf.so` runtime dependency** and needs
nothing extra installed.  Fetch the submodules once before building:

```sh
git submodule update --init thirdparty/upb thirdparty/utf8_range
make && make install
```

The submodules are pinned to the exact commits Protocol Buffers 24.3 uses
(`upb` @ `42cd0893`, `utf8_range` @ `de0b4a8`).  Because we link upb
statically we are *not* coupled to the platform protobuf version; to move
to a newer upb, look up the matching commit in that protobuf release's
`protobuf_deps.bzl` and bump the submodules together.

to run test

`make installcheck`

not this runs an integration test against an asumed running
kafka on localhost:9092 with zookeeper on  localhost:2181
see `test/init_kafka.sh`


## Usage

CREATE SERVER must specify a brokerlist using option `brokers`
```SQL
CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'localhost:9092');
```

CREATE USER MAPPING
```SQL
CREATE USER MAPPING FOR PUBLIC SERVER kafka_server;
```

CREATE FOREIGN TABLE
must specify the two meta columns for partition and offset.
These can be named abritrary just must be specified wich is what using options.
Note offset is a sql reserved keyword so naming a column `offset` needs quotation
when used.
The remaining columns must match the expected csv message format.
For more usage options see test/expected

```
CREATE FOREIGN TABLE kafka_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress', batch_size '30', buffer_delay '100');
```

The offset and partition columns are special.  Due to the way Kafka works, we _should_
specify these on all queries.


## Notes on Supported Formats

### CSV

CSV, like a PostgreSQL relation, represents data as a series of tuples.  In this respect
the mapping is fairly straight forward.  We use position to map to columns.  What CSV lacks'
however is any sort of schema enforcement between rows, to ensure that all values of a
particular column have the same data types, and other schema checks we expect from a relational
database.  For this reason, it is important to ask how much one trusts the schema enforcement
of the writers.  If the schema enforcement is trusted then you can assume that bad data should
throw an error.  But if it is not, then the error handling options documented here should be
used to enforce schema on read and skip but flag malformed rows.

On one side you can use `strict 'true'` if the format will never change and you fully trust
the writer to properly enforce schemas.  If you trust the writer to always be correct and allow
new columns to be added on to the end, however, you should leave this setting off.

If you do not trust the writer and wish to enforce schema on read only, then set a column with
the option junk 'true'` and another with the option `junk_error 'true'`.

## JSON

JSON has many of the same schema validation issues that CSV does but there are tools and standards
to validate and check JSON documents against schema specifications.  Thus the same error handling
recommendations that apply to CSV above apply here.

Mapping JSON fields to the relation fields is somewhat less straight forward than it with CSV.  JSON
objects represent key/value mappings in an arbitrary order.  For JSON we apply a mapping of the
tupple attribute name to the JSON object key name.  For JSON tables one uses the json option to specify
the json property mapped to.

The example in our test script is:

```
CREATE FOREIGN TABLE kafka_test_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)

SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json', batch_size '30', buffer_delay '100');
```

Here you can see that a message on partition 2, with an offset of 53 containing the document:

```
{
   "text_val": "Some arbitrary text, apparently",
   "date_val": "2011-05-04",
   "int_val": 3,
   "time_val": "2011-04-14 22:22:22"
}
```

would be turned into

(2, 13, 3, "Some text, apparently", 2011-05-04, "2011-04-14 22:22:22")

as a row in the above table.

Currently the Kafka FDW does not support series of JSON arrays, only JSON objects.  JSON arrays
in objects can be presented as text or JSON/JSONB fields, however.


## Protobuf

Protobuf messages are decoded with the message schema, not by position.
Each foreign-table column is bound to a field of the proto message via
`OPTIONS (protobuf '<field_name>')` — a top-level field, or a nested one
via a dotted path (see "Nested fields" below); the message type itself is
selected with the table option `proto_message`, and the serialized
`FileDescriptorSet` with `proto_desc`.  The descriptor must first be
registered into the catalog:

```SQL
SELECT kafka_fdw.register_proto_descriptor('/path/to/events.desc');
```

### Generating the descriptor

The `.desc` is a serialized `FileDescriptorSet` produced by `protoc`.
**Always pass `--include_imports`** so the file is self-contained — it
must carry the descriptors of every `.proto` your schema imports
(including the well-known types), because the FDW resolves the whole type
graph from this one file:

```sh
protoc --include_imports \
       --descriptor_set_out=events.desc \
       -I. -I/usr/local/include \
       events.proto
```

If a `.proto` `import`s another file and `--include_imports` is omitted,
the imported types are missing from the `.desc` and registering or
scanning fails with a "missing dependency" error.  With it, imports
(your own files and `google/protobuf/*`) resolve and decode normally —
you can even reach into an imported message type with a dotted path.

```SQL
CREATE FOREIGN TABLE pb_events (
    part       int    OPTIONS (partition 'true'),
    offs       bigint OPTIONS (offset 'true'),
    user_id    bigint            OPTIONS (protobuf 'user_id'),
    amount     double precision  OPTIONS (protobuf 'amount'),
    address    jsonb             OPTIONS (protobuf 'address'),
    tags       text[]            OPTIONS (protobuf 'tags')
)
SERVER kafka_server OPTIONS (
    format        'protobuf',
    topic         'contrib_regress_protobuf',
    proto_message 'com.acme.UserEvent',
    proto_desc    '/path/to/events.desc',
    batch_size    '10', buffer_delay '100');
```

### Type mapping

A scalar field is rendered to text and then fed to the column type's
input function, so the column type is flexible — any type whose input
accepts that text works (e.g. an `int64` field can go to `bigint`,
`numeric` or `text`).  The column types below are the natural/recommended
choice; the "unsigned" note flags values that can exceed the signed range
and therefore need a wider column.

| Protobuf type | Recommended column type | Notes |
|---|---|---|
| `double` | `double precision` | |
| `float` | `real` | |
| `int32`, `sint32`, `sfixed32` | `int` | |
| `int64`, `sint64`, `sfixed64` | `bigint` | |
| `uint32`, `fixed32` | `bigint` | unsigned; exceeds `int` range |
| `uint64`, `fixed64` | `numeric` | unsigned; exceeds `bigint` range |
| `bool` | `bool` | |
| `string` | `text` | |
| `bytes` | `bytea` | any byte value (built directly as a `bytea` datum; `\x` hex on the text/array paths). Inside `jsonb` (map/repeated) bytes are base64, per proto3 JSON |
| `enum` | `int` | integer value, not the label |
| nested `message` | `jsonb` | canonical proto3 JSON (or flatten a field out with a dotted path, below) |
| `repeated <scalar>` | `<scalar>[]` (e.g. `int[]`, `text[]`) | PostgreSQL array |
| `repeated <message>` | `jsonb` | JSON array of objects |
| `map<K,V>` | `jsonb` | JSON object; keys are always strings |
| `google.protobuf.Timestamp` | `timestamptz` | `seconds` + `nanos`, via PostgreSQL's `timestamptz` I/O (microsecond precision) |
| `google.protobuf.Duration` | `interval` | |
| scalar wrappers (`Int32Value`, `StringValue`, `BoolValue`, ...) | the wrapped scalar (`int`, `text`, `bool`, ...) | absent wrapper → SQL `NULL` |
| `Struct`, `Value`, `ListValue`, `Any`, `FieldMask` | `jsonb` | canonical proto3 JSON |

Singular well-known types are decoded to the native PostgreSQL type
above.  (Inside a `repeated`/`map`/`jsonb` column they stay in canonical
JSON form.)

### Nested fields (dotted paths)

A column can bind to a field inside a singular sub-message with a
dot-separated path, so you do not have to route everything through
`jsonb`:

```SQL
    addr_city    text OPTIONS (protobuf 'address.city'),
    addr_country text OPTIONS (protobuf 'address.country')
```

Every path component except the last must be a singular (non-repeated)
message field.  If any sub-message along the path is absent, the column
is SQL `NULL`.

### oneof

Map each `oneof` member to its own column.  Only the member actually set
in a given message decodes to a value; the other members of that `oneof`
decode to SQL `NULL`.  For example, for `oneof payload { int32 as_int;
string as_str; bool as_bool; }`:

```SQL
    as_int  int  OPTIONS (protobuf 'as_int'),
    as_str  text OPTIONS (protobuf 'as_str'),
    as_bool bool OPTIONS (protobuf 'as_bool')
```

### Limitations

* Dotted paths descend through **singular** sub-messages only; you cannot
  index into a `repeated` field or a `map` from a path.  Map those to a
  `jsonb` column and use the JSON operators (`->`, `->>`) instead.
* `enum` values are emitted as their integer, not their symbolic name.
* Map iteration order is not stable; compare individual keys rather than
  the whole `jsonb` object if you need deterministic results.


## Querying

With the defined meta columns you can query like so:

```
SELECT * FROM kafka_test WHERE part = 0 AND offs > 1000 LIMIT 60;
```

Here offs is the offset column. And defaults to  offset beginning.
Without any partition specified all partitions will be scanned.

Querying across partitions could be done as well.

```
SELECT * FROM kafka_test WHERE (part = 0 AND offs > 100) OR (part = 1 AND offs > 300) OR (part = 3 AND offs > 700)
```

## Error handling

The default for consuming kafka data is not very strict i.e. to less columns
will be assumed be NULL and to many will be ignored.
If you don't like this behaviour you can enable strictness via table options
`strict 'true'`. Thus any such column will error out the query.
However invalid or unparsable data e.g. text for numeric data or invalid date
or such will still error out per default. To ignore such data you can pass
`ignore_junk 'true'` as table options and these columns will be set to NULL.
Alternatively you can add table columns with the attributes
`junk 'true'` and / or `junk_error 'true'`. While fetching data kafka_fdw
will then put the whole payload into the junk column and / or the errormessage(s)
into the junk_error column.
see test/sql/junk_test.sql for a usage example.


## Producing

Inserting Data into kafka works with INSERT statements. If you provide the partition
as a values that will be user otherwise kafkas builtin partitioner will select partition.


add partition as a value

```
INSERT INTO kafka_test(part, some_int, some_text)
VALUES
    (0, 5464565, 'some text goes into partition 0'),
    (1, 5464565, 'some text goes into partition 1'),
    (0, 5464565, 'some text goes into partition 0'),
    (3, 5464565, 'some text goes into partition 3'),
    (NULL, 5464565, 'some text goes into partition selected by kafka');
```
use built in partitioner

```
INSERT INTO kafka_test(some_int, some_text)
VALUES
    (5464565, 'some text goes into partition selected by kafka');
```

### Testing

is currently broken I can't manage to have a proper repeatable topic setup

### Development

Although it works when used properly we need way more error handling.
Basically more test are needed for inapproiate usage like
no topic specified, topic doesn't exist, no partition and offsetcolumn defined
wrong format specification and stuff that might come.

### Future

The idea is to make the FDW more flexible in usage

* specify other formats like protobuf or binary

* specify encoding

* optimize performance with check_selective_binary_conversion
    i.e. WHEN just a single column is projected like
        SELECT one_coll FROM forein_table WHERE ...
    we won't need to take the effort to convert all columns

* better cost and row estmate

* some analyze options would be nice

* parallelism
    with multiple partitions we could theoretically consum them
    in parallel
....



