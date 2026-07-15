\i test/sql/setup.inc
\i test/tmp/desc_path.inc

-- ---------------------------------------------------------------
-- Protobuf format regression test.
--
-- Depends on test/init_kafka.sh having:
--   * created the contrib_regress_protobuf topic
--   * pre-populated it with 3 fixed com.acme.UserEvent messages
--     via test/proto/pb_produce_regress.c
--   * generated test/tmp/events.desc and written its absolute path
--     into test/tmp/desc_path.inc as the psql variable :desc_path
--
-- UserEvent is a "kitchen sink" message; this test covers the full set
-- of types the decoder supports:
--   * every scalar wire type (int32/64, uint32/64, sint32/64,
--     fixed32/64, sfixed32/64, float, double, bool, string, bytes, enum)
--   * nested message -> jsonb, repeated scalar -> array,
--     repeated message -> jsonb, map -> jsonb
--   * oneof (inactive members decode to SQL NULL)
-- ---------------------------------------------------------------

-- --- extension objects live in the kafka_fdw schema ---
SELECT nspname FROM pg_namespace WHERE nspname = 'kafka_fdw';

SELECT attname, format_type(atttypid, atttypmod), attnotnull
  FROM pg_attribute
 WHERE attrelid = 'kafka_fdw.proto_descriptors'::regclass
   AND attnum > 0
 ORDER BY attnum;

-- --- register the FileDescriptorSet (uploads bytes to the catalog) ---
-- Path is absolute and machine-specific, so query only stable columns.
SELECT kafka_fdw.register_proto_descriptor(:'desc_path') IS NULL AS ok;

SELECT count(*) AS n,
       max(octet_length(desc_bytes)) AS bytes
  FROM kafka_fdw.proto_descriptors;

-- --- register with a non-existent path must fail cleanly ---
SELECT kafka_fdw.register_proto_descriptor('/nonexistent/kafka_fdw_regress.desc');

-- --- create the foreign table + consume the 3 pre-produced messages ---
CREATE FOREIGN TABLE pb_events (
    part       int    OPTIONS (partition 'true'),
    offs       bigint OPTIONS (offset 'true'),
    user_id    bigint            OPTIONS (protobuf 'user_id'),
    event_ts   text              OPTIONS (protobuf 'event_ts'),
    event_type text              OPTIONS (protobuf 'event_type'),
    amount     double precision  OPTIONS (protobuf 'amount'),
    is_admin   bool              OPTIONS (protobuf 'is_admin'),
    address    jsonb             OPTIONS (protobuf 'address'),
    tags       text[]            OPTIONS (protobuf 'tags'),
    prev_addrs jsonb             OPTIONS (protobuf 'prev_addrs'),
    counters   jsonb             OPTIONS (protobuf 'counters'),
    -- remaining scalar wire types
    i32        int               OPTIONS (protobuf 'i32'),
    u32        bigint            OPTIONS (protobuf 'u32'),
    u64        numeric           OPTIONS (protobuf 'u64'),
    s32        int               OPTIONS (protobuf 's32'),
    s64        bigint            OPTIONS (protobuf 's64'),
    f32        bigint            OPTIONS (protobuf 'f32'),
    f64        numeric           OPTIONS (protobuf 'f64'),
    sf32       int               OPTIONS (protobuf 'sf32'),
    sf64       bigint            OPTIONS (protobuf 'sf64'),
    flt        real              OPTIONS (protobuf 'flt'),
    blob       bytea             OPTIONS (protobuf 'blob'),
    color      int               OPTIONS (protobuf 'color'),
    -- repeated non-string scalar
    scores     int[]             OPTIONS (protobuf 'scores'),
    -- oneof members
    as_int     int               OPTIONS (protobuf 'as_int'),
    as_str     text              OPTIONS (protobuf 'as_str'),
    as_bool    bool              OPTIONS (protobuf 'as_bool'),
    -- nested field flattening via dotted path
    addr_city    text            OPTIONS (protobuf 'address.city'),
    addr_country text            OPTIONS (protobuf 'address.country'),
    -- well-known types mapped to native PG types
    created_at timestamptz       OPTIONS (protobuf 'created_at'),
    elapsed    interval          OPTIONS (protobuf 'elapsed'),
    opt_int    int               OPTIONS (protobuf 'opt_int'),
    opt_str    text              OPTIONS (protobuf 'opt_str'),
    -- bytes in compound types
    blobs      bytea[]           OPTIONS (protobuf 'blobs'),
    blob_map   jsonb             OPTIONS (protobuf 'blob_map')
)
SERVER kafka_server
OPTIONS (
    format        'protobuf',
    topic         'contrib_regress_protobuf',
    proto_message 'com.acme.UserEvent',
    proto_desc    :'desc_path',
    batch_size    '10',
    buffer_delay  '100'
);

SELECT part, offs, user_id, event_ts, event_type, amount, is_admin
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 5;

-- --- scalar sub-message, repeated scalar, repeated message columns ---
-- Emitted in schema order by upb_JsonEncode, so exact-match is safe.
SELECT offs, address, tags, prev_addrs
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- map column: upb_Map iteration order is not stable, so extract
--- individual keys instead of comparing the full jsonb object ---
SELECT offs,
       counters->'clicks' AS clicks,
       counters->'views'  AS views,
       jsonb_typeof(counters) AS ty
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- all remaining scalar wire types (fixed across rows) ---
-- Values exercise unsigned ranges beyond signed 32/64-bit maxima and
-- zigzag-encoded negatives; identical in every message so one row is
-- enough to lock the decoding.
SELECT i32, u32, u64, s32, s64, f32, f64, sf32, sf64, flt
  FROM pb_events
 WHERE part = 0 AND offs = 0;

-- --- enum (integer value) + bytes (-> bytea) ---
SELECT color, blob
  FROM pb_events
 WHERE part = 0 AND offs = 0;

-- --- repeated non-string scalar -> int[] ---
SELECT offs, scores
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- oneof: each message sets exactly one member; the other two must
--- decode to SQL NULL (offs 0 -> as_int, 1 -> as_str, 2 -> as_bool) ---
SELECT offs, as_int, as_str, as_bool
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- nested field flattening: dotted path reaches into a sub-message ---
SELECT offs, addr_city, addr_country
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- well-known types -> native PG types ---
-- Timestamp -> timestamptz (shown in UTC for a stable, TZ-independent
-- result), Duration -> interval, Int32Value/StringValue -> bare scalar.
-- opt_str is only set in the first message, so it is NULL elsewhere.
SELECT offs,
       created_at AT TIME ZONE 'UTC' AS created_utc,
       elapsed,
       opt_int,
       opt_str
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- bytes in compound types: repeated bytes -> bytea[] (\x hex, must
--- round-trip NUL/high bytes), map<string,bytes> -> jsonb (base64).
--- blobs is ordered; the map is key-extracted since upb_Map order is
--- not stable. ---
SELECT offs,
       blobs,
       blob_map->>'a' AS a_b64,
       blob_map->>'b' AS b_b64,
       jsonb_typeof(blob_map) AS ty
  FROM pb_events
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- column alignment: a parsable column with NO protobuf mapping must
--- decode to SQL NULL without shifting the mapped columns after it
--- (mirrors the CSV/JSON readers, which emit one field per parsable
--- column).  "unmapped" sits between user_id and event_type. ---
CREATE FOREIGN TABLE pb_align (
    part       int    OPTIONS (partition 'true'),
    offs       bigint OPTIONS (offset 'true'),
    user_id    bigint OPTIONS (protobuf 'user_id'),
    unmapped   text,                                 -- no protobuf option
    event_type text   OPTIONS (protobuf 'event_type')
)
SERVER kafka_server
OPTIONS (
    format        'protobuf',
    topic         'contrib_regress_protobuf',
    proto_message 'com.acme.UserEvent',
    proto_desc    :'desc_path',
    batch_size    '10',
    buffer_delay  '100'
);

SELECT offs, user_id, unmapped, event_type
  FROM pb_align
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- bytes column mapped to a text target must carry bytea-hex
--- ("\xHH...") rather than raw octets, so that a downstream consumer
--- can copy-paste or decode('hex') the value without ambiguity.
--- Same protobuf field as pb_events.blob (bytes -> "abc") re-mapped
--- to text; length locks that the emission is the 8-char hex string
--- (2 chars "\x" + 6 chars "616263"), not the 3 raw bytes. ---
CREATE FOREIGN TABLE pb_bytes_as_text (
    part       int    OPTIONS (partition 'true'),
    offs       bigint OPTIONS (offset 'true'),
    blob_text  text   OPTIONS (protobuf 'blob')
)
SERVER kafka_server
OPTIONS (
    format        'protobuf',
    topic         'contrib_regress_protobuf',
    proto_message 'com.acme.UserEvent',
    proto_desc    :'desc_path',
    batch_size    '10',
    buffer_delay  '100'
);

SELECT offs, blob_text, octet_length(blob_text) AS len
  FROM pb_bytes_as_text
 WHERE part = 0 AND offs = 0;

-- --- Regression fixtures still missing (require .proto + producer
--- changes, so tracked here rather than added inline):
---   * proto3 `optional` scalar unset -> SQL NULL (HasPresence gate)
---   * `string` field containing an embedded NUL -> SQL NULL
--- Add these when events.proto grows an `optional int32` / `optional
--- string` and pb_produce_regress emits a NUL-tainted string. ---

-- --- typmod enforcement: a length-constrained column must go through
--- the cstring / InputFunctionCall path (which applies atttypmod), NOT
--- the direct-Datum fast path -- otherwise char(n) would not blank-pad
--- and varchar(n) would not length-check.  event_type -> char(10) must
--- come back blank-padded to 10 octets. ---
CREATE FOREIGN TABLE pb_typmod (
    part int      OPTIONS (partition 'true'),
    offs bigint   OPTIONS (offset 'true'),
    evt  char(10) OPTIONS (protobuf 'event_type')
)
SERVER kafka_server
OPTIONS (
    format        'protobuf',
    topic         'contrib_regress_protobuf',
    proto_message 'com.acme.UserEvent',
    proto_desc    :'desc_path',
    batch_size    '10',
    buffer_delay  '100'
);

-- octet_length counts the stored bytes: char(10) blank-pads to 10, so a
-- 6-char "signup" stored via the cstring path is 10 octets.  The
-- direct-Datum path would store the raw text unpadded (6 octets), so
-- octlen == 10 proves the typmod-aware path was taken.
SELECT offs, octet_length(evt) AS octlen
  FROM pb_typmod
 WHERE part = 0 AND offs >= 0
 ORDER BY offs
 LIMIT 3;

-- --- ALTER OPTIONS still works ---
ALTER FOREIGN TABLE pb_events OPTIONS (SET buffer_delay '200');
