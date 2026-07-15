/*
 * Regression-test protobuf producer for kafka_fdw.
 *
 * Writes THREE fixed com.acme.UserEvent messages to the topic given
 * on the command line (default "contrib_regress_protobuf"). Values
 * are deterministic so protobuf_test.sql can compare exact output.
 *
 * Build: see test/init_kafka.sh - the sh script generates
 * events.pb-c.{h,c} via protoc-c and links against libprotobuf-c +
 * librdkafka.
 */
#include <librdkafka/rdkafka.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "events.pb-c.h"

/*
 * Deterministic fixture.  The compound fields (address, tags,
 * prev_addrs, counters) and every scalar wire type are identical across
 * all three messages so protobuf_test.sql can compare exact output.
 * Only the oneof member differs per message (payload_kind: 0=as_int,
 * 1=as_str, 2=as_bool) so the test can verify that inactive oneof
 * members decode to SQL NULL.
 *
 * (upb_JsonEncode emits message fields in schema order, so the nested /
 * repeated-message output is stable; map order is not, so the SQL sorts
 * jsonb keys before comparing.)
 */
static void
produce_one(rd_kafka_t *rk, rd_kafka_topic_t *rkt,
            int64_t user_id, const char *ts, const char *type,
            double amount, protobuf_c_boolean is_admin, int payload_kind)
{
    Com__Acme__UserEvent ev = COM__ACME__USER_EVENT__INIT;
    ev.user_id    = user_id;
    ev.event_ts   = (char *) ts;
    ev.event_type = (char *) type;
    ev.amount     = amount;
    ev.is_admin   = is_admin;

    /* Nested message: address. */
    Com__Acme__Address addr = COM__ACME__ADDRESS__INIT;
    addr.city    = "Beijing";
    addr.country = "CN";
    ev.address   = &addr;

    /* Repeated scalar: tags. */
    char *tags[] = { "vip", "beta" };
    ev.n_tags = 2;
    ev.tags   = tags;

    /* Repeated message: prev_addrs. */
    Com__Acme__Address prev0 = COM__ACME__ADDRESS__INIT;
    Com__Acme__Address prev1 = COM__ACME__ADDRESS__INIT;
    prev0.city = "Shanghai";  prev0.country = "CN";
    prev1.city = "Tokyo";     prev1.country = "JP";
    Com__Acme__Address *prev_ptrs[] = { &prev0, &prev1 };
    ev.n_prev_addrs = 2;
    ev.prev_addrs   = prev_ptrs;

    /* Map: counters. */
    Com__Acme__UserEvent__CountersEntry ck1 =
        COM__ACME__USER_EVENT__COUNTERS_ENTRY__INIT;
    Com__Acme__UserEvent__CountersEntry ck2 =
        COM__ACME__USER_EVENT__COUNTERS_ENTRY__INIT;
    ck1.key = "clicks"; ck1.value = 3;
    ck2.key = "views";  ck2.value = 7;
    Com__Acme__UserEvent__CountersEntry *counters[] = { &ck1, &ck2 };
    ev.n_counters = 2;
    ev.counters   = counters;

    /* Remaining scalar wire types (fixed values, chosen to exercise
       sign and unsigned ranges beyond the signed 32/64-bit maxima). */
    ev.i32  = -100;
    ev.u32  = 4000000000u;            /* > INT32_MAX  */
    ev.u64  = 18000000000000000000ULL;/* > INT64_MAX  */
    ev.s32  = -12345;                 /* zigzag       */
    ev.s64  = -9876543210LL;          /* zigzag       */
    ev.f32  = 4294967295u;            /* fixed32 max  */
    ev.f64  = 12345678901234567890ULL;/* fixed64 big  */
    ev.sf32 = -2000000000;
    ev.sf64 = -9000000000000000000LL;
    ev.flt  = 3.25f;                  /* exact in binary float */
    ev.blob.data = (uint8_t *) "abc"; /* bytes -> bytea \x616263 */
    ev.blob.len  = 3;
    ev.color = COM__ACME__COLOR__GREEN;   /* enum -> integer 2 */

    /* Repeated non-string scalar: scores -> int[]. */
    int32_t scores[] = { 10, 20, 30 };
    ev.n_scores = 3;
    ev.scores   = scores;

    /* oneof payload: exactly one member set per message. */
    switch (payload_kind)
    {
        case 0:
            ev.payload_case = COM__ACME__USER_EVENT__PAYLOAD_AS_INT;
            ev.as_int = 42;
            break;
        case 1:
            ev.payload_case = COM__ACME__USER_EVENT__PAYLOAD_AS_STR;
            ev.as_str = "hello";
            break;
        case 2:
            ev.payload_case = COM__ACME__USER_EVENT__PAYLOAD_AS_BOOL;
            ev.as_bool = 1;
            break;
    }

    /* Well-known types: fixed values so the native (timestamptz /
       interval / scalar) decoding can be locked exactly. */
    Google__Protobuf__Timestamp created = GOOGLE__PROTOBUF__TIMESTAMP__INIT;
    created.seconds = 1600000000;   /* 2020-09-13T12:26:40Z */
    created.nanos   = 250000000;    /* .25s -> .250000 us   */
    ev.created_at   = &created;

    Google__Protobuf__Duration elapsed = GOOGLE__PROTOBUF__DURATION__INIT;
    elapsed.seconds = 90;
    elapsed.nanos   = 500000000;    /* 90.5 seconds */
    ev.elapsed      = &elapsed;

    Google__Protobuf__Int32Value oi = GOOGLE__PROTOBUF__INT32_VALUE__INIT;
    oi.value    = 7;
    ev.opt_int  = &oi;              /* wrapper present in every message */

    /* opt_str is a StringValue wrapper set only in the first message, so
       the test can verify that an absent wrapper decodes to SQL NULL. */
    Google__Protobuf__StringValue os = GOOGLE__PROTOBUF__STRING_VALUE__INIT;
    os.value = (char *) "present";
    if (payload_kind == 0)
        ev.opt_str = &os;

    /* repeated bytes -> bytea[]: elements carry NUL / high bytes that the
       raw-octet path could not round-trip, exercising the \x hex path. */
    uint8_t blob0[] = { 0x00, 0x01, 0x02 };
    uint8_t blob1[] = { 0xfe, 0xff };
    ProtobufCBinaryData blobs[2];
    blobs[0].len = sizeof(blob0); blobs[0].data = blob0;
    blobs[1].len = sizeof(blob1); blobs[1].data = blob1;
    ev.n_blobs = 2;
    ev.blobs   = blobs;

    /* map<string,bytes> -> jsonb: values are base64 (proto3 canonical);
       "a" = {0x00} -> "AA==", "b" = {0xff} -> "/w==". */
    uint8_t bm_a[] = { 0x00 };
    uint8_t bm_b[] = { 0xff };
    Com__Acme__UserEvent__BlobMapEntry bme0 =
        COM__ACME__USER_EVENT__BLOB_MAP_ENTRY__INIT;
    Com__Acme__UserEvent__BlobMapEntry bme1 =
        COM__ACME__USER_EVENT__BLOB_MAP_ENTRY__INIT;
    bme0.key = "a"; bme0.value.len = sizeof(bm_a); bme0.value.data = bm_a;
    bme1.key = "b"; bme1.value.len = sizeof(bm_b); bme1.value.data = bm_b;
    Com__Acme__UserEvent__BlobMapEntry *blob_map[2] = { &bme0, &bme1 };
    ev.n_blob_map = 2;
    ev.blob_map   = blob_map;

    size_t   len = com__acme__user_event__get_packed_size(&ev);
    uint8_t *buf = malloc(len);
    com__acme__user_event__pack(&ev, buf);

    if (rd_kafka_produce(rkt, 0 /* partition 0 */,
                         RD_KAFKA_MSG_F_COPY,
                         buf, len, NULL, 0, NULL) == -1)
    {
        fprintf(stderr, "produce failed: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        exit(1);
    }
    free(buf);
}

int
main(int argc, char **argv)
{
    const char *brokers = "localhost:9092";
    const char *topic   = (argc > 1) ? argv[1] : "contrib_regress_protobuf";
    char        errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "conf: %s\n", errstr);
        return 1;
    }

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf,
                                  errstr, sizeof(errstr));
    if (!rk) { fprintf(stderr, "producer: %s\n", errstr); return 1; }

    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt) { fprintf(stderr, "topic_new failed\n"); return 1; }

    /* Three deterministic UserEvent messages; each activates a
       different oneof member (0=as_int, 1=as_str, 2=as_bool). */
    produce_one(rk, rkt, 10001, "2026-07-08 10:00:00", "signup",   0.0,   0, 0);
    produce_one(rk, rkt, 10002, "2026-07-08 10:00:01", "purchase", 42.5,  0, 1);
    produce_one(rk, rkt, 10003, "2026-07-08 10:00:02", "refund",   -7.25, 1, 2);

    rd_kafka_flush(rk, 10000);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    fprintf(stderr, "kafka_fdw regress: produced 3 protobuf messages to %s\n",
            topic);
    return 0;
}
