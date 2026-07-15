/*-------------------------------------------------------------------------
 *
 * parser_protobuf.h
 *    Public interface for the upb-based Protobuf decoder used by
 *    kafka_fdw. Wire format matches CSV/JSON parsers - each Kafka
 *    payload becomes a set of textual field values in
 *    festate->raw_fields[], which the tuple builder then feeds to
 *    InputFunctionCall().
 *
 * Lifecycle
 * ---------
 *   BeginForeignScan  -> KafkaProtoDecoderOpen()
 *                          - reads proto_desc + proto_message
 *                            table options
 *                          - loads the FileDescriptorSet
 *                          - resolves the message MiniTable
 *                          - builds a PG column -> upb_FieldDef cache
 *   IterateForeignScan -> KafkaReadAttributesProtobuf() (per Kafka message)
 *   EndForeignScan    -> KafkaProtoDecoderClose()
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSER_PROTOBUF_H
#define PARSER_PROTOBUF_H

#include "kafka_fdw.h"

extern KafkaProtoDecoder *KafkaProtoDecoderOpen(Relation rel,
                                                KafkaFdwExecutionState *festate);

extern int KafkaReadAttributesProtobuf(const char *payload,
                                       int payload_len,
                                       KafkaFdwExecutionState *festate,
                                       bool *had_error);

extern void KafkaProtoDecoderClose(KafkaProtoDecoder *dec);

/*
 * Direct-Datum fast path accessors used by the dispatcher in
 * kafka_fdw.c.  When KafkaProtoDatumAvailable(dec, fldnum) returns
 * true, the caller reads the Datum from KafkaProtoDatumGet(...) and
 * skips InputFunctionCall for that column.  All other formats and
 * columns fall through to the normal cstring path unchanged.
 */
extern bool KafkaProtoDatumAvailable(KafkaProtoDecoder *dec, int fldnum);
extern Datum KafkaProtoDatumGet(KafkaProtoDecoder *dec, int fldnum, bool *isnull);

#endif  /* PARSER_PROTOBUF_H */
