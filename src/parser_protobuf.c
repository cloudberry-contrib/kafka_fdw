/*-------------------------------------------------------------------------
 *
 * parser_protobuf.c
 *	  upb-based Protobuf decoder for kafka_fdw.
 *
 * Foreign tables with format='protobuf' must supply two table options:
 * proto_desc, the path to a serialized FileDescriptorSet produced by
 * "protoc --descriptor_set_out=x.desc x.proto", and proto_message,
 * the fully-qualified name of the message type to decode.
 *
 * The .desc bytes are cached in kafka_fdw.proto_descriptors (see
 * kafka_fdw_register_proto_descriptor) and loaded into a upb DefPool
 * at BeginForeignScan; any message type declared by the descriptor
 * can then be decoded without recompiling kafka_fdw.
 *
 * The decoder fills festate->attribute_buf and festate->raw_fields[]
 * with the text representation of each protobuf field, mirroring the
 * CSV/JSON readers; downstream InputFunctionCall() then turns the
 * cstrings into typed Datums.
 *
 * IDENTIFICATION
 *	  kafka_fdw/src/parser_protobuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "parser_protobuf.h"
#include "kafka_fdw.h"

#include <math.h>
#include <time.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "common/base64.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "upb/base/status.h"
#include "upb/base/string_view.h"
#include "upb/mem/arena.h"
#include "upb/message/accessors.h"
#include "upb/collections/array.h"
#include "upb/collections/map.h"
#include "upb/message/message.h"
#include "upb/collections/message_value.h"
#include "upb/mini_table/field.h"
#include "upb/mini_table/message.h"
#include "upb/reflection/def.h"
#include "upb/reflection/def_pool.h"
#include "upb/reflection/field_def.h"
#include "upb/reflection/file_def.h"
#include "upb/reflection/message.h"
#include "upb/reflection/message_def.h"
#include "upb/wire/decode.h"
#include "upb/json/encode.h"
#include "google/protobuf/descriptor.upb.h"

PG_FUNCTION_INFO_V1(kafka_fdw_register_proto_descriptor);

/*
 * The FileDescriptorSet MiniTable (google_protobuf_FileDescriptorSet_msg_init,
 * a single-underscore data symbol in upb 24.x) comes from descriptor.upb.h
 * above; individual FileDescriptorProto values are passed opaque into
 * upb_DefPool_AddFile().
 */
struct google_protobuf_FileDescriptorProto;

/*
 * A foreign-table column maps to a chain of protobuf fields so that
 * dotted paths like OPTIONS (protobuf 'address.city') can reach into
 * nested sub-messages.  steps[0] is resolved against the top-level
 * message; each non-final step must be a singular sub-message, and
 * steps[len-1] is the leaf field actually rendered.  A plain top-level
 * field is just a path of length 1.
 */
#define KAFKA_PROTO_MAX_PATH 16

typedef struct KafkaProtoFieldPath
{
	const upb_FieldDef *steps[KAFKA_PROTO_MAX_PATH];
	/*
	 * Parallel MiniTableField pointers cached at Open time.  The scan
	 * loop drives its scalar fast path off these -- accessors like
	 * upb_Message_GetInt64(msg, mtf, 0) resolve to a direct hasbits +
	 * offset load, whereas the FieldDef variants above walk the def
	 * graph twice per field per row.  mt_steps[i] is upb_FieldDef_
	 * MiniTable(steps[i]); NULL when steps[i] is NULL (unmapped col).
	 */
	const upb_MiniTableField *mt_steps[KAFKA_PROTO_MAX_PATH];
	int					len;
} KafkaProtoFieldPath;

/*
 * For every mapped column whose (proto scalar type, PG atttype) pair maps
 * cleanly to a PG Datum, we skip the "sprintf into cstring, then InputFunctionCall
 * to parse it back" round-trip and build the Datum in-line during the
 * scan pass.  The dispatcher in kafka_fdw.c consults datum_cols[fldnum].ok
 * per column; when set, it reads datum_values/datum_nulls instead of
 * running the fmgr loop.
 *
 * datum_cols[].ok is filled once in KafkaProtoDecoderOpen and stays
 * constant across the whole scan (schema and column types don't change).
 * datum_values / datum_nulls are filled per row by the scan loop.
 */
typedef struct KafkaProtoDatumCol
{
	bool	ok;				/* direct-Datum path applies to this column */
	Oid		pg_type;		/* target PG type OID; used for on-the-fly Datum shape */
	int32	pg_typmod;		/* atttypmod (for varchar length, numeric precision, ...) */
} KafkaProtoDatumCol;

typedef struct KafkaProtoDecoder
{
	upb_DefPool			 *pool;			 /* long-lived, holds all defs */
	upb_Arena			 *desc_arena;	 /* holds the parsed .desc buffer */
	const upb_MessageDef *msg_def;
	const upb_MiniTable	 *mini_table;	 /* cached from msg_def */
	KafkaProtoFieldPath	 *paths;		 /* dense, in attnum order */
	int					  nfields;
	upb_Arena			 *decode_arena;	 /* reset per message */

	/* direct-Datum fast path, see comment above KafkaProtoDatumCol. */
	KafkaProtoDatumCol	 *datum_cols;	 /* nfields entries; ok=false ⇒ cstring path */
	Datum				 *datum_values;	 /* nfields entries, valid iff datum_cols[i].ok */
	bool				 *datum_nulls;	 /* same */
	bool				  any_datum;	/* any datum_cols[i].ok true (cheap dispatcher gate) */

	/*
	 * Per-message arena scratch.  upb_Arena_Init(mem, n, alloc) uses this
	 * buffer as the arena's initial block and only falls back to
	 * upb_pg_allocator (-> palloc) when a message exceeds it.  Sized at
	 * 4 KiB from measurement: mix100 (100 flat scalars incl. u64/string)
	 * reports upb_Arena_SpaceAllocated() = 2400 B/message, so 4 KiB gives
	 * ~70% headroom before the slow-malloc path re-engages.  Aligned to
	 * 16 B for the arena's own alignment requirement.
	 */
#define KAFKA_PROTO_ARENA_SCRATCH_SIZE 4096
	char				  arena_scratch[KAFKA_PROTO_ARENA_SCRATCH_SIZE]
	                          __attribute__((aligned(16)));
} KafkaProtoDecoder;

static bool field_to_text(const upb_FieldDef *fd, upb_MessageValue val,
                          const upb_DefPool *pool, StringInfo out);
static bool field_to_text_scalar(const upb_Message *msg,
                                 const upb_MiniTableField *mtf,
                                 StringInfo out);
static Datum pb_extract_datum(const upb_Message *msg,
                              const upb_MiniTableField *mtf,
                              Oid pg_type);
static bool emit_wellknown_type(StringInfo out, const upb_Message *m,
                                const upb_MessageDef *md, const upb_DefPool *pool);
static void *upb_pg_alloc(upb_alloc *alloc, void *ptr,
                          size_t oldsize, size_t size);

static upb_alloc upb_pg_allocator = { .func = upb_pg_alloc };

/*
 * read_desc_file
 *		Read a .desc file into a palloc'd buffer.
 *
 * Reports the file's mtime to the caller as well.  All I/O failures
 * are turned into ereport(ERROR).
 */
static void
read_desc_file(const char *path, char **out_buf, size_t *out_len, time_t *out_mtime)
{
    int         fd;
    int         save;
    struct stat st;
    ssize_t     got;
    char       *buf;

    fd = open(path, O_RDONLY);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open protobuf descriptor \"%s\": %m", path)));
    if (fstat(fd, &st) < 0)
    {
        save = errno;
        close(fd);
        errno = save;
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not stat protobuf descriptor \"%s\": %m", path)));
    }

    buf = (char *) palloc(st.st_size);
    got = read(fd, buf, st.st_size);
    close(fd);
    if (got != st.st_size)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("short read on protobuf descriptor \"%s\"", path)));

    *out_buf = buf;
    *out_len = (size_t) st.st_size;
    *out_mtime = st.st_mtime;
}

/*
 * kafka_fdw_register_proto_descriptor
 *		SQL-callable: cache the given .desc file into
 *		kafka_fdw.proto_descriptors so QE segments can read it via
 *		a local index probe.
 */
Datum
kafka_fdw_register_proto_descriptor(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *bytes;
	size_t		len;
	time_t		mtime = 0;
	bytea	   *desc_bytea;
	Oid			argtypes[3];
	Datum		argvals[3];
	int			ret;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("kafka_fdw: register_proto_descriptor requires a non-NULL path")));

	path = text_to_cstring(PG_GETARG_TEXT_PP(0));

	read_desc_file(path, &bytes, &len, &mtime);

	/* Wrap the raw bytes in a bytea for the SPI parameter. */
	desc_bytea = (bytea *) palloc(VARHDRSZ + len);
	SET_VARSIZE(desc_bytea, VARHDRSZ + len);
	memcpy(VARDATA(desc_bytea), bytes, len);

    argtypes[0] = TEXTOID;
    argtypes[1] = BYTEAOID;
    argtypes[2] = TIMESTAMPTZOID;

    argvals[0] = CStringGetTextDatum(path);
    argvals[1] = PointerGetDatum(desc_bytea);
    argvals[2] = TimestampTzGetDatum(time_t_to_timestamptz(mtime));

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("kafka_fdw: SPI_connect failed")));

    ret = SPI_execute_with_args(
        "INSERT INTO kafka_fdw.proto_descriptors (path, desc_bytes, mtime) "
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (path) DO UPDATE SET "
        "  desc_bytes = EXCLUDED.desc_bytes, "
        "  mtime      = EXCLUDED.mtime, "
        "  updated_at = now()",
        3, argtypes, argvals, NULL, false /* read_only */, 0);

    if (ret != SPI_OK_INSERT && ret != SPI_OK_INSERT_RETURNING &&
        ret != SPI_OK_UPDATE && ret != SPI_OK_UPDATE_RETURNING)
        ereport(ERROR,
                (errmsg("kafka_fdw: failed to cache descriptor \"%s\" "
                        "(SPI rc=%d)", path, ret)));

    SPI_finish();
    pfree(bytes);
    pfree(desc_bytea);

    PG_RETURN_VOID();
}

/*
 * read_desc_from_catalog
 *		Fetch a cached FileDescriptorSet from kafka_fdw.proto_descriptors.
 *
 * kafka_fdw_register_proto_descriptor() populates this table on the
 * QD.  Because it is DISTRIBUTED REPLICATED, every segment holds a
 * local copy and this lookup is a plain local index probe.
 * A missing row means the QD upload has not
 * happened yet - the caller must run kafka_fdw.register_proto_descriptor
 * or re-set the proto_desc option.
 */
static void
read_desc_from_catalog(const char *path, char **out_buf, size_t *out_len)
{
    Oid         nsoid;
    Oid         relid;
    Relation    rel;
    SysScanDesc scan;
    ScanKeyData key;
    HeapTuple   tup;
    TupleDesc   tupdesc;
    Datum       bytesDatum;
    bool        isnull;
    bytea      *raw;

    nsoid = get_namespace_oid("kafka_fdw", true /* missing_ok */);
    if (!OidIsValid(nsoid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_SCHEMA),
                 errmsg("schema \"kafka_fdw\" not found"),
                 errhint("Run CREATE EXTENSION kafka_fdw or "
                         "ALTER EXTENSION kafka_fdw UPDATE.")));

    relid = get_relname_relid("proto_descriptors", nsoid);
    if (!OidIsValid(relid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_TABLE),
                 errmsg("kafka_fdw.proto_descriptors relation not found"),
                 errhint("Run CREATE EXTENSION kafka_fdw or "
                         "ALTER EXTENSION kafka_fdw UPDATE.")));

    rel     = table_open(relid, AccessShareLock);
    tupdesc = RelationGetDescr(rel);

    /* WHERE path = $1; path is attnum 1. */
    ScanKeyInit(&key,
                (AttrNumber) 1,
                BTEqualStrategyNumber,
                F_TEXTEQ,
                CStringGetTextDatum(path));

    /*
     * Passing indexOK = true lets systable_beginscan pick the primary
     * key index automatically.
     */
    scan = systable_beginscan(rel, InvalidOid, true, NULL, 1, &key);

    tup = systable_getnext(scan);
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("protobuf descriptor for \"%s\" is not cached in kafka_fdw.proto_descriptors",
                        path),
                 errhint("Call kafka_fdw.register_proto_descriptor('%s') "
                         "before querying the foreign table.", path)));

    /* desc_bytes is attnum 2. */
    bytesDatum = heap_getattr(tup, 2, tupdesc, &isnull);
    if (isnull)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("desc_bytes is NULL for path \"%s\"", path)));

    /*
     * Copy the bytes out of the scan buffer before ending the scan;
     * the pointer would otherwise be freed with the scan cursor.
     * DatumGetByteaPP handles TOAST expansion in place.
     */
    raw     = DatumGetByteaPP(bytesDatum);
    *out_len = VARSIZE_ANY_EXHDR(raw);
    *out_buf = (char *) palloc(*out_len);
    memcpy(*out_buf, VARDATA_ANY(raw), *out_len);

    systable_endscan(scan);
    table_close(rel, AccessShareLock);
}

/*
 * upb_pg_alloc
 *		palloc-backed allocator hook for upb arenas.
 *
 * upb arenas free their slabs during upb_Arena_Free() by calling this
 * back with size == 0.
 */
static void *
upb_pg_alloc(upb_alloc *alloc, void *ptr, size_t oldsize, size_t size)
{
    (void) alloc;
    if (size == 0)
    {
        if (ptr != NULL)
            pfree(ptr);
        return NULL;
    }
    if (ptr == NULL)
        return palloc(size);
    return repalloc(ptr, size);
}

/*
 * kafka_proto_decoder_reset_callback
 *		MemoryContext reset/delete callback for KafkaProtoDecoder.
 *
 *		Normal EndForeignScan calls KafkaProtoDecoderClose() explicitly
 *		and then sets the slot to NULL; this callback becomes a no-op.
 *		On query abort EndForeignScan never runs, so the callback is
 *		the only chance to release upb_DefPool -- which is malloc-
 *		backed inside upb and therefore invisible to memcontext sweep.
 */
static void
kafka_proto_decoder_reset_callback(void *arg)
{
    KafkaProtoDecoder **slot = (KafkaProtoDecoder **) arg;
    KafkaProtoDecoder  *dec;

    if (slot == NULL)
        return;
    dec = *slot;
    if (dec == NULL)
        return;
    *slot = NULL;

    if (dec->decode_arena != NULL)
        upb_Arena_Free(dec->decode_arena);
    if (dec->pool != NULL)
        upb_DefPool_Free(dec->pool);
    if (dec->desc_arena != NULL)
        upb_Arena_Free(dec->desc_arena);
}

/*
 * build_defpool_from_desc
 *		Turn a serialized FileDescriptorSet into the decoder's upb
 *		DefPool: parse the bytes and register each contained
 *		FileDescriptorProto.  Knows nothing about where the bytes came
 *		from -- `srcname` is only used to label errors.
 */
static void
build_defpool_from_desc(KafkaProtoDecoder *dec,
                        const char *bytes, size_t len, const char *srcname)
{
    const upb_MiniTable      *fds_mt;
    const upb_MiniTableField *file_field;
    upb_Message              *fds_msg;
    const upb_Array          *files_arr;
    size_t                    nfiles;
    size_t                    i;
    upb_Status                status;
    upb_DecodeStatus          dec_st;
    upb_MessageValue          elem;
    const void               *file_proto;
    const upb_FileDef        *fd;

    upb_Status_Clear(&status);

    dec->desc_arena = upb_Arena_Init(NULL, 0, &upb_pg_allocator);
    if (dec->desc_arena == NULL)
        ereport(ERROR, (errmsg("upb: Arena_Init failed")));

    fds_mt  = &google_protobuf_FileDescriptorSet_msg_init;
    fds_msg = upb_Message_New(fds_mt, dec->desc_arena);
    if (fds_msg == NULL)
        ereport(ERROR, (errmsg("upb: Message_New(FDSet) failed")));

    dec_st = upb_Decode(bytes, len, fds_msg, fds_mt,
                        NULL /* extreg */, 0 /* options */,
                        dec->desc_arena);
    if (dec_st != kUpb_DecodeStatus_Ok)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("failed to parse FileDescriptorSet at \"%s\" (upb %d)",
                        srcname, (int) dec_st)));

    /*
     * FileDescriptorSet has exactly one field:
     *     repeated FileDescriptorProto file = 1;
     * Pull it out by number so we don't have to rely on the
     * bootstrap-generated C helpers.
     */
    file_field = upb_MiniTable_FindFieldByNumber(fds_mt, 1);
    if (file_field == NULL)
        ereport(ERROR, (errmsg("upb: field #1 missing on FDSet MiniTable")));

    files_arr = upb_Message_GetArray(fds_msg, file_field);
    nfiles = (files_arr != NULL) ? upb_Array_Size(files_arr) : 0;

    dec->pool = upb_DefPool_New();
    if (dec->pool == NULL)
        ereport(ERROR, (errmsg("upb: DefPool_New failed")));

    for (i = 0; i < nfiles; i++)
    {
        elem       = upb_Array_Get(files_arr, i);
        file_proto = elem.msg_val;
        fd = upb_DefPool_AddFile(dec->pool,
                                 (const struct google_protobuf_FileDescriptorProto *)
                                     file_proto,
                                 &status);
        if (fd == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("upb: failed to add file %zu from \"%s\": %s",
                            i, srcname, upb_Status_ErrorMessage(&status))));
    }
}

/*
 * pb_datum_compatible
 *		Decide whether a (protobuf scalar CType, PG column type) pair
 *		can be handed straight to the executor as a Datum, bypassing
 *		the sprintf -> cstring -> InputFunctionCall round-trip.
 *
 *		Only singular scalar leaves qualify.  Repeated / map / message
 *		fields still go through the reflection-driven JSON encode +
 *		fmgr path.
 */
static bool
pb_datum_compatible(upb_CType ctype, Oid pg_type)
{
    switch (ctype)
    {
        case kUpb_CType_Int32:
        case kUpb_CType_Enum:
            return pg_type == INT4OID || pg_type == INT8OID;
        case kUpb_CType_UInt32:
            /* fits in signed int64 without truncation. */
            return pg_type == INT8OID;
        case kUpb_CType_Int64:
            return pg_type == INT8OID;
        case kUpb_CType_UInt64:
            /* uint64 can be > INT64_MAX, so int8 can silently truncate;
             * numeric holds it exactly. */
            return pg_type == NUMERICOID;
        case kUpb_CType_Float:
            return pg_type == FLOAT4OID || pg_type == FLOAT8OID;
        case kUpb_CType_Double:
            return pg_type == FLOAT8OID;
        case kUpb_CType_Bool:
            return pg_type == BOOLOID;
        case kUpb_CType_String:
            return pg_type == TEXTOID || pg_type == VARCHAROID ||
                   pg_type == BPCHAROID;
        case kUpb_CType_Bytes:
            return pg_type == BYTEAOID;
        case kUpb_CType_Message:
            /* fast path can't emit sub-messages -- they need JSON
             * canonicalisation which requires the reflection def
             * graph.  Falls back to cstring / jsonb_in. */
            return false;
    }
    return false;
}

/*
 * resolve_field_path
 *		Resolve a column's OPTIONS (protobuf '<spec>') into a chain of
 *		FieldDefs.  <spec> is a dot-separated path ("field" or
 *		"parent.child.leaf"); every component except the last must name a
 *		singular sub-message.  Errors out on unknown fields, attempts to
 *		descend through a non-message field, or an over-deep path.
 */
static void
resolve_field_path(const upb_MessageDef *root, const char *spec,
                   const char *msg_name, KafkaProtoFieldPath *out_path)
{
    const upb_MessageDef *cur = root;
    char                 *dup = pstrdup(spec);
    char                 *saveptr = NULL;
    char                 *tok;
    int                   len = 0;
    const upb_FieldDef   *fd;

    for (tok = strtok_r(dup, ".", &saveptr); tok != NULL;
         tok = strtok_r(NULL, ".", &saveptr))
    {
        if (cur == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("protobuf path \"%s\": cannot descend into a non-message field before \"%s\"",
                            spec, tok)));
        if (len >= KAFKA_PROTO_MAX_PATH)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("protobuf path \"%s\" is too deep (max %d)",
                            spec, KAFKA_PROTO_MAX_PATH)));

        fd = upb_MessageDef_FindFieldByName(cur, tok);
        if (fd == NULL)
            ereport(ERROR,
                    (errcode(ERRCODE_FDW_ERROR),
                     errmsg("protobuf field \"%s\" (in path \"%s\") not found in message \"%s\"",
                            tok, spec, msg_name)));

        out_path->steps[len++] = fd;

        /* Only a singular sub-message can be descended into further. */
        if (upb_FieldDef_CType(fd) == kUpb_CType_Message &&
            !upb_FieldDef_IsRepeated(fd))
            cur = upb_FieldDef_MessageSubDef(fd);
        else
            cur = NULL;
    }

    if (len == 0)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("empty protobuf field path")));

    out_path->len = len;
    pfree(dup);
}

/*
 * KafkaProtoDecoderOpen
 *		Set up a decoder for one foreign scan: load the .desc, look
 *		up the message type, build the dense per-column map to
 *		upb_FieldDef, and allocate the per-message decode arena.
 *
 * Only exposed to callers as KafkaProtoDecoder *.
 */
KafkaProtoDecoder *
KafkaProtoDecoderOpen(Relation rel, KafkaFdwExecutionState *festate)
{
    KafkaProtoDecoder *dec;
    const char        *desc_path = festate->parse_options.proto_descriptor;
    const char        *msg_name  = festate->parse_options.proto_message;
    TupleDesc          tupdesc;
    AttrNumber         attnum;
    char              *desc_bytes;
    size_t             desc_len;
    MemoryContext         ctx;
    MemoryContextCallback *cb;

    if (desc_path == NULL || *desc_path == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("protobuf format requires the \"proto_desc\" table option")));
    if (msg_name == NULL || *msg_name == '\0')
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("protobuf format requires the \"proto_message\" table option")));

    dec = (KafkaProtoDecoder *) palloc0(sizeof(KafkaProtoDecoder));

    /*
     * Fetch the registered descriptor bytes from the catalog, then build
     * the upb DefPool from them.  The two steps are deliberately kept
     * separate: read_desc_from_catalog owns "where the bytes come from",
     * build_defpool_from_desc owns "bytes -> DefPool".
     */
    read_desc_from_catalog(desc_path, &desc_bytes, &desc_len);

    build_defpool_from_desc(dec, desc_bytes, desc_len, desc_path);

    dec->msg_def = upb_DefPool_FindMessageByName(dec->pool, msg_name);
    if (dec->msg_def == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("protobuf message \"%s\" not found in \"%s\"",
                        msg_name, desc_path)));
    dec->mini_table = upb_MessageDef_MiniTable(dec->msg_def);

    /*
     * Build the per-column field-path map that raw_fields[] will follow.
     *
     * The tuple builder (ReadKafkaMessage) consumes exactly one
     * raw_fields[] slot for every *parsable* column -- i.e. every
     * non-dropped column that is not partition/offset/junk -- in attnum
     * order.  So, mirroring the JSON reader, we must emit one path entry
     * per parsable column, whether or not it carries a protobuf mapping;
     * a parsable column with no OPTIONS (protobuf '...') keeps its
     * zero-initialised path (len == 0) and decodes to SQL NULL.  Meta
     * columns are filled by the builder itself and are skipped here so
     * the slot counts stay aligned.
     */
    tupdesc = RelationGetDescr(rel);

    dec->paths      = (KafkaProtoFieldPath *)
        palloc0(sizeof(KafkaProtoFieldPath) * tupdesc->natts);
    dec->datum_cols  = (KafkaProtoDatumCol *)
        palloc0(sizeof(KafkaProtoDatumCol) * tupdesc->natts);
    dec->datum_values = (Datum *) palloc0(sizeof(Datum) * tupdesc->natts);
    dec->datum_nulls  = (bool *)  palloc0(sizeof(bool)  * tupdesc->natts);
    dec->any_datum    = false;
    dec->nfields     = 0;

    for (attnum = 1; attnum <= tupdesc->natts; attnum++)
    {
        Form_pg_attribute           attr;
        List                       *options;
        ListCell                   *lc;
        DefElem                    *def;
        const char                 *field_spec;
        KafkaProtoFieldPath        *p;
        const upb_MiniTableField   *mt;
        int                         s;

        attr = TupleDescAttr(tupdesc, attnum - 1);
        if (attr->attisdropped)
            continue;
        if (!parsable_attnum(attnum, festate->kafka_options))
            continue;

        field_spec = NULL;
        options = GetForeignColumnOptions(RelationGetRelid(rel), attnum);
        foreach (lc, options)
        {
            def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, "protobuf") == 0)
            {
                field_spec = defGetString(def);
                break;
            }
        }

        /* A parsable column with no OPTIONS(protobuf ...) keeps its
         * zero-initialised path (len == 0) and decodes to SQL NULL. */
        if (field_spec == NULL)
        {
            dec->nfields++;
            continue;
        }

        p = &dec->paths[dec->nfields];
        resolve_field_path(dec->msg_def, field_spec, msg_name, p);

        /* Cache MiniTableField for every path step so the scan loop's
         * scalar fast path never walks the def graph. */
        for (s = 0; s < p->len; s++)
            p->mt_steps[s] = upb_FieldDef_MiniTable(p->steps[s]);

        /*
         * Decide once whether this column can bypass sprintf +
         * InputFunctionCall.  Restricted to plain top-level scalars
         * (no dotted paths, no repeated/map); pb_datum_compatible()
         * vets the type pairing.
         *
         * atttypmod == -1 is required: the direct-Datum path builds the
         * value without a typmod, so it cannot enforce a declared length
         * / precision.  Length- or precision-constrained types
         * (varchar(n), char(n), numeric(p,s)) must go through the cstring
         * path so InputFunctionCall(..., atttypmod) applies the limit and
         * bpchar blank-padding.  The unconstrained scalars we fast-path
         * (int/float/bool/bytea/text, and unbounded varchar/numeric)
         * always have atttypmod == -1, so this costs them nothing.
         */
        if (p->len == 1)
        {
            mt = p->mt_steps[0];
            if (!upb_IsRepeatedOrMap(mt) &&
                attr->atttypmod == -1 &&
                pb_datum_compatible(upb_MiniTableField_CType(mt), attr->atttypid))
            {
                dec->datum_cols[dec->nfields].ok        = true;
                dec->datum_cols[dec->nfields].pg_type   = attr->atttypid;
                dec->datum_cols[dec->nfields].pg_typmod = attr->atttypmod;
                dec->any_datum = true;
            }
        }

        dec->nfields++;
    }

    dec->decode_arena = upb_Arena_Init(dec->arena_scratch,
                                       sizeof dec->arena_scratch,
                                       &upb_pg_allocator);
    if (dec->decode_arena == NULL)
        ereport(ERROR, (errmsg("upb: Arena_Init failed for decode arena")));

    /*
     * Attach a reset callback so upb_DefPool -- which is malloc-backed
     * inside upb and therefore invisible to memcontext sweep -- is
     * released even if the query aborts before EndForeignScan runs.
     * Registered on the context where `dec` (and hence festate) lives;
     * the arg is the slot address so the manual-close path can zero
     * it and turn this callback into a no-op.
     */
    ctx = GetMemoryChunkContext(dec);
    cb = (MemoryContextCallback *) MemoryContextAllocZero(ctx, sizeof(*cb));

    cb->func = kafka_proto_decoder_reset_callback;
    cb->arg  = &festate->proto_decoder;
    MemoryContextRegisterResetCallback(ctx, cb);

    return dec;
}

static void
pb_report_error(KafkaFdwExecutionState *festate, const char *msg)
{
    if (festate->kafka_options.junk_error_attnum == -1)
        return;
    appendStringInfoString(&festate->junk_buf, msg);
}

static const char *
pb_decode_status_msg(upb_DecodeStatus st)
{
    switch (st)
    {
        case kUpb_DecodeStatus_Malformed:
            return "protobuf: malformed wire format";
        case kUpb_DecodeStatus_OutOfMemory:
            return "protobuf: arena out of memory";
        case kUpb_DecodeStatus_BadUtf8:
            return "protobuf: string field has invalid UTF-8";
        case kUpb_DecodeStatus_MaxDepthExceeded:
            return "protobuf: message nesting depth exceeded";
        case kUpb_DecodeStatus_MissingRequired:
            return "protobuf: required field missing";
        case kUpb_DecodeStatus_UnlinkedSubMessage:
            return "protobuf: unlinked sub-message referenced";
        default:
            return "protobuf: decode failed";
    }
}

/*
 * KafkaReadAttributesProtobuf
 *		Decode one Kafka payload and expose each field's text form
 *		through festate->raw_fields[], mirroring the CSV/JSON path.
 *
 * Returns the count of fields written, or -1 on decode failure (with
 * *had_error set to true).  On failure, and only when the foreign
 * table declares a junk_error column, appends a descriptive reason to
 * festate->junk_buf so downstream rows carry the specific upb status
 * rather than a bare "protobuf decode failed".
 */
int
KafkaReadAttributesProtobuf(const char *payload, int payload_len,
                            KafkaFdwExecutionState *festate,
                            bool *had_error)
{
    KafkaProtoDecoder          *dec;
    upb_Message                *msg;
    upb_DecodeStatus            dec_st;
    int                        *offsets;
    bool                       *hasval;
    int                         i;
    const KafkaProtoFieldPath  *path;
    const upb_Message          *cur;
    const upb_FieldDef         *leaf;
    const upb_FieldDef         *step;
    const upb_MiniTableField   *mt_leaf;
    upb_MessageValue            val;
    upb_MessageValue            mv;
    bool                        isnull;
    int                         s;

    *had_error = false;
    dec = festate->proto_decoder;

    if (dec == NULL)
    {
        pb_report_error(festate, "protobuf: decoder not initialised");
        *had_error = true;
        return -1;
    }

    /*
     * Free and re-create the arena so allocations from the previous
     * message do not accumulate.  Batching this across N rows was
     * tried and reverted: upb's arena grows its slabs exponentially
     * (upb/mem/arena.c: block_size = max(request, last_size*2)) and
     * an arena that outlives multiple messages hits a 4GB slab
     * request after ~24 growth steps, well within the row counts we
     * see here.
     *
     * The re-init hands upb our decoder-owned scratch buffer as the
     * initial block, so 0 malloc + 0 free happens for messages that
     * fit inside KAFKA_PROTO_ARENA_SCRATCH_SIZE (mix100 measured at
     * 2400 B/message).  Bigger messages fall back to upb_pg_allocator
     * (palloc) transparently and pay the same cost as the pre-scratch
     * behaviour.
     */
    upb_Arena_Free(dec->decode_arena);
    dec->decode_arena = upb_Arena_Init(dec->arena_scratch,
                                       sizeof dec->arena_scratch,
                                       &upb_pg_allocator);
    if (dec->decode_arena == NULL)
    {
        pb_report_error(festate,
                        "protobuf: per-message arena allocation failed");
        *had_error = true;
        return -1;
    }

    msg = upb_Message_New(dec->mini_table, dec->decode_arena);
    if (msg == NULL)
    {
        pb_report_error(festate, "protobuf: message allocation failed");
        *had_error = true;
        return -1;
    }

    dec_st = upb_Decode(payload, (size_t) payload_len, msg,
                        dec->mini_table, NULL /* extreg */, 0 /* options */,
                        dec->decode_arena);
    if (dec_st != kUpb_DecodeStatus_Ok)
    {
        pb_report_error(festate, pb_decode_status_msg(dec_st));
        *had_error = true;
        return -1;
    }

    resetStringInfo(&festate->attribute_buf);

    offsets = (int *)  palloc(sizeof(int)  * dec->nfields);
    hasval  = (bool *) palloc(sizeof(bool) * dec->nfields);

    for (i = 0; i < dec->nfields; i++)
    {
        path       = &dec->paths[i];
        cur        = msg;
        isnull     = false;
        offsets[i] = festate->attribute_buf.len;

        /*
         * A parsable column with no protobuf mapping (len == 0) always
         * decodes to SQL NULL; this keeps raw_fields[] aligned with
         * the tuple builder, which consumes one slot per parsable column.
         */
        if (path->len == 0)
        {
            if (dec->datum_cols[i].ok)
                dec->datum_nulls[i] = true;
            hasval[i] = false;
            continue;
        }

        /*
         * Walk the intermediate sub-message steps of a dotted path.
         * A singular sub-message that is absent (or an inactive oneof
         * member, which also reports not-present) makes the whole
         * column SQL NULL.  For a plain top-level field (path->len ==
         * 1) this loop does nothing and cur stays the root message.
         */
        for (s = 0; s < path->len - 1; s++)
        {
            step = path->steps[s];
            if (!upb_Message_HasFieldByDef(cur, step))
            {
                isnull = true;
                break;
            }
            mv = upb_Message_GetFieldByDef(cur, step);
            if (mv.msg_val == NULL)
            {
                isnull = true;
                break;
            }
            cur = mv.msg_val;
        }
        if (isnull)
        {
            if (dec->datum_cols[i].ok)
                dec->datum_nulls[i] = true;
            hasval[i] = false;
            continue;
        }

        leaf    = path->steps[path->len - 1];
        mt_leaf = path->mt_steps[path->len - 1];

        /*
         * Presence gate on the leaf.  Uses the MiniTable variant so
         * the check is a hasbits load; the FieldDef equivalent would
         * walk the def graph twice on the hot path.
         */
        if (upb_MiniTableField_HasPresence(mt_leaf) &&
            !upb_Message_HasField(cur, mt_leaf))
        {
            if (dec->datum_cols[i].ok)
                dec->datum_nulls[i] = true;
            hasval[i] = false;
            continue;
        }

        /* Direct-Datum fast path: skip the sprintf + fmgr round-trip. */
        if (dec->datum_cols[i].ok)
        {
            dec->datum_values[i] = pb_extract_datum(cur, mt_leaf,
                                                    dec->datum_cols[i].pg_type);
            dec->datum_nulls[i]  = false;
            hasval[i]            = false;   /* raw_fields[i] stays NULL */
            continue;
        }

        /* Cstring fast path: MiniTable accessors + attribute_buf. */
        if (!upb_IsRepeatedOrMap(mt_leaf) &&
            upb_MiniTableField_CType(mt_leaf) != kUpb_CType_Message)
        {
            if (!field_to_text_scalar(cur, mt_leaf, &festate->attribute_buf))
            {
                hasval[i] = false;
                continue;
            }
            appendStringInfoChar(&festate->attribute_buf, '\0');
            hasval[i] = true;
            continue;
        }

        /* Slow path: repeated / map / sub-message, reflection-driven. */
        val = upb_Message_GetFieldByDef(cur, leaf);
        if (!field_to_text(leaf, val, dec->pool, &festate->attribute_buf))
        {
            hasval[i] = false;
            continue;
        }
        appendStringInfoChar(&festate->attribute_buf, '\0');
        hasval[i] = true;
    }

    for (i = 0; i < dec->nfields; i++)
        festate->raw_fields[i] = hasval[i]
            ? festate->attribute_buf.data + offsets[i]
            : NULL;

    pfree(offsets);
    pfree(hasval);

    return dec->nfields;
}

/*
 * KafkaProtoDatumAvailable
 *		True iff the direct-Datum fast path applies to this column.
 *		Set at scan open based on the (proto scalar CType, PG column
 *		type) pair -- see pb_datum_compatible().  Callers on the
 *		dispatcher hot path skip the raw_fields[] / InputFunctionCall
 *		branch when this returns true and read the Datum via
 *		KafkaProtoDatumGet() instead.
 */
bool
KafkaProtoDatumAvailable(KafkaProtoDecoder *dec, int fldnum)
{
    if (dec == NULL || !dec->any_datum)
        return false;
    if (fldnum < 0 || fldnum >= dec->nfields)
        return false;
    return dec->datum_cols[fldnum].ok;
}

/*
 * KafkaProtoDatumGet
 *		Retrieve the Datum that KafkaReadAttributesProtobuf wrote
 *		into dec->datum_values[fldnum] for the current row.  Only
 *		valid to call when KafkaProtoDatumAvailable returns true; the
 *		null flag is written into *isnull.
 */
Datum
KafkaProtoDatumGet(KafkaProtoDecoder *dec, int fldnum, bool *isnull)
{
    *isnull = dec->datum_nulls[fldnum];
    return *isnull ? (Datum) 0 : dec->datum_values[fldnum];
}

/*
 * KafkaProtoDecoderClose
 *		Release everything owned by the decoder.  Safe to call with
 *		NULL.
 */
void
KafkaProtoDecoderClose(KafkaProtoDecoder *dec)
{
    if (dec == NULL)
        return;

    if (dec->decode_arena != NULL)
        upb_Arena_Free(dec->decode_arena);
    if (dec->pool != NULL)
        upb_DefPool_Free(dec->pool);
    if (dec->desc_arena != NULL)
        upb_Arena_Free(dec->desc_arena);
    if (dec->paths != NULL)
        pfree(dec->paths);
    pfree(dec);
}

/*
 * Emit `len` bytes as a JSON-escaped, double-quoted string.  Handles
 * the mandatory JSON escapes (control chars, quote, backslash) and
 * lets everything else pass through - protobuf strings are already
 * UTF-8, and for bytes the caller may still want the raw octets
 * (base64 encoding is left to the reader if they need it).
 */
static void
emit_json_string(StringInfo out, const char *data, size_t len)
{
    size_t          i;
    unsigned char   c;

    appendStringInfoChar(out, '"');
    for (i = 0; i < len; i++)
    {
        c = (unsigned char) data[i];
        switch (c)
        {
            case '"':  appendStringInfoString(out, "\\\""); break;
            case '\\': appendStringInfoString(out, "\\\\"); break;
            case '\b': appendStringInfoString(out, "\\b");  break;
            case '\f': appendStringInfoString(out, "\\f");  break;
            case '\n': appendStringInfoString(out, "\\n");  break;
            case '\r': appendStringInfoString(out, "\\r");  break;
            case '\t': appendStringInfoString(out, "\\t");  break;
            default:
                if (c < 0x20)
                    appendStringInfo(out, "\\u%04x", c);
                else
                    appendStringInfoChar(out, (char) c);
                break;
        }
    }
    appendStringInfoChar(out, '"');
}

/*
 * Append a float / double using PostgreSQL's own output functions
 * (float4out / float8out) rather than a hand-picked printf format, so
 * the text is the canonical shortest round-trippable form and matches
 * what float4in / float8in expect when the column is fed back through
 * InputFunctionCall.
 */
static void
append_float4(StringInfo out, float v)
{
    char *s = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(v)));

    appendStringInfoString(out, s);
    pfree(s);
}

static void
append_float8(StringInfo out, double v)
{
    char *s = DatumGetCString(DirectFunctionCall1(float8out, Float8GetDatum(v)));

    appendStringInfoString(out, s);
    pfree(s);
}

/*
 * Emit one scalar upb value in JSON syntax.  Follows proto3 canonical
 * JSON for the 64-bit integer types (rendered as strings to preserve
 * precision) and represents non-finite floats as null (JSON has no
 * NaN/Infinity literals).
 */
static void
emit_scalar_json(StringInfo out, const upb_FieldDef *fd, upb_MessageValue val)
{
    switch (upb_FieldDef_CType(fd))
    {
        case kUpb_CType_Int32:
            appendStringInfo(out, "%d", val.int32_val);
            break;
        case kUpb_CType_UInt32:
            appendStringInfo(out, "%u", val.uint32_val);
            break;
        case kUpb_CType_Int64:
            appendStringInfo(out, "\"" INT64_FORMAT "\"", val.int64_val);
            break;
        case kUpb_CType_UInt64:
            appendStringInfo(out, "\"" UINT64_FORMAT "\"", val.uint64_val);
            break;
        case kUpb_CType_Float:
            if (isnan(val.float_val) || isinf(val.float_val))
                appendStringInfoString(out, "null");
            else
                append_float4(out, val.float_val);
            break;
        case kUpb_CType_Double:
            if (isnan(val.double_val) || isinf(val.double_val))
                appendStringInfoString(out, "null");
            else
                append_float8(out, val.double_val);
            break;
        case kUpb_CType_Bool:
            appendStringInfoString(out, val.bool_val ? "true" : "false");
            break;
        case kUpb_CType_String:
            emit_json_string(out, val.str_val.data, val.str_val.size);
            break;
        case kUpb_CType_Bytes:
        {
            /*
             * proto3 canonical JSON encodes bytes as base64 (and so does
             * upb_JsonEncode for bytes nested inside a message), so match
             * that here -- emitting raw octets would produce invalid
             * UTF-8 / invalid JSON that jsonb_in would reject.
             */
            upb_StringView bv     = val.str_val;
            int            enclen = pg_b64_enc_len((int) bv.size);
            char          *b64    = palloc(enclen + 1);
            int            n      = pg_b64_encode(bv.data, (int) bv.size,
                                                  b64, enclen);

            if (n < 0)
                n = 0;
            b64[n] = '\0';
            emit_json_string(out, b64, n);
            pfree(b64);
            break;
        }
        case kUpb_CType_Enum:
            appendStringInfo(out, "%d", val.int32_val);
            break;
        default:
            appendStringInfoString(out, "null");
            break;
    }
}

/*
 * Emit one scalar upb value as a PostgreSQL array-literal element.
 * Numeric and boolean elements go in bare; string and bytes are
 * double-quoted with backslash-escaped double-quote / backslash so
 * they parse back through array_in().
 *
 * Bytes with embedded NUL bytes are not supported here; downstream
 * strtok() logic assumes NUL-terminated cstrings anyway.
 */
static void
emit_scalar_pgarray(StringInfo out, const upb_FieldDef *fd, upb_MessageValue val)
{
    upb_StringView  sv;
    size_t          i;
    char            c;

    switch (upb_FieldDef_CType(fd))
    {
        case kUpb_CType_Int32:
            appendStringInfo(out, "%d", val.int32_val);
            break;
        case kUpb_CType_UInt32:
            appendStringInfo(out, "%u", val.uint32_val);
            break;
        case kUpb_CType_Int64:
            appendStringInfo(out, INT64_FORMAT, val.int64_val);
            break;
        case kUpb_CType_UInt64:
            appendStringInfo(out, UINT64_FORMAT, val.uint64_val);
            break;
        case kUpb_CType_Float:
            append_float4(out, val.float_val);
            break;
        case kUpb_CType_Double:
            append_float8(out, val.double_val);
            break;
        case kUpb_CType_Bool:
            appendStringInfoString(out, val.bool_val ? "t" : "f");
            break;
        case kUpb_CType_Enum:
            appendStringInfo(out, "%d", val.int32_val);
            break;
        case kUpb_CType_String:
            sv = val.str_val;
            appendStringInfoChar(out, '"');
            for (i = 0; i < sv.size; i++)
            {
                c = sv.data[i];
                if (c == '\\' || c == '"')
                    appendStringInfoChar(out, '\\');
                appendStringInfoChar(out, c);
            }
            appendStringInfoChar(out, '"');
            break;
        case kUpb_CType_Bytes:
            /*
             * Emit a quoted bytea hex literal ("\\xNN..") so array_in ->
             * byteain round-trips any byte value; raw octets would break
             * on an embedded backslash or NUL (unlike the singular bytea
             * path, which builds the datum directly).  The doubled
             * backslash survives array_in's quoted-element de-escaping.
             */
            sv = val.str_val;
            appendStringInfoChar(out, '"');
            appendStringInfoChar(out, '\\');
            appendStringInfoChar(out, '\\');
            appendStringInfoChar(out, 'x');
            for (i = 0; i < sv.size; i++)
                appendStringInfo(out, "%02x", (unsigned char) sv.data[i]);
            appendStringInfoChar(out, '"');
            break;
        default:
            /* Nested types cannot appear inside a repeated-scalar path. */
            appendStringInfoString(out, "NULL");
            break;
    }
}

/*
 * Two-pass encode a message via upb_JsonEncode, appending the JSON
 * text (without any trailing NUL) to `out`.  upb_JsonEncode has
 * snprintf semantics, so the first call sizes the output and the
 * second call fills the reserved space.
 */
static void
emit_message_json(StringInfo out, const upb_Message *msg,
                  const upb_MessageDef *msg_def, const upb_DefPool *pool)
{
    upb_Status status;
    size_t     needed;
    int        options = 0;

    upb_Status_Clear(&status);
    needed = upb_JsonEncode(msg, msg_def, pool, options, NULL, 0, &status);
    if (!upb_Status_IsOk(&status))
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("upb_JsonEncode failed: %s",
                        upb_Status_ErrorMessage(&status))));

    enlargeStringInfo(out, (int) needed + 1);
    upb_Status_Clear(&status);
    (void) upb_JsonEncode(msg, msg_def, pool, options,
                          out->data + out->len, needed + 1, &status);
    if (!upb_Status_IsOk(&status))
        ereport(ERROR,
                (errcode(ERRCODE_FDW_ERROR),
                 errmsg("upb_JsonEncode failed: %s",
                        upb_Status_ErrorMessage(&status))));
    out->len += (int) needed;
    out->data[out->len] = '\0';
}

/*
 * Emit a repeated (non-map) field.  Scalar element types produce a
 * PostgreSQL array literal (e.g. "{1,2,3}"), so users can map them to
 * int[], text[], and so on.  Message elements produce a JSON array
 * ("[{...},{...}]") intended for jsonb columns.
 */
static void
emit_repeated(StringInfo out, const upb_FieldDef *fd,
              const upb_Array *arr, const upb_DefPool *pool)
{
    upb_CType             ct = upb_FieldDef_CType(fd);
    size_t                n  = arr ? upb_Array_Size(arr) : 0;
    size_t                i;
    const upb_MessageDef *sub_def;

    if (ct == kUpb_CType_Message)
    {
        sub_def = upb_FieldDef_MessageSubDef(fd);
        appendStringInfoChar(out, '[');
        for (i = 0; i < n; i++)
        {
            if (i > 0)
                appendStringInfoChar(out, ',');
            emit_message_json(out, upb_Array_Get(arr, i).msg_val,
                              sub_def, pool);
        }
        appendStringInfoChar(out, ']');
        return;
    }

    appendStringInfoChar(out, '{');
    for (i = 0; i < n; i++)
    {
        if (i > 0)
            appendStringInfoChar(out, ',');
        emit_scalar_pgarray(out, fd, upb_Array_Get(arr, i));
    }
    appendStringInfoChar(out, '}');
}

/*
 * Emit a map<K,V> field as a JSON object.  Proto3 JSON mandates that
 * map keys be JSON strings, even when the wire key is an integer or
 * bool, so we always double-quote the key.
 */
static void
emit_map(StringInfo out, const upb_FieldDef *fd,
         const upb_Map *map, const upb_DefPool *pool)
{
    const upb_MessageDef *entry_def = upb_FieldDef_MessageSubDef(fd);
    const upb_FieldDef   *key_fd    = upb_MessageDef_FindFieldByNumber(entry_def, 1);
    const upb_FieldDef   *val_fd    = upb_MessageDef_FindFieldByNumber(entry_def, 2);
    size_t                iter      = kUpb_Map_Begin;
    upb_MessageValue      k;
    upb_MessageValue      v;
    bool                  first     = true;
    StringInfoData        keybuf;

    appendStringInfoChar(out, '{');
    while (map && upb_Map_Next(map, &k, &v, &iter))
    {
        if (!first)
            appendStringInfoChar(out, ',');
        first = false;

        /*
         * Render the key first into a scratch buffer using the same
         * JSON emitter as scalar values, then unconditionally re-wrap
         * it in quotes so integer/bool keys become JSON strings per spec.
         */
        initStringInfo(&keybuf);
        emit_scalar_json(&keybuf, key_fd, k);
        if (keybuf.len > 0 && keybuf.data[0] == '"')
            appendBinaryStringInfo(out, keybuf.data, keybuf.len);
        else
            emit_json_string(out, keybuf.data, keybuf.len);
        pfree(keybuf.data);

        appendStringInfoChar(out, ':');
        if (upb_FieldDef_CType(val_fd) == kUpb_CType_Message)
            emit_message_json(out, v.msg_val,
                              upb_FieldDef_MessageSubDef(val_fd), pool);
        else
            emit_scalar_json(out, val_fd, v);
    }
    appendStringInfoChar(out, '}');
}

/*
 * pb_extract_datum
 *		Directly build a PG Datum from a protobuf scalar leaf,
 *		bypassing sprintf + InputFunctionCall.
 *
 *		Precondition: pb_datum_compatible(ctype, pg_type) returned true
 *		at scan open (guarded by dec->datum_cols[i].ok in the caller),
 *		so we do not need to re-vet the type pairing here.  Only the
 *		known-good branches are implemented; the switch falls off the
 *		end for the "impossible" cases via elog(ERROR) so a wire type
 *		mismatch surfaces rather than corrupts.
 *
 *		Presence is the caller's business (upb_Message_HasField gate);
 *		this function assumes the field is present or that returning
 *		the proto3 default is desired.
 */
static Datum
pb_extract_datum(const upb_Message *msg,
                 const upb_MiniTableField *mtf,
                 Oid pg_type)
{
    upb_StringView  empty_sv = { .data = "", .size = 0 };
    upb_StringView  sv;
    int32           i32v;
    uint64          u64v;
    float           fv;
    bytea          *out;
    char            u64buf[32];

    switch (upb_MiniTableField_CType(mtf))
    {
        case kUpb_CType_Int32:
        case kUpb_CType_Enum:
            i32v = upb_Message_GetInt32(msg, mtf, 0);
            if (pg_type == INT8OID)
                return Int64GetDatum((int64) i32v);
            return Int32GetDatum(i32v);

        case kUpb_CType_UInt32:
            /* Widen to int8; upper bit is safe (fits in signed 64). */
            return Int64GetDatum((int64) upb_Message_GetUInt32(msg, mtf, 0));

        case kUpb_CType_Int64:
            return Int64GetDatum((int64) upb_Message_GetInt64(msg, mtf, 0));

        case kUpb_CType_UInt64:
            /*
             * Values <= INT64_MAX go through int64_to_numeric() directly
             * (one Numeric palloc, no fmgr).  Values above that fall back
             * to numeric_in on the base-10 form -- still cheaper than the
             * cstring path because we skip attribute_buf + the outer
             * InputFunctionCall dispatch, and the > INT64_MAX case is
             * rare in practice.
             */
            u64v = upb_Message_GetUInt64(msg, mtf, 0);
            if (u64v <= (uint64) PG_INT64_MAX)
                return NumericGetDatum(int64_to_numeric((int64) u64v));
            snprintf(u64buf, sizeof(u64buf), UINT64_FORMAT, u64v);
            return DirectFunctionCall3(numeric_in,
                                       CStringGetDatum(u64buf),
                                       ObjectIdGetDatum(InvalidOid),
                                       Int32GetDatum(-1));

        case kUpb_CType_Float:
            fv = upb_Message_GetFloat(msg, mtf, 0.0f);
            if (pg_type == FLOAT8OID)
                return Float8GetDatum((double) fv);
            return Float4GetDatum(fv);

        case kUpb_CType_Double:
            return Float8GetDatum(upb_Message_GetDouble(msg, mtf, 0.0));

        case kUpb_CType_Bool:
            return BoolGetDatum(upb_Message_GetBool(msg, mtf, false));

        case kUpb_CType_String:
            /*
             * cstring_to_text_with_len handles embedded NUL safely for
             * TEXT/VARCHAR (varlena header carries the length -- no
             * C-string terminator concern).  The cstring fast path
             * refuses embedded-NUL strings because fmgr would truncate
             * at the NUL; here we can keep them.
             */
            sv = upb_Message_GetString(msg, mtf, empty_sv);
            return PointerGetDatum(cstring_to_text_with_len(sv.data, sv.size));

        case kUpb_CType_Bytes:
            sv = upb_Message_GetString(msg, mtf, empty_sv);
            out = (bytea *) palloc(VARHDRSZ + sv.size);
            SET_VARSIZE(out, VARHDRSZ + sv.size);
            if (sv.size > 0)
                memcpy(VARDATA(out), sv.data, sv.size);
            return PointerGetDatum(out);

        default:
            /*
             * Should not happen: datum_cols[].ok is set only for the
             * branches above.
             */
            elog(ERROR, "protobuf fast path: unexpected CType %d",
                 (int) upb_MiniTableField_CType(mtf));
            return (Datum) 0;
    }
}

/*
 * field_to_text_scalar
 *		MiniTableField-driven fast path for singular scalar leaves.
 *
 *		The dispatch is a single upb_MiniTableField_CType() switch
 *		instead of the FieldDef variant used by field_to_text() below,
 *		which walks the reflection def graph for IsMap / IsRepeated /
 *		CType every call.  For the numeric and bool scalars we
 *		bypass upb_Message_GetFieldByDef entirely and go through the
 *		inlined typed accessors, which resolve to a hasbits check plus
 *		a direct offset load.  String/bytes take the same length-copy
 *		semantics as field_to_text() and inherit the embedded-NUL
 *		refusal / bytes -> hex escape rules.
 *
 *		Returns false to defer to the slow path (message / repeated /
 *		map, or any type the fast path does not implement).  Callers
 *		that see false must fall back to field_to_text().
 */
static bool
field_to_text_scalar(const upb_Message *msg,
                     const upb_MiniTableField *mtf, StringInfo out)
{
    upb_StringView  empty_sv = { .data = "", .size = 0 };
    upb_StringView  sv;
    size_t          k;

    switch (upb_MiniTableField_CType(mtf))
    {
        case kUpb_CType_Int32:
            appendStringInfo(out, "%d", upb_Message_GetInt32(msg, mtf, 0));
            return true;
        case kUpb_CType_UInt32:
            appendStringInfo(out, "%u", upb_Message_GetUInt32(msg, mtf, 0));
            return true;
        case kUpb_CType_Int64:
            appendStringInfo(out, INT64_FORMAT,
                             (int64) upb_Message_GetInt64(msg, mtf, 0));
            return true;
        case kUpb_CType_UInt64:
            appendStringInfo(out, UINT64_FORMAT,
                             (uint64) upb_Message_GetUInt64(msg, mtf, 0));
            return true;
        case kUpb_CType_Float:
            append_float4(out, upb_Message_GetFloat(msg, mtf, 0.0f));
            return true;
        case kUpb_CType_Double:
            append_float8(out, upb_Message_GetDouble(msg, mtf, 0.0));
            return true;
        case kUpb_CType_Bool:
            appendStringInfoString(out,
                                   upb_Message_GetBool(msg, mtf, false)
                                   ? "t" : "f");
            return true;
        case kUpb_CType_Enum:
            /* proto3 enums decode as int32; matches field_to_text(). */
            appendStringInfo(out, "%d", upb_Message_GetInt32(msg, mtf, 0));
            return true;
        case kUpb_CType_String:
            sv = upb_Message_GetString(msg, mtf, empty_sv);
            if (sv.data != NULL && sv.size > 0)
            {
                if (memchr(sv.data, '\0', sv.size) != NULL)
                    return false;   /* embedded NUL -> SQL NULL */
                appendBinaryStringInfo(out, sv.data, sv.size);
            }
            return true;
        case kUpb_CType_Bytes:
            sv = upb_Message_GetString(msg, mtf, empty_sv);
            appendStringInfoString(out, "\\x");
            for (k = 0; k < sv.size; k++)
                appendStringInfo(out, "%02x", (unsigned char) sv.data[k]);
            return true;
        default:
            /* Message -- caller must fall through to the slow path
             * because we need FieldDef to walk sub-message def graph. */
            return false;
    }
}

/*
 * field_to_text
 *      Cstring renderer for the *composite* leaves that neither the
 *      cstring fast path (field_to_text_scalar, MiniTable-driven) nor
 *      the direct-Datum fast path (pb_extract_datum) handle: repeated,
 *      map, and sub-message fields.  Scalar leaves reach the caller
 *      through the two faster paths and never enter here; we still
 *      route them through emit_scalar_json when they appear *inside*
 *      a repeated list or map value, which is emit_repeated /
 *      emit_map's job, not ours.
 *
 *      Called only from the scan loop's slow-path arm when
 *      datum_cols[i].ok is false AND the leaf is repeated / map /
 *      message.  Returns false to signal SQL NULL (currently only the
 *      "message field is NULL" case).
 */
static bool
field_to_text(const upb_FieldDef *fd, upb_MessageValue val,
              const upb_DefPool *pool, StringInfo out)
{
    const upb_MessageDef *sub;

    /*
     * Map fields are also IsRepeated() in upb, so check IsMap() first
     * to keep the map-object encoder from being masked by the general
     * repeated path.
     */
    if (upb_FieldDef_IsMap(fd))
    {
        emit_map(out, fd, val.map_val, pool);
        return true;
    }
    if (upb_FieldDef_IsRepeated(fd))
    {
        emit_repeated(out, fd, val.array_val, pool);
        return true;
    }

    if (upb_FieldDef_CType(fd) != kUpb_CType_Message)
        return false;

    if (val.msg_val == NULL)
        return false;

    sub = upb_FieldDef_MessageSubDef(fd);
    /* Well-known types render to a native scalar/timestamp/interval
       text form; everything else falls back to canonical jsonb. */
    if (emit_wellknown_type(out, val.msg_val, sub, pool))
        return true;
    emit_message_json(out, val.msg_val, sub, pool);
    return true;
}

/*
 * emit_wellknown_type
 *		Render a singular google.protobuf well-known type to a native
 *		text form so it can map to a native PostgreSQL column instead of
 *		jsonb:
 *
 *		  * the nine scalar wrappers (Int32Value, StringValue, ...) are
 *		    unwrapped to their bare "value" (-> int / text / bool / ...);
 *		  * Timestamp -> TimestampTz rendered with timestamptz_out;
 *		  * Duration  -> Interval rendered with interval_out.
 *
 *		Timestamp/Duration build the PG datum and use PG's own output
 *		function rather than a hand-rolled format string, so the text is
 *		guaranteed to round-trip through the column's input function.
 *
 *		Struct / Value / ListValue / Any / FieldMask have no natural
 *		scalar form, so emit_wellknown_type returns false for them and the caller
 *		encodes them as canonical jsonb.  `m` is guaranteed non-NULL.
 */
static bool
emit_wellknown_type(StringInfo out, const upb_Message *m,
                    const upb_MessageDef *md, const upb_DefPool *pool)
{
    const upb_FieldDef *vf;
    const upb_FieldDef *sf;
    const upb_FieldDef *nf;
    int64_t             seconds;
    int32_t             nanos;
    TimestampTz         ts;
    Interval            iv;
    char               *s;

    /*
     * upb tags each MessageDef with its well-known type when the
     * DefPool is built, so classify by that enum rather than by
     * matching the message's full name.
     */
    switch (upb_MessageDef_WellKnownType(md))
    {
        /* Scalar wrappers: unwrap field #1 ("value") to a bare scalar. */
        case kUpb_WellKnown_DoubleValue:
        case kUpb_WellKnown_FloatValue:
        case kUpb_WellKnown_Int64Value:
        case kUpb_WellKnown_UInt64Value:
        case kUpb_WellKnown_Int32Value:
        case kUpb_WellKnown_UInt32Value:
        case kUpb_WellKnown_StringValue:
        case kUpb_WellKnown_BytesValue:
        case kUpb_WellKnown_BoolValue:
            vf = upb_MessageDef_FindFieldByNumber(md, 1);
            if (vf == NULL)
                return false;
            return field_to_text(vf, upb_Message_GetFieldByDef(m, vf), pool, out);

        case kUpb_WellKnown_Timestamp:
            sf = upb_MessageDef_FindFieldByNumber(md, 1);
            nf = upb_MessageDef_FindFieldByNumber(md, 2);
            seconds = sf ? upb_Message_GetFieldByDef(m, sf).int64_val : 0;
            nanos   = nf ? upb_Message_GetFieldByDef(m, nf).int32_val : 0;
            /* seconds since the Unix epoch -> TimestampTz, plus the
             * sub-second part as microseconds (PG's resolution). */
            ts = time_t_to_timestamptz((pg_time_t) seconds) + nanos / 1000;
            s  = DatumGetCString(DirectFunctionCall1(timestamptz_out,
                                                    TimestampTzGetDatum(ts)));
            appendStringInfoString(out, s);
            pfree(s);
            return true;

        case kUpb_WellKnown_Duration:
            sf = upb_MessageDef_FindFieldByNumber(md, 1);
            nf = upb_MessageDef_FindFieldByNumber(md, 2);
            seconds = sf ? upb_Message_GetFieldByDef(m, sf).int64_val : 0;
            nanos   = nf ? upb_Message_GetFieldByDef(m, nf).int32_val : 0;
            /* A protobuf Duration is a pure elapsed time, so map it
             * onto the microsecond field of an Interval (no days /
             * months). */
            iv.month = 0;
            iv.day   = 0;
            iv.time  = seconds * USECS_PER_SEC + nanos / 1000;
            s = DatumGetCString(DirectFunctionCall1(interval_out,
                                                   IntervalPGetDatum(&iv)));
            appendStringInfoString(out, s);
            pfree(s);
            return true;

        default:
            /* Any / FieldMask / Struct / Value / ListValue (and non-WKT
             * messages) have no natural scalar form -> caller renders
             * them as canonical jsonb. */
            return false;
    }
}
