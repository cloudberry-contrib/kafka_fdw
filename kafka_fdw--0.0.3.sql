CREATE TABLE kafka_fdw_offset_dump(
    tbloid oid,
    "partition" int,
    "offset" bigint,
    last_fetch timestamp DEFAULT statement_timestamp(),
    PRIMARY KEY(tbloid, "partition")
);
SELECT pg_catalog.pg_extension_config_dump('kafka_fdw_offset_dump', '');

--
-- Dedicated internal schema for kafka_fdw's bookkeeping tables. Keeps
-- them out of the user's default search_path (public) so \d shows a
-- clean workspace.
--
CREATE SCHEMA kafka_fdw;
GRANT USAGE ON SCHEMA kafka_fdw TO PUBLIC;

--
-- kafka_fdw.proto_descriptors: the protobuf FileDescriptorSet cache.
--
-- Populated by kafka_fdw_validator() at CREATE / ALTER FOREIGN TABLE
-- time on the QD: the .desc file is read from disk once and its bytes
-- are UPSERT'd here. Because the table is DISTRIBUTED REPLICATED,
-- Cloudberry keeps a full copy on every segment, so QE-side decoders
-- can look up the descriptor via a local index scan instead of
-- needing the file to exist on segment hosts.
--
-- 'path' is used verbatim as the lookup key — same string the user
-- gave in OPTIONS (proto_desc '...') — so refresh is just
-- ALTER FOREIGN TABLE ... OPTIONS (SET proto_desc '<same>').
--
CREATE TABLE kafka_fdw.proto_descriptors(
    path        text PRIMARY KEY,
    desc_bytes  bytea NOT NULL,
    mtime       timestamptz,
    updated_at  timestamptz NOT NULL DEFAULT now()
) DISTRIBUTED REPLICATED;
SELECT pg_catalog.pg_extension_config_dump('kafka_fdw.proto_descriptors', '');

CREATE FUNCTION kafka_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kafka_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kafka_fdw
  HANDLER kafka_fdw_handler
  VALIDATOR kafka_fdw_validator;

CREATE FUNCTION kafka_get_watermarks(IN rel regclass,
	OUT "partition" int,
	OUT offset_low bigint,
	OUT offset_high bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'kafka_get_watermarks'
LANGUAGE C STRICT;

--
-- Read a serialized FileDescriptorSet from the QD's filesystem and
-- cache it into kafka_fdw.proto_descriptors. Must be called BEFORE
-- CREATE FOREIGN TABLE ... OPTIONS (proto_desc '<same path>'); call
-- it again with the same path to refresh the cached bytes after the
-- .desc file changes.
--
CREATE FUNCTION kafka_fdw.register_proto_descriptor(path text)
RETURNS void
AS 'MODULE_PATHNAME', 'kafka_fdw_register_proto_descriptor'
LANGUAGE C STRICT;
