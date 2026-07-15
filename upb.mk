# --------------------------------------------------------------------
# upb.mk -- compile the upb protobuf runtime into kafka_fdw.so
#
# Included by the top-level Makefile BEFORE "include $(PGXS)", so that
# OBJS / PG_CPPFLAGS are augmented before PGXS consumes them.  This file
# lives at the repo root (not under thirdparty/) so the include always
# resolves even before the submodules have been checked out.
#
# The "format 'protobuf'" decoder is built on upb (protobuf's small C
# runtime).  upb and its utf8_range dependency are git submodules under
# thirdparty/ and are compiled straight into kafka_fdw.so, so the
# extension is fully self-contained: NO libupb*.so / libprotobuf.so
# runtime dependency, and nothing extra to install.  The submodules are
# pinned to the exact commits protobuf 24.3 uses (upb 42cd0893,
# utf8_range de0b4a8); because we link statically kafka_fdw is not
# coupled to the platform protobuf version.
#
# Fetch the submodules once before building:
#     git submodule update --init thirdparty/upb thirdparty/utf8_range
#
# CRITICAL: the upb/utf8_range objects are compiled with
# -fvisibility=hidden (see the UPB_OBJS rule below) so their symbols stay
# private to kafka_fdw.so.  pax_storage's pax.so pulls the platform
# libupb.so.36 into the same backend; without hidden visibility the two
# upb copies would clash (symbol interposition -> arena corruption).
# --------------------------------------------------------------------

UPB_ROOT    = thirdparty/upb
U8_ROOT     = thirdparty/utf8_range

UPB_SUBDIRS = base collections hash json lex mem message \
              mini_descriptor mini_descriptor/internal \
              mini_table mini_table/internal reflection wire

# descriptor.upb.c is self-contained (minitables + accessors, consistent
# single-underscore msg_init names in upb 24.x).  upb_so.c (the libupb.so
# entry point) and reflection/stage0 (the bootstrap descriptor that would
# duplicate descriptor.upb.c's symbols) are deliberately excluded.
UPB_SRCS    = $(foreach d,$(UPB_SUBDIRS),$(wildcard $(UPB_ROOT)/upb/$(d)/*.c)) \
              $(UPB_ROOT)/cmake/google/protobuf/descriptor.upb.c \
              $(U8_ROOT)/naive.c \
              $(U8_ROOT)/range2-sse.c \
              $(U8_ROOT)/range2-neon.c

$(if $(wildcard $(UPB_ROOT)/upb/mem/arena.c),,$(error upb sources not found; run: git submodule update --init thirdparty/upb thirdparty/utf8_range))

UPB_OBJS    = $(patsubst %.c,%.o,$(UPB_SRCS))
OBJS       += $(UPB_OBJS)
EXTRA_CLEAN += $(UPB_OBJS)

# upb headers are also needed by src/parser_protobuf.c, so the include
# paths go in the global PG_CPPFLAGS.  -I$(UPB_ROOT) resolves "upb/...",
# -I$(UPB_ROOT)/cmake resolves the generated
# "google/protobuf/descriptor.upb.h", -I$(U8_ROOT) resolves utf8_range.h.
PG_CPPFLAGS += -I$(UPB_ROOT) -I$(UPB_ROOT)/cmake -I$(U8_ROOT)

# Compile ONLY the upb/utf8_range objects with hidden visibility (keeps
# their symbols out of kafka_fdw.so's dynamic table) and -w (upb is not
# warning-clean under PG's strict flags, and it is not our code to fix).
#
# upb is production-grade third-party code and its hot loops (wire
# decode, MiniTable accessors, arena bump allocator) matter far more
# than debuggability of upb internals.  Force -O2 -DNDEBUG regardless
# of the parent PG build's CFLAGS -- a cassert/debug PG install passes
# -O0 -g3, which cripples upb's UPB_API_INLINE headers (they end up
# not inlined) and leaves every UPB_ASSERT / UPB_ASSUME running as a
# real branch on the hot path.  filter-out -O0/-O1 first because gcc
# resolves multiple -O flags with the last one winning.
$(UPB_OBJS): CFLAGS := \
    $(filter-out -O0 -O1 -g3, $(CFLAGS)) \
    -O2 -DNDEBUG -fPIC -fvisibility=hidden -w
