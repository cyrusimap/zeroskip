AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
AM_CFLAGS = $(AM_CPPFLAGS) $(LIBUUID_CFLAGS) $(LIBZLIB_CFLAGS)

# Define _XOPEN_SOURCE
AM_CFLAGS += -D_XOPEN_SOURCE=500 -D_POSIX_C_SOURCE
if IS_DARWIN
AM_CFLAGS += -DMACOSX -DDARWIN -D_BSD_SOURCE
endif
if IS_DRAGONFLY
AM_CFLAGS += -DDRAGONFLY
endif
if IS_LINUX
AM_CFLAGS += -DLINUX -D_DEFAULT_SOURCE -D_GNU_SOURCE
endif
if IS_FREEBSD
AM_CFLAGS += -DFREEBSD
endif
if IS_NETBSD
AM_CFLAGS += -DNETBSD
endif
if IS_OPENBSD
AM_CFLAGS += -DOPENBSD
endif
if IS_SUNOS
AM_CFLAGS += -DSUNOS -DSOLARIS
endif

# Enable debug flags
if IS_DEBUG
AM_CFLAGS += -ggdb
else
AM_CFLAGS += -ggdb -O2
endif

lib_LTLIBRARIES = libzeroskip.la

libzeroskip_la_LIBADD = $(LIBUUID_LIBS) $(LIBZLIB_LIBS)
libzeroskip_la_LDFLAGS = \
	-version-info $(ZS_VERSION_LIBTOOL) \
	-no-undefined \
	-export-symbols libzeroskip.exp

libzeroskip_la_SOURCES = \
	btree.c \
	crc32c.h crc32c.c \
	cstring.h cstring.c \
	file-lock.h file-lock.c \
	htable.h htable.c \
	list.h \
	log.c \
	mappedfile.h mappedfile.c \
	pqueue.h pqueue.c \
	strarray.c \
	util.c \
	vecu64.h vecu64.c \
	zeroskip-priv.h \
	zeroskip.c \
	zeroskip-active.c \
	zeroskip-dotzsdb.c \
	zeroskip-file.c \
	zeroskip-filename.c \
	zeroskip-finalised.c \
	zeroskip-header.c \
	zeroskip-iterator.c \
	zeroskip-packed.c \
	zeroskip-record.c \
	zeroskip-transaction.c


libzeroskip_la_DEPENDENCIES = libzeroskip.exp

libzeroskip.exp: libzeroskip.symbols
	$(AM_V_GEN) sed -e '/^$$/d' $< > $@

libzeroskip.lo: revision.h

FORCE:
$(top_srcdir)/revision: FORCE
	@if (test -e $(top_srcdir)/.git && cd $(top_srcdir) \
	     && git rev-parse --verify HEAD) > revision-t 2>/dev/null \
	  && ! cmp -s revision-t $@; then \
	  mv -f revision-t $@; \
	else \
	  rm -f revision-t; \
	  if ! test -f $@; then touch $@; fi; \
	fi

revision.h: $(top_srcdir)/revision Makefile.am
	$(AM_V_GEN) echo "#define ZS_VERSION_REVISION \""`cat $(top_srcdir)/revision`"\"" > $@

BUILT_SOURCES = revision.h
EXTRA_DIST = libzeroskip.symbols $(top_srcdir)/revision
CLEANFILES = libzeroskip.exp revision.h revision-t

