include $(top_srcdir)/flymake.mk

AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
AM_CFLAGS = $(AM_CPPFLAGS) $(LIBUUID_CFLAGS) $(LIBZLIB_CFLAGS)

# Enable debug flags
if IS_DEBUG
AM_CFLAGS += -ggdb
else
AM_CFLAGS += -ggdb -O2
endif

LDADD = $(top_builddir)/src/libzeroskip.la

TESTS = unit
check_PROGRAMS = unit
unit_SOURCES = \
	unit.h \
	unit.c \
	unit-memtree.c \
	unit-strarr.c \
	unit-vecu64.c \
	unit-zsdb.c \
	$(top_builddir)/include/zeroskip.h
unit_CFLAGS = @CHECK_CFLAGS@ $(AM_CFLAGS)
unit_LDADD = $(top_builddir)/src/libzeroskip.la @CHECK_LIBS@
