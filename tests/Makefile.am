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
	unit-vecu64.c \
	unit-zsdb.c \
	$(top_builddir)/include/zeroskip.h
unit_CFLAGS = @CHECK_CFLAGS@ $(AM_CFLAGS)
unit_LDADD = $(top_builddir)/src/libzeroskip.la @CHECK_LIBS@

# noinst_PROGRAMS = \
# 	bt-test \
# 	vecu64-test \
# 	zst-abort-transaction \
# 	zst-delete \
# 	zst-foreach-changes \
# 	zst-foreach-count \
# 	zst-foreach-heirarchy \
# 	zst-fetchnext-simple \
# 	zst-many \
# 	zst-multiopen

# bt_test_SOURCES = \
# 	bt-test.c

# vecu64_test_SOURCES = \
# 	vecu64-test.c

# zst_abort_transaction_SOURCES = \
# 	zst-abort-transaction.c

# zst_delete_SOURCES = \
# 	zst-delete.c

# zst_foreach_changes_SOURCES = \
# 	zst-foreach-changes.c

# zst_foreach_count_SOURCES = \
# 	zst-foreach-count.c

# zst_foreach_heirarchy_SOURCES = \
# 	zst-foreach-heirarchy.c

# zst_fetchnext_simple_SOURCES = \
# 	zst-fetchnext-simple.c

# zst_many_SOURCES = \
# 	zst-many.c

# zst_multiopen_SOURCES = \
# 	zst-multiopen.c
