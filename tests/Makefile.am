AM_CPPFLAGS = -I$(top_builddir)/include -I$(top_srcdir)/include
LDADD = $(top_builddir)/src/libzeroskip.la

bin_PROGRAMS = \
	bt-test \
	zst-abort-transaction \
	zst-foreach-count \
	zst-foreach-heirarchy

bt_test_SOURCES = \
	bt-test.c

zst_abort_transaction_SOURCES = \
	zst-abort-transaction.c

zst_foreach_count_SOURCES = \
	zst-foreach-count.c

zst_foreach_heirarchy_SOURCES = \
	zst-foreach-heirarchy.c

