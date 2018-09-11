SUBDIRS = include src

if ENABLE_TOOL
SUBDIRS += tool
endif

if ENABLE_EXAMPLES
SUBDIRS += examples
endif

if ENABLE_TESTS
SUBDIRS += tests
endif

if ENABLE_BENCH
SUBDIRS += benchmark
endif

if ENABLE_DOC
SUBDIRS += doc
endif

AM_MAKEFLAGS = -s
ACLOCAL_AMFLAGS = -I m4

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libzeroskip.pc

EXTRA_DIST = \
	libzeroskip.pc.in

