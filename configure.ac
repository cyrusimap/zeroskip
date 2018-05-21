# version
m4_define([zs_version_major],[0])
m4_define([zs_version_minor],[1])
m4_define([zs_version_micro],[0])
m4_define([zs_version_suffix],[devel])
m4_define([zs_version], zs_version_major.zs_version_minor.zs_version_micro[]m4_ifset([zs_version_suffix],-[zs_version_suffix]))

# libtool version
m4_define([zs_version_lt_current],[0])
m4_define([zs_version_lt_revision],[0])
m4_define([zs_version_lt_age],[0])

# autoconf intialisation
AC_PREREQ([2.60])
AC_INIT([libzeroskip],[zs_version])
AC_CONFIG_SRCDIR([config.h.in])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# automake initialisation
AM_INIT_AUTOMAKE([-Wall -Werror foreign])

# fix for automake >= 1.12
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])

# libtool intialisation
LT_PREREQ([2.2.0])
LT_INIT

# zeroskip tool
AC_ARG_ENABLE([tool],
	[AS_HELP_STRING([--enable-tool=@<:@yes/no@:>@],
		[Build the zeroskip tool @<:@default=yes@:>@])],
	[], [enable_tool=yes])
AM_CONDITIONAL([ENABLE_TOOL], [test "x$enable_tool" = "xyes"])

# documentation
AC_ARG_ENABLE([doc],
	[AS_HELP_STRING([--enable-doc=@<:@yes/no@:>@],
		[Build documentation @<:@default=yes@:>@])],
	[], [enable_doc=yes])
AM_CONDITIONAL([ENABLE_DOC], [test "x$enable_doc" = "xyes"])

# examples
AC_ARG_ENABLE([examples],
	[AS_HELP_STRING([--enable-examples=@<:@yes/no@:>@],
		[Build example applications @<:@default=yes@:>@])],
	[], [enable_examples=yes])
AM_CONDITIONAL([ENABLE_EXAMPLES], [test "x$enable_examples" = "xyes"])

# tests
AC_ARG_ENABLE([tests],
	[AS_HELP_STRING([--enable-tests=@<:@yes/no@:>@],
		[Build the tests @<:@default=yes@:>@])],
	[], [enable_tests=yes])
AM_CONDITIONAL([ENABLE_TESTS], [test "x$enable_tests" = "xyes"])

# check for programs
AC_PROG_CC
AC_PROG_CC_C99
AC_CHECK_PROGS([DOXYGEN], [doxygen])
AC_CHECK_PROGS([MANDOC], [mandoc])

AM_CONDITIONAL([HAVE_DOXYGEN], [test -n "$DOXYGEN"])
AM_CONDITIONAL([HAVE_MANDOC], [test -n "$MANDOC"])

# silent build rules
m4_ifdef([AM_SILENT_RULES],[AM_SILENT_RULES([yes])])


# check for dependencies
DEPENDENCIES=""

# check for libuuid
AC_SUBST(LIBUUID_CFLAGS)
AC_SUBST(LIBUUID_LIBS)
if pkg-config uuid; then
   LIBUUID_CFLAGS=`pkg-config --cflags uuid`
   LIBUUID_LIBS=`pkg-config --libs uuid`
   AC_DEFINE([HAVE_LIBUUID], [1], [libuuid library])
   DEPENDENCIES="$DEPENDENCIES libuuid"
else
   AC_MSG_ERROR("You need libuuid to be able to build libzeroskip")
fi

# check for libzlib
AC_SUBST(LIBZLIB_CFLAGS)
AC_SUBST(LIBZLIB_LIBS)
if pkg-config zlib; then
   LIBZLIB_CFLAGS=`pkg-config --cflags zlib`
   LIBZLIB_LIBS=`pkg-config --libs zlib`
   AC_DEFINE([HAVE_LIBZLIB], [1], [libzlib library])
   DEPENDENCIES="$DEPENDENCIES libzlib"
else
   AC_MSG_ERROR("You need libzlib to be able to build libzeroskip")
fi

AC_SUBST([DEPENDENCIES])


# check for header file
AC_CHECK_HEADERS([getopt.h])

# check for structures
AC_CHECK_MEMBERS([struct stat.st_mtim.tv_nsec],,,[
#include <time.h>
])

# check for library functions
AC_CHECK_FUNCS([getopt_long])
AC_CHECK_FUNCS([memmem])

# checks for supported compiler options
AX_APPEND_COMPILE_FLAGS([ \
	-pedantic \
	-Wall \
	-Wextra \
	-Wshadow \
	-Wrestrict \
	-Wformat=2 \
	-Wwrite-strings \
	-Wcast-qual \
	-Wpointer-arith \
	-Wstrict-prototypes \
	-Wmissing-prototypes \
	-Wmissing-declarations \
	-Wno-unused-parameter \
])


# versioning
AC_SUBST([ZS_VERSION],[zs_version])
AC_SUBST([ZS_VERSION_MAJOR],[zs_version_major])
AC_SUBST([ZS_VERSION_MINOR],[zs_version_minor])
AC_SUBST([ZS_VERSION_MICRO],[zs_version_micro])
AC_SUBST([ZS_VERSION_LIBTOOL],[zs_version_lt_current:zs_version_lt_revision:zs_version_lt_age])

# version suffix
m4_ifset([zs_version_suffix],[
	AC_DEFINE(HAVE_VERSION_SUFFIX, [1], [Define if a version suffix is present.])
])


AC_CONFIG_FILES([
   libzeroskip.pc
   Makefile
   include/Makefile
   include/libzeroskip/Makefile
   include/libzeroskip/version.h
   src/Makefile
   doc/Makefile
   doc/doxygen.cfg
   doc/man/Makefile
   tool/Makefile
   examples/Makefile
   tests/Makefile
])
AC_OUTPUT
