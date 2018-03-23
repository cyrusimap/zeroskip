## Silent by default

CCCOLOUR="\033[34m"
LINKCOLOUR="\033[34;1m"
SRCCOLOUR="\033[33m"
BINCOLOUR="\033[37;1m"
MAKECOLOUR="\033[32;1m"
RMCOLOUR="\033[31m"
ARCOLOUR="\033[35m"
ENDCOLOUR="\033[0m"

## Verbosity
V =
ifeq ($(strip $(V)),)
	E = @echo
	Q = @
	QCC = @printf '    %b %b\n' $(CCCOLOUR)CC$(ENDCOLOUR) $(SRCCOLOUR)$@$(ENDCOLOUR) 1>&2;
	QLD = @printf '    %b %b\n' $(LINKCOLOUR)LINK$(ENDCOLOUR) $(BINCOLOUR)$@$(ENDCOLOUR) 1>&2;
	QIN = @printf '    %b %b\n' $(LINKCOLOUR)INSTALL$(ENDCOLOUR) $(BINCOLOUR)$@$(ENDCOLOUR) 1>&2;
	QRM = @printf '    %b\n' $(RMCOLOUR)RM$(ENDCOLOUR) 1>&2;
	QAR = @printf '    %b %b\n' $(ARCOLOUR)AR$(ENDCOLOUR) $(BINCOLOUR)$@$(ENDCOLOUR) 1>&2;
	QMKDIR = @printf '    %b\n' $(RMCOLOUR)MKDIR$(ENDCOLOUR) 1>&2;
else
	E = @\#
	Q =
endif
export E Q QCC QLD QIN QMKDIR QAR

## Endianess
ENDIANESS=$(shell printf '\1' | od -dAn)

ifeq ($(strip $(ENDIANESS)),1)
	ENDIAN = -DLENDIAN
else
	ENDIAN = -DBENDIAN
endif

## Defaults
NULL=
PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install
CC=gcc
AR_LIB_FILE=libzeroskip.a
SO_LIB_FILE=

## Aliases
ZS_INSTALL=$(QIN)$(INSTALL)
ZS_RM=$(QRM)rm -rf
ZS_MKDIR=$(QMKDIR)mkdir -p
ZS_AR=$(QAR)ar

## Platform
UNAME := $(shell $(CC) -dumpmachine 2>&1 | grep -E -o "linux|darwin|solaris")

ifeq ($(UNAME), linux)
OSFLAGS = -DLINUX -D_GNU_SOURCE -std=c99
DEBUG = -ggdb
else ifeq ($(UNAME), darwin)
OSFLAGS = -DMACOSX -D_BSD_SOURCE
DEBUG = -g
else ifeq ($(UNAME), solaris)
OSFLAGS = -DSOLARIS
DEBUG = -g
endif

## Compiler options
ZS_EXTRA_CFLAGS = -mtune=native -O3 -pedantic
ZS_CFLAGS=-Wextra -Wall -W -Wno-missing-field-initializers -O0 $(CFLAGS) $(DEBUG) $(ENDIAN) $(OSFLAGS) -DZS_DEBUG
ZS_LDFLAGS=$(LDFLAGS) $(DEBUG) -lz -luuid
ZS_LIBS=
ARFLAGS=rcs

ZS_CC=$(QCC)$(CC) $(ZS_CFLAGS)
ZS_LD=$(QLD)$(CC) $(ZS_LDFLAGS)
