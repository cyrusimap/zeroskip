# zeroskip library
-include ../config.mk

# Object files
ZS_LIBOBJS =  			\
	btree.o			\
	crc32c.o		\
	cstring.o		\
	file-lock.o		\
	log.o			\
	mappedfile.o		\
	pqueue.o		\
	strarray.o		\
	util.o			\
	vecu64.o		\
	zeroskip.o		\
	zeroskip-active.o	\
	zeroskip-dotzsdb.o	\
	zeroskip-file.o		\
	zeroskip-filename.o	\
	zeroskip-finalised.o	\
	zeroskip-header.o	\
	zeroskip-packed.o	\
	zeroskip-record.o	\
	$(NULL)

# Headers
ZS_HDRS = \
	$(NULL)

all: clean-ar $(AR_LIB_FILE)

Makefile.dep:
	$(ZS_CC) -MM *.c > Makefile.dep 2> /dev/null || true

-include Makefile.dep

%.o:  %.c
	$(ZS_CC) -c $<

$(AR_LIB_FILE): $(ZS_LIBOBJS)
	$(ZS_AR) $(ARFLAGS) $@ $^

clean-ar:
	$(ZS_RM) $(AR_LIB_FILE)

clean:
	$(ZS_RM) $(ZS_LIBOBJS) $(AR_LIB_FILE) *.o Makefile.dep

check-syntax:
	$(CC) $(CFLAGS) -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)

.PHONY: all clean check-syntax clean-ar
