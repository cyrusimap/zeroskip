# Tool for zeroskip
-include ../config.mk

TOOL_OBJS =			\
	cmd.o			\
	cmd-batch.o		\
	cmd-consistent.o	\
	cmd-damage.o		\
	cmd-delete.o		\
	cmd-dump.o		\
	cmd-get.o		\
	cmd-repack.o		\
	cmd-new.o		\
	cmd-set.o		\
	cmd-show.o		\
	cmd-info.o		\
	zeroskip.o 		\
	$(NULL)

TOOL_HDRS = 	\
	cmds.h 	\
	$(NULL)

LIBS=

DBTOOL=zeroskip

ZEROSKIP_INC=../src

all: $(DBTOOL)

Makefile.dep:
	$(ZS_CC) -MM *.c > Makefile.dep 2> /dev/null || true

-include Makefile.dep

%.o: %.c
	$(ZS_CC) -I$(ZEROSKIP_INC) -c $<

$(DBTOOL): $(TOOL_OBJS)
	$(ZS_LD) -I$(ZEROSKIP_INC) -o $@ $^ ../src/$(AR_LIB_FILE) $(LIBS) $(ZS_LDFLAGS)

clean:
	$(ZS_RM) $(TOOL_OBJS) $(DBTOOL) *.o Makefile.dep

check-syntax:
	$(CC) $(CFLAGS) -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)

.PHONY: all clean check-syntax

