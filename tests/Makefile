# Tests
-include ../config.mk

TEST_RESULTS_DIR = results

# BTree test
BT_OBJS = \
	bt-test.o    \
	$(NULL)
BT_TEST=bt-test
BT_LIBS+=

# zs foreach test
ZST_FOREACH_COUNT_OBJS = \
	zst-foreach-count.o	\
	$(NULL)
ZST_FOREACH_COUNT=zst-foreach-count

ZST_FOREACH_HEIRARCHY_OBJS = \
	zst-foreach-heirarchy.o	\
	$(NULL)
ZST_FOREACH_HEIRARCHY=zst-foreach-heirarchy

# zs abort test
ZST_ABORT_TRANSACTION_OBJS = \
	zst-abort-transaction.o \
	$(NULL)
ZST_ABORT_TRANSACTION=zst-abort-transaction

# Zeroskip Test Libs
ZST_LIBS+= $(UUID_LIBS) $(ZLIB_LIBS)

ZEROSKIP_INC=../src

all: test $(BT_TEST) \
	$(ZST_FOREACH_COUNT)  	\
	$(ZST_FOREACH_HEIRARCHY) \
	$(ZST_ABORT_TRANSACTION) \
	$(NULL)

test: clean prep

prep:
	$(ZS_MKDIR) -p $(TEST_RESULTS_DIR)

Makefile.dep:
	$(ZS_CC) -MM *.c > Makefile.dep 2> /dev/null || true

-include Makefile.dep

%.o:  %.c
	$(ZS_CC) -I$(ZEROSKIP_INC) -c $<

$(BT_TEST): $(BT_OBJS)
	$(ZS_LD) -I$(ZEROSKIP_INC) -o $@ $^ ../src/$(AR_LIB_FILE) $(BT_LIBS)

$(ZST_FOREACH_COUNT): $(ZST_FOREACH_COUNT_OBJS)
	$(ZS_LD) -I$(ZEROSKIP_INC) -o $@ $^ ../src/$(AR_LIB_FILE) $(ZST_LIBS)

$(ZST_FOREACH_HEIRARCHY): $(ZST_FOREACH_HEIRARCHY_OBJS)
	$(ZS_LD) -I$(ZEROSKIP_INC) -o $@ $^ ../src/$(AR_LIB_FILE) $(ZST_LIBS)

$(ZST_ABORT_TRANSACTION): $(ZST_ABORT_TRANSACTION_OBJS)
	$(ZS_LD) -I$(ZEROSKIP_INC) -o $@ $^ ../src/$(AR_LIB_FILE) $(ZST_LIBS)

test-run:
	$(E) "Running tests..."
clean:
	$(ZS_RM) *.o \
	$(TEST_RESULTS_DIR) \
	$(BT_TEST) \
	$(ZST_FOREACH_COUNT) \
	$(ZST_FOREACH_HEIRARCHY) \
	Makefile.dep \
	$(NULL)

check-syntax:
	$(CC) $(CFLAGS) -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)

.PHONY: all clean check-sytanx

