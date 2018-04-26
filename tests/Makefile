# Tests
-include ../config.mk

TEST_RESULTS_DIR = results

# BTree test
BT_OBJS = \
	bt-test.o    \
	$(NULL)
BT_TEST=bt-test
BT_LIBS+=

ZEROSKIP_INC=../src

all: test $(BT_TEST)

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

test-run:
	$(E) "Running tests..."
clean:
	$(ZS_RM) *.o \
	$(TEST_RESULTS_DIR) \
	$(BT_TEST) \
	Makefile.dep \
	$(NULL)

check-syntax:
	$(CC) $(CFLAGS) -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)

.PHONY: all clean check-sytanx

