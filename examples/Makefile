#examples
-include ../config.mk

ZEROSKIP_INC=../src

all:

Makefile.dep:
	$(ZS_CC) -MM *.c > Makefile.dep 2> /dev/null || true

-include Makefile.dep

%.o:  %.c
	$(ZS_CC) -I$(ZEROSKIP_INC) -c $<

clean:
	$(ZS_RM) *.o Makefile.dep

.PHONY: all clean check-syntax
