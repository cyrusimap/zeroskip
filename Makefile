default: all

.DEFAULT:
	cd src && $(MAKE) $@
	cd tools && $(MAKE) $@
	cd examples && $(MAKE) $@

install:
	cd src && $(MAKE)

test:
	cd tests && $(MAKE)

example:
	cd examples && $(MAKE)

tool:
	cd tools && $(MAKE)

.PHONY:install check-syntax

check-syntax:
	$(CC) $(CFLAGS) -Wextra -pedantic -fsyntax-only $(CHK_SOURCES)
