MANPAGES = \
	libzeroskip.3

HTMLPAGES = $(MANPAGES:%=%.html)

dist_man_MANS = $(MANPAGES)

if HAVE_MANDOC
doc_DATA = $(HTMLPAGES)
endif

SUFFIXES = .3 .3.html .2 .2.html

.3.3.html:
	$(AM_V_GEN) $(MANDOC) -Thtml -Ostyle=mandoc.css,man=%N.%S.html $< > $@

.2.2.html:
	$(AM_V_GEN) $(MANDOC) -Thtml -Ostyle=mandoc.css,man=%N.%S.html $< > $@


CLEANFILES=$(HTMLPAGES)
