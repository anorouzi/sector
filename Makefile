SUBDIRS = udt comm log routing metadata sql server client
TARGETS = clean all install

subdirs:
	for dir in $(SUBDIRS); do \
		cd $$dir; \
		$(MAKE) $(TARGETS); \
		cd ..; \
	done
