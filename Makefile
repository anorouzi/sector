SUBDIRS = udt util gmp routing metadata sql server client
TARGETS = clean all install

subdirs:
	for dir in $(SUBDIRS); do \
		cd $$dir; \
		$(MAKE); \
		$(MAKE) install; \
		cd ..; \
	done

clean:
	for dir in $(SUBDIRS); do \
		cd $$dir; \
		$(MAKE) clean; \
		cd ..; \
	done
