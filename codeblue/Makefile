SUBDIRS = udt gmp routing client server
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
