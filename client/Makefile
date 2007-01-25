include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lclient -lrpc -ludt -lsql -lmeta

OBJS = fsclient.o

all: libclient.so libclient.a test download upload ls

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^

libclient.a: $(OBJS)
	ar -rcs $@ $^

test: test.cpp
	$(C++) test.cpp -o test $(CCFLAGS) $(LDFLAGS)

download: download.cpp
	$(C++) download.cpp -o download $(CCFLAGS) $(LDFLAGS)

upload: upload.cpp
	$(C++) upload.cpp -o upload $(CCFLAGS) $(LDFLAGS)

ls: ls.cpp
	$(C++) ls.cpp -o ls $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a test download upload ls

install:
	mv libclient.so libclient.a ../lib
