include ../Makefile.common

CCFLAGS += 
ifeq ($(STATIC), 1)
   LDFLAGS += libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libudt.a ../lib/libcommon.a
else
   LDFLAGS += -lclient -lsecurity -lrpc -ludt -lcommon
endif

OBJS = client.o fsclient.o dcclient.o

all: libclient.so libclient.a testfs testdc download upload ls

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^

libclient.a: $(OBJS)
	ar -rcs $@ $^

testfs: testfs.cpp
	$(C++) testfs.cpp -o testfs $(CCFLAGS) $(LDFLAGS)

testdc: testdc.cpp
	$(C++) testdc.cpp -o testdc $(CCFLAGS) $(LDFLAGS)

download: download.cpp
	$(C++) download.cpp -o download $(CCFLAGS) $(LDFLAGS)

upload: upload.cpp
	$(C++) upload.cpp -o upload $(CCFLAGS) $(LDFLAGS)

ls: ls.cpp
	$(C++) ls.cpp -o ls $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a testfs testdc download upload ls

install:
	mv libclient.so libclient.a ../lib
