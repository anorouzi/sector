include ../Makefile.common

CCFLAGS += 
ifeq ($(STATIC), 1)
   LDFLAGS += libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libudt.a ../lib/libcommon.a
else
   LDFLAGS += -lclient -lsecurity -lrpc -ludt -lcommon
endif

OBJS = client.o fsclient.o dcclient.o

all: libclient.so libclient.a testfs testdc download upload ls rm mkdir sysinfo sectorfs

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

rm: rm.cpp
	$(C++) rm.cpp -o rm $(CCFLAGS) $(LDFLAGS)

mkdir: mkdir.cpp
	$(C++) mkdir.cpp -o mkdir $(CCFLAGS) $(LDFLAGS)

sysinfo: sysinfo.cpp
	$(C++) sysinfo.cpp -o sysinfo $(CCFLAGS) $(LDFLAGS)

sectorfs: sectorfs.cpp
	$(C++) sectorfs.cpp -o sectorfs $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a testfs testdc download upload ls rm mkdir sysinfo sectorfs

install:
	mv libclient.so libclient.a ../lib
