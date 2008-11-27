include ../../Makefile.common

CCFLAGS += -I../ -I../../udt -I../../common -I../../security -I../../gmp
LDFLAGS += -L../../lib -lclient -lsecurity -lrpc -ludt -lcommon

all: download upload ls rm mkdir sysinfo sectorfs

download: download.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

upload: upload.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

ls: ls.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

rm: rm.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

mkdir: mkdir.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

sysinfo: sysinfo.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

sectorfs: sectorfs.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o download upload ls rm mkdir sysinfo sectorfs
