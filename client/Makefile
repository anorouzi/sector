include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lclient -lrpc -ludt -lsql -lmeta

OBJS = client.o fsclient.o sqlclient.o speclient.o

all: libclient.so libclient.a testfs testsql testspe download upload 

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^

libclient.a: $(OBJS)
	ar -rcs $@ $^

testfs: testfs.cpp
	$(C++) testfs.cpp -o testfs $(CCFLAGS) $(LDFLAGS)

testsql: testsql.cpp
	$(C++) testsql.cpp -o testsql $(CCFLAGS) $(LDFLAGS)

testspe: testspe.cpp
	$(C++) testspe.cpp -o testspe $(CCFLAGS) $(LDFLAGS)

download: download.cpp
	$(C++) download.cpp -o download $(CCFLAGS) $(LDFLAGS)

upload: upload.cpp
	$(C++) upload.cpp -o upload $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a testfs testsql download upload

install:
	mv libclient.so libclient.a ../lib
