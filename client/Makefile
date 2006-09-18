include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lclient -lrouting -lcrypto -lrpc -ludt -llog -lsql -lmeta -lstore

OBJS = fsclient.o

all: libclient.so libclient.a test

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^

libclient.a: $(OBJS)
	ar -rcs $@ $^

test: test.cpp
	$(C++) test.cpp -o test $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a test

install:
	mv libclient.so libclient.a ../lib
