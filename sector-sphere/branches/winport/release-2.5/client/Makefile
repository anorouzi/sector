include ../Makefile.common

CCFLAGS += -I../udt -I../gmp -I../common -I../security

OBJS = fscache.o client.o fsclient.o dcclient.o clientmgmt.o utility.o

all: libclient.so libclient.a

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libclient.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS) -L../lib -lrpc -ludt -lcommon -lcrypto -lssl

libclient.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libclient.so libclient.a ../lib
