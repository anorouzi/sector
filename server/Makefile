include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lstore -lrouting -lcrypto -lrpc -ludt -lsql -lmeta

OBJS = store.o

all: libstore.so libstore.a cbfs

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libstore.so: $(OBJS)
	$(C++) -shared -o $@ $^

libstore.a: $(OBJS)
	ar -rcs $@ $^

cbfs: cbfs.cpp
	$(C++) cbfs.cpp -o cbfs $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a cbfs

install:
	mv libstore.so libstore.a ../lib
