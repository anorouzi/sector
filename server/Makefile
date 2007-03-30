include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lserver -lrouting -lcrypto -lrpc -ludt -lsql -lmeta -dl

OBJS = server.o file.o sql.o spe.o

all: libserver.so libserver.a cbfs

%.o: %.cpp server.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libserver.so: $(OBJS)
	$(C++) -shared -o $@ $^

libserver.a: $(OBJS)
	ar -rcs $@ $^

cbfs: cbfs.cpp
	$(C++) cbfs.cpp -o cbfs $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a cbfs

install:
	mv libserver.so libserver.a ../lib
