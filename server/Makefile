include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lserver -lrouting -lcrypto -lrpc -ludt -lmeta -lclient -lutil -ldl

OBJS = fs.o server.o file.o spe.o

all: libserver.so libserver.a sector

%.o: %.cpp server.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libserver.so: $(OBJS)
	$(C++) -shared -o $@ $^

libserver.a: $(OBJS)
	ar -rcs $@ $^

sector: sector.cpp
	$(C++) sector.cpp -o sector $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a sector

install:
	mv libserver.so libserver.a ../lib
