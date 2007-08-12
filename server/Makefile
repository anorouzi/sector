include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lserver -lrouting -lcrypto -lrpc -ludt -lmeta -lclient -lutil -ldl

OBJS = conf.o log.o kb.o fs.o file.o index.o server.o serv_file.o serv_spe.o

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
