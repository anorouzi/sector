include ../Makefile.common

CCFLAGS += 
LDFLAGS += 

OBJS = security.o ssltransport.o

all: libsecurity.so libsecurity.a sslserver sslclient sserver sclient

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libsecurity.so: $(OBJS)
	$(C++) -shared -o $@ $^

libsecurity.a: $(OBJS)
	ar -rcs $@ $^

sserver: sserver.cpp
	$(C++) sserver.cpp -o sserver $(CCFLAGS) $(LDFLAGS)

sclient: sclient.cpp
	$(C++) sclient.cpp -o sclient $(CCFLAGS) $(LDFLAGS)

sslserver: sslserver.cpp
	$(C++) sslserver.cpp -o sslserver $(CCFLAGS) $(LDFLAGS)

sslclient: sslclient.cpp
	$(C++) sslclient.cpp -o sslclient $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a sserver sclient sslserver sslclient

install:
	mv libsecurity.so libsecurity.a ../lib
