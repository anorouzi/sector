include ../Makefile.common

CCFLAGS += 
ifeq ($(STATIC), 1)
   LDFLAGS += libsecurity.a ../lib/libcommon.a
else
   LDFLAGS += -lsecurity -lcommon
endif

OBJS = security.o ssltransport.o

all: libsecurity.so libsecurity.a sslserver sslclient sserver ssl_cert_gen

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libsecurity.so: $(OBJS)
	$(C++) -shared -o $@ $^

libsecurity.a: $(OBJS)
	ar -rcs $@ $^

sserver: sserver.cpp
	$(C++) sserver.cpp -o sserver $(CCFLAGS) $(LDFLAGS)

sslserver: sslserver.cpp
	$(C++) sslserver.cpp -o sslserver $(CCFLAGS) $(LDFLAGS)

sslclient: sslclient.cpp
	$(C++) sslclient.cpp -o sslclient $(CCFLAGS) $(LDFLAGS)

ssl_cert_gen: ssl_cert_gen.cpp
	$(C++) ssl_cert_gen.cpp -o ssl_cert_gen -lssl -lstdc++

clean:
	rm -f *.o *.so *.a sserver sslserver sslclient

install:
	mv libsecurity.so libsecurity.a ../lib
	./ssl_cert_gen security
