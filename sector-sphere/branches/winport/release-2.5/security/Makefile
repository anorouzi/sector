include ../Makefile.common

CCFLAGS += -I../common -I../udt

ifeq ($(STATIC), 1)
   LDFLAGS += libsecurity.a ../lib/libcommon.a ../lib/libudt.a  -lssl -lcrypto
else
   LDFLAGS += -L../lib -ludt -lcommon -lsecurity -lssl -lcrypto
endif

OBJS = security.o filesrc.o

all: libsecurity.so libsecurity.a sserver ssl_cert_gen

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libsecurity.so: $(OBJS)
	$(C++) -shared -o $@ $^ -Wl,--build-id -L../lib -ludt -lcommon -lssl -lcrypto

libsecurity.a: $(OBJS)
	ar -rcs $@ $^

sserver: sserver.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

ssl_cert_gen: ssl_cert_gen.cpp
	$(C++) $^ -o $@ -lssl -lstdc++ $(LDFLAGS)

clean:
	rm -f *.o *.so *.a sserver ssl_cert_gen

install:
	mv libsecurity.so libsecurity.a ../lib
	./ssl_cert_gen security
