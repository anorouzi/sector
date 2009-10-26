include ../Makefile.common

CCFLAGS += -I../udt

ifeq ($(STATIC), 1)
   LDFLAGS += ../lib/libcommon.a ../lib/libudt.a
else
   LDFLAGS += -L../lib -lcommon -ludt
endif

OBJS = conf.o snode.o index.o index2.o topology.o sysstat.o log.o sphere.o constant.o crypto.o transport.o datachn.o dhash.o routing.o

all: libcommon.so libcommon.a testcrypto

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libcommon.so: $(OBJS)
	$(C++) -shared -o $@ $^

libcommon.a: $(OBJS)
	ar -rcs $@ $^

testcrypto: testcrypto.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a testcrypto

install:
	mv libcommon.so libcommon.a ../lib
