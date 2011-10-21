include ../Makefile.common

OBJS = osportable.o \
       conf.o \
       sfopt.o \
       snode.o \
       meta.o \
       index.o \
       memobj.o \
       transaction.o \
       topology.o \
       log.o \
       sphere.o \
       constant.o \
       crypto.o \
       routing.o \
       udttransport.o \
       ssltransport.o \
       tcptransport.o \
       datachn.o \
       threadpool.o \
       writelog.o \
       message.o

all: libcommon.so libcommon.a
test: crypto_unittest

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libcommon.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS) -ludt

libcommon.a: $(OBJS)
	ar -rcs $@ $^

crypto_unittest: crypto.h crypto.cpp all
	$(C++) $(CCFLAGS) crypto_unittest.cpp -lcommon -ludt -o $@ $(LDFLAGS)

clean:
	rm -f *.o *.so *.a

install:
	mv libcommon.so libcommon.a ../lib
