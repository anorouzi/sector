include ../Makefile.common

OBJS = conf.o \
       snode.o \
       meta.o \
       index.o \
       index2.o \
       memobj.o \
       transaction.o \
       topology.o \
       log.o \
       sphere.o \
       constant.o \
       crypto.o \
       dhash.o \
       routing.o \
       udttransport.o \
       ssltransport.o \
       tcptransport.o \
       datachn.o \
       threadpool.o \
       writelog.o

all: libcommon.so libcommon.a

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libcommon.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS) -lcrypto -ludt -L../lib

libcommon.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libcommon.so libcommon.a ../lib
