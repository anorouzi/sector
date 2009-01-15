include ../Makefile.common

CCFLAGS += -I../udt -I../security

ifeq ($(STATIC), 1)
   LDFLAGS += librpc.a ../lib/libudt.a ../lib/libsecurity.a ../lib/libcommon.a
else
   LDFLAGS += -L../lib -lrpc -ludt -lsecurity -lcommon
endif

OBJS = gmp.o message.o prec.o

all: librpc.so librpc.a gs gc

%.o: %.cpp %.h
	$(C++) -fPIC  $(CCFLAGS) $< -c

librpc.so: $(OBJS)
	$(C++) -shared -o $@ $^

librpc.a: $(OBJS)
	ar -rcs $@ $^

gs: $(OBJS) gs.cpp
	$(C++) -o gs gs.cpp $(CCFLAGS) $(LDFLAGS)

gc: $(OBJS) gc.cpp
	$(C++) -o gc gc.cpp $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a gs gc

install:
	mv librpc.so librpc.a ../lib
