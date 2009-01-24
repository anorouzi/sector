include ../Makefile.common

CCFLAGS += -I../udt

OBJS = conf.o index.o topology.o sysstat.o log.o sphere.o constant.o

all: libcommon.so libcommon.a

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libcommon.so: $(OBJS)
	$(C++) -shared -o $@ $^

libcommon.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libcommon.so libcommon.a ../lib
