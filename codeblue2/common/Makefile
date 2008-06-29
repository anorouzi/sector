include ../Makefile.common

CCFLAGS +=
LDFLAGS +=

OBJS = conf.o index.o sysstat.o

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
