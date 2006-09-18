include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lmeta

OBJS = file.o index.o data.o

all: libmeta.so libmeta.a

%.o: %.cpp %.h
	$(C++) -fPIC  $(CCFLAGS) $< -c

libmeta.so: $(OBJS)
	$(C++) -shared -o $@ $^

libmeta.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libmeta.so libmeta.a ../lib
