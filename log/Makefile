include ../Makefile.common

CCFLAGS += 
LDFLAGS += -llog

OBJS = log.o kb.o

all: liblog.so liblog.a

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

liblog.so: $(OBJS)
	$(C++) -shared -o $@ $^

liblog.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv liblog.so liblog.a ../lib
