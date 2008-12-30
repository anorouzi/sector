include ../Makefile.common

CCFLAGS += -D LINUX -D IA32 -I../security
LDFLAGS += 

OBJS = md5.o common.o window.o list.o buffer.o packet.o channel.o queue.o core.o co-op.o api.o ccc.o transport.o

all: libudt.so libudt.a

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libudt.so: $(OBJS)
	$(C++) -shared -o $@ $^

libudt.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libudt.so libudt.a ../lib
