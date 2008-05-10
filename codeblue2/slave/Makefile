include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lslave

OBJS = slave.o serv_file.o serv_spe.o

all: libslave.so libslave.a start_slave

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libslave.so: $(OBJS)
	$(C++) -shared -o $@ $^

libslave.a: $(OBJS)
	ar -rcs $@ $^

start_slave: start_slave.cpp
	$(C++) start_slave.cpp -o start_slave $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a start_slave

install:
	mv libslave.so libslave.a ../lib
