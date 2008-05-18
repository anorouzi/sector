include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lmaster

OBJS = topology.o master.o

all: libmaster.so libmaster.a start_master start_all

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libmaster.so: $(OBJS)
	$(C++) -shared -o $@ $^

libmaster.a: $(OBJS)
	ar -rcs $@ $^

start_master: start_master.cpp
	$(C++) start_master.cpp -o start_master $(CCFLAGS) $(LDFLAGS)

start_all: start_all.cpp
	$(C++) start_all.cpp -o start_all $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a start_master start_all

install:
	mv libmaster.so libmaster.a ../lib
