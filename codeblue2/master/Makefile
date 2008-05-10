include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lmaster

OBJS = master.o

all: libmaster.so libmaster.a start_master

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

libmaster.so: $(OBJS)
	$(C++) -shared -o $@ $^

libmaster.a: $(OBJS)
	ar -rcs $@ $^

start_master: start_master.cpp
	$(C++) start_master.cpp -o start_master $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a start_master

install:
	mv libmaster.so libmaster.a ../lib
