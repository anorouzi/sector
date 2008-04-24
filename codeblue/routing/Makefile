include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lrouting -lcrypto -lrpc -ludt

OBJS = routing.o chord.o center.o

all: librouting.so librouting.a testchord

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

librouting.so: $(OBJS)
	$(C++) -shared -o $@ $^

librouting.a: $(OBJS)
	ar -rcs $@ $^

testchord: testchord.cpp
	$(C++) testchord.cpp -o testchord $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a testchord

install:
	mv librouting.so librouting.a ../lib
