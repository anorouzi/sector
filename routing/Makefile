include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lrouting -lcrypto -lrpc -ludt

OBJS = routing.o

all: librouting.so librouting.a chord

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

librouting.so: $(OBJS)
	$(C++) -shared -o $@ $^

librouting.a: $(OBJS)
	ar -rcs $@ $^

chord: chord.cpp
	$(C++) chord.cpp -o chord $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a chord

install:
	mv librouting.so librouting.a ../lib
