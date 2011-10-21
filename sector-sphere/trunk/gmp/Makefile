include ../Makefile.common

CCFLAGS += -I../udt
LDFLAGS += -L../lib -ludt

OBJS = gmp.o dhash.o prec.o udt_helper.o

all: librpc.so librpc.a test

%.o: %.cpp
	$(C++) -fPIC $(CCFLAGS) $< -c

librpc.so: $(OBJS)
	$(C++) -shared -o $@ $^ $(LDFLAGS)

librpc.a: $(OBJS)
	ar -rcs $@ $^

test: test.cpp $(OBJS)
	$(C++) test.cpp -o $@ $(OBJS) $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a

install:
	mv librpc.so librpc.a ../lib
