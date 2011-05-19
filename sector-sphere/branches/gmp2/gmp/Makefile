include ../Makefile.common

CCFLAGS += -I../udt -I../common
LDFLAGS += -L../lib -ludt -lcommon

OBJS = gmp.o message.o prec.o

all: librpc.so librpc.a test

%.o: %.cpp %.h
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
