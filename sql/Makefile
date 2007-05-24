include ../Makefile.common

CCFLAGS += 
LDFLAGS += -lmeta -lsql -ludt

OBJS = sql.o table.o

all: libsql.so libsql.a test

%.o: %.cpp %.h
	$(C++) -fPIC  $(CCFLAGS) $< -c

libsql.so: $(OBJS)
	$(C++) -shared -o $@ $^

libsql.a: $(OBJS)
	ar -rcs $@ $^

test: $(OBJS) test.cpp
	$(C++) -o test test.cpp $(CCFLAGS) $(LDFLAGS)

clean:
	rm -f *.o *.so *.a test

install:
	mv libsql.so libsql.a ../lib
