include ../Makefile.common

CCFLAGS += 

OBJS = util.o conf.o log.o

all: libutil.so libutil.a

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

libutil.so: $(OBJS)
	$(C++) -shared -o $@ $^

libutil.a: $(OBJS)
	ar -rcs $@ $^

clean:
	rm -f *.o *.so *.a

install:
	mv libutil.so libutil.a ../lib
