include ../../Makefile.common

CCFLAGS += -I../ -I../../udt -I../../gmp -I../../common -I../../security
LDFLAGS += -L../../lib -lclient -lsecurity -lrpc -ludt -lcommon

ifeq ($(STATIC), 1)
   LDFLAGS += -static
endif

all: testfs testdc wordcount text.idx mrsort mrword funcs

testfs: testfs.cpp
	$(C++) testfs.cpp -o testfs $(CCFLAGS) $(LDFLAGS)

testdc: testdc.cpp
	$(C++) testdc.cpp -o testdc $(CCFLAGS) $(LDFLAGS)

wordcount: wordcount.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

text.idx: text.idx.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

mrsort: mrsort.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

mrword: mrword.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

funcs: _always_check_
	cd ./funcs; make; cd ../

_always_check_:
	true

clean:
	rm -f *.o *.so *.a testfs testdc wordcount text.idx mrsort mrword
	rm -f ./funcs/*.so
