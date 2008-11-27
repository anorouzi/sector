include ../../Makefile.common

CCFLAGS += -I../ -I../../udt -I../../gmp -I../../common -I../../security
LDFLAGS += -L../ -L../../lib -lsecurity -lrpc -ludt -lcommon -lclient

all: testfs testdc wordcount text.idx mrsort mrword funcs

testfs: testfs.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

testdc: testdc.cpp
	$(C++) $^ -o $@ $(CCFLAGS) $(LDFLAGS)

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
