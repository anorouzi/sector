include ../Makefile.common

CCFLAGS += -I../client

ifeq ($(STATIC), 1)
   LDFLAGS += ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a 
else
   LDFLAGS += -L../lib -lclient -lcommon -lsecurity -lrpc -ludt 
endif
LDFLAGS += -lssl -lcrypto

APP = testfs testdc wordcount text.idx mrsort mrword

all: $(APP) funcs

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
	rm -f *.o *.so *.a $(APP) ./funcs/*.so

install:
	
