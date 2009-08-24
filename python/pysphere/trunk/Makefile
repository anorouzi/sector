include ./Makefile.common

CCFLAGS = -I. -I$(SECTOR_HOME)/udt -I$(SECTOR_HOME)/client -I$(SECTOR_HOME)/gmp -I$(SECTOR_HOME)/common -I$(SECTOR_HOME)/security
LDFLAGS = -lstdc++ -lpthread -lssl -L. -L$(SECTOR_HOME)/lib

ifeq ($(DEBUG), 1)
   CCFLAGS += -Wall -g
endif

ifeq ($(STATIC), 1)
   LDFLAGS += $(SECTOR_HOME)/lib/libclient.a $(SECTOR_HOME)/lib/libsecurity.a $(SECTOR_HOME)/lib/librpc.a $(SECTOR_HOME)/lib/libcommon.a $(SECTOR_HOME)/lib/libudt.a
else
   LDFLAGS += -lclient -lsecurity -lrpc -ludt -lcommon
endif

all: PySphereMR processing_funcs

%.o: %.cpp %.h
	$(C++) -fPIC $(CCFLAGS) $< -c

PySphereMR: PySphereMR.cpp
	$(C++) PySphereMR.cpp -o PySphereMR $(CCFLAGS) $(LDFLAGS)

processing_funcs:
	cd funcs; make; cd ../

clean:
	rm -f *.o *.so *.a PySphereMR *~
	rm -f ./funcs/*.so ./funcs/*~

