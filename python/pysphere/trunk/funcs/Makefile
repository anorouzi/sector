include ../Makefile.common

CCFLAGS = -shared -fPIC -g -I$(SECTOR_HOME)/common -I/usr/include/python2.5 
LDFLAGS = -Xlinker -export-dynamic -Wl,-O1 -Wl,-Bsymbolic-functions
all: PySphereMRFuncs.so 

PySphereMRFuncs.so: PySphereMRFuncs.cpp
	$(C++) $(CCFLAGS) $(LDFLAGS) -o $@ $^ -lstdc++ -lpython2.5

clean:
	rm -f *.so
