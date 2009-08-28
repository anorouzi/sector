# Makefile to build Sector Python extension. Note that the preferred method of
# building is setup.py. See README for details.

# Sector installation directory:
SECTOR_HOME=/opt/sector
# Location of Python header files:
PYTHON_INCLUDE=/usr/include/python2.5

CCFLAGS = -fPIC -g -Wall -I $(SECTOR_HOME)/common -I $(SECTOR_HOME)/client -I $(SECTOR_HOME)/gmp -I $(SECTOR_HOME)/udt -I /usr/include/python2.5

LDFLAGS = -L $(SECTOR_HOME)/lib -lssl -lstdc++ -lpthread $(SECTOR_HOME)/lib/libclient.a $(SECTOR_HOME)/lib/libsecurity.a $(SECTOR_HOME)/lib/librpc.a $(SECTOR_HOME)/lib/libcommon.a $(SECTOR_HOME)/lib/libudt.a

all: sector_module

swig:
	swig -c++ -python sector.i

compile:
	gcc $(CCFLAGS) -c sector.cpp sector_wrap.cxx

link:
	g++ -shared sector.o sector_wrap.o -o _sector.so $(LDFLAGS)

sector_module: swig compile link

clean:
	rm -f *.o *.so sector_wrap* sector.py *.pyc *~ MANIFEST
