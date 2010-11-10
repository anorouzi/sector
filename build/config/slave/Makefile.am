include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/gmp -I@top_srcdir@/security -I@top_srcdir@/udt -I@top_srcdir@/common

lib_LIBRARIES = libslave.a 
libslave_a_SOURCES = slave_conf.cpp slave.cpp serv_file.cpp serv_spe.cpp slave.h 
libslave_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = libslave.la
libslave_la_SOURCES = slave_conf.cpp slave.cpp serv_file.cpp serv_spe.cpp slave.h

if STATIC
   SLAVE_LD_FLAGS= -ldl libslave.a ../lib/librpc.a ../lib/libsecurity.a ../lib/libcommon.a ../lib/libudt.a 
else
   SLAVE_LD_FLAGS= -ldl -L../lib -lslave -lrpc -lsecurity -lcommon -ludt
endif

bin_PROGRAMS = start_slave
start_slave_SOURCES = start_slave.cpp
start_slave_DEPENDENCIES = libslave.a libslave.la
start_slave_LDFLAGS = $(SLAVE_LD_FLAGS) 

SUBDIRS = sphere

all: libslave.a libslave.la start_slave
	cp .libs/libslave.so libslave.a ../lib
