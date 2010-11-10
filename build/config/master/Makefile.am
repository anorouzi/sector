include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/gmp -I@top_srcdir@/security -I@top_srcdir@/udt -I@top_srcdir@/common

lib_LIBRARIES = libmaster.a 
libmaster_a_SOURCES = master_conf.cpp slavemgmt.cpp user.cpp master.cpp master.h slavemgmt.h user.h 
libmaster_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = libmaster.la
libmaster_la_SOURCES = master_conf.cpp slavemgmt.cpp user.cpp master.cpp master.h slavemgmt.h user.h

if STATIC
   MASTER_LD_FLAGS= libmaster.a ../lib/librpc.a ../lib/libsecurity.a ../lib/libcommon.a ../lib/libudt.a 
else
   MASTER_LD_FLAGS= -L../lib -lmaster -lrpc -lsecurity -lcommon -ludt
endif

bin_PROGRAMS = start_master start_all stop_all
start_master_SOURCES = start_master.cpp
start_master_DEPENDENCIES = libmaster.a libmaster.la
start_master_LDFLAGS = $(MASTER_LD_FLAGS)

start_all_SOURCES = start_all.cpp
start_all_DEPENDENCIES = libmaster.a libmaster.la
start_all_LDFLAGS = $(MASTER_LD_FLAGS)

stop_all_SOURCES = stop_all.cpp
stop_all_DEPENDENCIES = libmaster.a libmaster.la
stop_all_LDFLAGS = $(MASTER_LD_FLAGS)

all: libmaster.a libmaster.la start_master start_all stop_all
	cp .libs/libmaster.so libmaster.a ../lib
	../security/ssl_cert_gen master
