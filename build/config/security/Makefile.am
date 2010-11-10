include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common

lib_LIBRARIES = libsecurity.a 
libsecurity_a_SOURCES = filesrc.cpp filesrc.h security.h security.cpp  
libsecurity_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = libsecurity.la
libsecurity_la_SOURCES = filesrc.cpp filesrc.h security.h security.cpp

bin_PROGRAMS = sserver ssl_cert_gen
sserver_SOURCES = sserver.cpp
sserver_DEPENDENCIES = libsecurity.a 
ssl_cert_gen_SOURCES = ssl_cert_gen.cpp 
ssl_cert_gen_DEPENDENCIES = libsecurity.a   
if STATIC
   sserver_LDFLAGS = libsecurity.a ../lib/libcommon.a ../lib/libudt.a
   ssl_cert_gen_LDFLAGS = libsecurity.a ../lib/libcommon.a ../lib/libudt.a
else
   sserver_LDFLAGS = -L../lib -ludt -lcommon -lsecurity
   ssl_cert_gen_LDFLAGS = -L../lib -ludt -lcommon -lsecurity
endif

all:libsecurity.a libsecurity.la sserver ssl_cert_gen
	cp libsecurity.a ../lib
	cp .libs/libsecurity.so ../lib/libsecurity.so
