include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common -I@top_srcdir@/gmp

lib_LIBRARIES = libclient.a 
libclient_a_SOURCES = client_conf.cpp fscache.cpp client.cpp fsclient.cpp dcclient.cpp \
		      clientmgmt.cpp client.h clientmgmt.h dcclient.h fscache.h fsclient.h
libclient_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = libclient.la
libclient_la_SOURCES = client_conf.cpp fscache.cpp client.cpp fsclient.cpp dcclient.cpp \
		       clientmgmt.cpp client.h clientmgmt.h dcclient.h fscache.h fsclient.h
all: libclient.a libclient.la
	cp ./.libs/libclient.so libclient.a ../lib
