include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common

lib_LIBRARIES = librpc.a 
librpc_a_SOURCES = gmp.cpp gmp.h message.cpp message.h prec.cpp prec.h 
librpc_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = librpc.la
librpc_la_SOURCES = gmp.cpp gmp.h message.cpp message.h prec.cpp prec.h

all: librpc.a librpc.la
	cp .libs/librpc.so librpc.a ../lib
