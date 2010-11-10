include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common

lib_LIBRARIES = libcommon.a 
libcommon_a_SOURCES =  osportable.cpp conf.cpp sfopt.cpp snode.cpp meta.cpp index.cpp \
       		       index2.cpp memobj.cpp transaction.cpp topology.cpp log.cpp  sphere.cpp \
       		       constant.cpp crypto.cpp dhash.cpp routing.cpp udttransport.cpp \
       		       ssltransport.cpp tcptransport.cpp  datachn.cpp threadpool.cpp \
       		       writelog.cpp crypto.h dhash.h index.h meta.h ssltransport.h  \
		       threadpool.h transaction.h writelog.h datachn.h index2.h  \
		       log.h routing.h tcptransport.h topology.h udttransport.h

libcommon_a_CXXFLAGS = -fPIC

lib_LTLIBRARIES = libcommon.la
libcommon_la_SOURCES =  osportable.cpp conf.cpp sfopt.cpp snode.cpp meta.cpp index.cpp \
       		        index2.cpp memobj.cpp transaction.cpp topology.cpp log.cpp  sphere.cpp \
       		        constant.cpp crypto.cpp dhash.cpp routing.cpp udttransport.cpp \
       		        ssltransport.cpp tcptransport.cpp datachn.cpp threadpool.cpp \
       		        writelog.cpp crypto.h dhash.h index.h meta.h ssltransport.h  \
		        threadpool.h transaction.h writelog.h datachn.h index2.h  \
		        log.h routing.h tcptransport.h topology.h udttransport.h
all: libcommon.a libcommon.la
	cp libcommon.a ../lib
	cp .libs/libcommon.so ../lib/libcommon.so
