include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt 

if AMD64
   AM_CXXFLAGS = -D AMD64
else
   AM_CXXFLAGS = -D IA32
endif

lib_LIBRARIES = libudt.a 
libudt_a_SOURCES = common.cpp common.h window.cpp window.h list.cpp list.h \
		   buffer.cpp buffer.h packet.cpp packet.h channel.cpp channel.h \
		   queue.cpp queue.h core.cpp core.h cache.cpp cache.h epoll.cpp \
		   api.cpp api.h ccc.cpp ccc.h udt.h md5.cpp md5.h 
libudt_a_CXXFLAGS = -D LINUX -fPIC

lib_LTLIBRARIES = libudt.la
libudt_la_SOURCES = common.cpp common.h window.cpp window.h list.cpp list.h \
		    buffer.cpp buffer.h packet.cpp packet.h channel.cpp channel.h \
		    queue.cpp queue.h core.cpp core.h cache.cpp cache.h epoll.cpp \
		    api.cpp api.h ccc.cpp ccc.h udt.h md5.cpp md5.h
libudt_la_CXXFLAGS = -D LINUX
all: libudt.a libudt.la
	mv libudt.a ../lib
	cp .libs/libudt.so ../lib/libudt.so
