include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/client -I@top_srcdir@/common -I@top_srcdir@/udt

if STATIC
   FUSE_LD_FLAGS = ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
else
   FUSE_LD_FLAGS = -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

bin_PROGRAMS = sector_fuse
sector_fuse_SOURCES = sectorfs.cpp  sectorfs.h  sector-fuse.cpp 
sector_fuse_CXXFLAGS = -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=26
sector_fuse_LDFLAGS = $(FUSE_LD_FLAGS) -lfuse

