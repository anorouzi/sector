include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common

bin_PROGRAMS = testfs testdc wordcount text_idx mrsort mrword

if STATIC
      EXAMPLE_LD_FLAGS = ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
else
      EXAMPLE_LD_FLAGS= -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

testfs_SOURCES = testfs.cpp
testfs_LDFLAGS = $(EXAMPLE_LD_FLAGS)
testdc_SOURCES = testdc.cpp
testdc_LDFLAGS = $(EXAMPLE_LD_FLAGS)
wordcount_SOURCES = wordcount.cpp
wordcount_LDFLAGS = $(EXAMPLE_LD_FLAGS)
text_idx_SOURCES = text_idx.cpp
text_idx_LDFLAGS = $(EXAMPLE_LD_FLAGS)
mrsort_SOURCES = mrsort.cpp
mrsort_LDFLAGS = $(EXAMPLE_LD_FLAGS)
mrword_SOURCES = mrword.cpp
mrword_LDFLAGS = $(EXAMPLE_LD_FLAGS)
SUBDIRS = funcs
