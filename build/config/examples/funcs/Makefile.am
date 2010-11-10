include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt 
AM_CXXFLAGS = -shared -fPIC -O3 -g  -DGAMEPCH=0 -DSCRIPTPCH=0
bin_PROGRAMS = randwriter.so sort.so sorthash.so wordbucket.so gen_idx.so mr_sort.so mr_word.so
randwriter_so_SOURCES = randwriter.cpp 
randwriter_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
sort_so_SOURCES = sort.cpp 
sort_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
sorthash_so_SOURCES = sorthash.cpp 
sorthash_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
wordbucket_so_SOURCES = wordbucket.cpp 
wordbucket_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
gen_idx_so_SOURCES = gen_idx.cpp 
gen_idx_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
mr_sort_so_SOURCES = mr_sort.cpp 
mr_sort_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a
mr_word_so_SOURCES = mr_word.cpp 
mr_word_so_LDFLAGS = -lstdc++ @top_srcdir@/lib/libcommon.a @top_srcdir@/lib/libudt.a

SUBDIRS=funcs
