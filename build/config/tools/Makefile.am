include @top_srcdir@/Rules

INCLUDES = -I. -I@top_srcdir@/include -I@top_srcdir@/udt -I@top_srcdir@/common

bin_PROGRAMS = sector_download sector_upload sector_ls sector_rm sector_mkdir sector_stat \
	       sector_sysinfo sector_mv sector_cp sector_pipe sector_shutdown sector_check_replica \
	       sphere_stream

if STATIC
      TOOL_LD_FLAGS = ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
else
      TOOL_LD_FLAGS= -L../lib -lsecurity -lrpc -ludt -lcommon -lclient
endif

CLINET_SHARE_LIBS = ../lib/libclient.a ../lib/libsecurity.a ../lib/librpc.a ../lib/libcommon.a ../lib/libudt.a
CLINET_STATIC_LIBS = ../lib/libclient.so ../lib/libsecurity.so ../lib/librpc.so ../lib/libcommon.so ../lib/libudt.so
sector_download_SOURCES = download.cpp
sector_download_LDFLAGS = $(TOOL_LD_FLAGS)
sector_download_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS) 
sector_upload_SOURCES = upload.cpp
sector_upload_LDFLAGS = $(TOOL_LD_FLAGS)
sector_upload_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_ls_SOURCES = ls.cpp
sector_ls_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_ls_LDFLAGS = $(TOOL_LD_FLAGS)
sector_rm_SOURCES = rm.cpp
sector_rm_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_rm_LDFLAGS = $(TOOL_LD_FLAGS)
sector_mkdir_SOURCES = mkdir.cpp
sector_mkdir_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_mkdir_LDFLAGS = $(TOOL_LD_FLAGS)
sector_stat_SOURCES = stat.cpp 
sector_stat_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_stat_LDFLAGS = $(TOOL_LD_FLAGS)
sector_sysinfo_SOURCES = sysinfo.cpp
sector_sysinfo_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_sysinfo_LDFLAGS = $(TOOL_LD_FLAGS)
sector_mv_SOURCES = mv.cpp
sector_mv_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_mv_LDFLAGS = $(TOOL_LD_FLAGS)
sector_cp_SOURCES = cp.cpp
sector_cp_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_cp_LDFLAGS = $(TOOL_LD_FLAGS)
sector_pipe_SOURCES = pipe.cpp
sector_pipe_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_pipe_LDFLAGS = $(TOOL_LD_FLAGS) 
sector_shutdown_SOURCES = shutdown.cpp
sector_shutdown_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_shutdown_LDFLAGS = $(TOOL_LD_FLAGS)
sector_check_replica_SOURCES = check_replica.cpp
sector_check_replica_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sector_check_replica_LDFLAGS = $(TOOL_LD_FLAGS)
sphere_stream_SOURCES = stream.cpp probot.cpp probot.h
sphere_stream_DEPENDENCIES = $(CLINET_SHARE_LIBS) $(CLINET_STATIC_LIBS)
sphere_stream_LDFLAGS = $(TOOL_LD_FLAGS)
