%ifarch x86_64
%define libs_dir /usr/lib64
%else
%define libs_dir /usr/lib
%endif

Name: sector
Version: 2.5
Release: 0
Group: System Environment/Daemons
License: BSD
Source: sector.2.5.tar.gz
BuildRoot: %{_tmppath}/%{name}-root
BuildPrereq: libstdc++-devel, openssl-devel, fuse-libs, fuse-devel
Requires: openssl

Summary: Distributed data processing engine

%description
Sector/Sphere supports distributed data storage, distribution, and processing over large clusters of commodity computers. Sector is a high performance, scalable,
and secure distributed file system. Sphere is a high performance parallel data processing engine that can process Sector data files with very simple programming
interfaces.

# Devel
%package -n sector_sphere-devel
Summary: Sector/sphere headers and libs
Group: System Environment/Base
%description -n sector_sphere-devel
Sector/sphere API headers and libraries


# sector-sphere 
%package -n sector_sphere
Summary: Sector/sphere package
Group: System Environment/Base

%description -n sector_sphere
Sector/sphere packages

# Build process
%prep
%setup -q -n sector-sphere

%build
rm -f conf/*.{cert,key}
make
( cd fuse ; make )

%clean
rm -rf %buildroot


%install 
rm -rf %buildroot
mkdir -p $RPM_BUILD_ROOT/opt/sector

# libs
install -d $RPM_BUILD_ROOT/%libs_dir
install -m 0755 lib/lib{client,common,rpc,security,udt}.so $RPM_BUILD_ROOT/%libs_dir

# tools
install -d $RPM_BUILD_ROOT/usr/local/bin
install -d $RPM_BUILD_ROOT/usr/local/sbin
install -m 0755 tools/sector_{cp,download,ls,mkdir,mv,pipe,rm,stat,sysinfo,upload} $RPM_BUILD_ROOT/usr/local/bin
install -m 0755 tools/sphere_stream $RPM_BUILD_ROOT/usr/local/bin

install -m 0755 fuse/sector-fuse $RPM_BUILD_ROOT/usr/local/bin/sector_fuse
install -m 0755 security/ssl_cert_gen $RPM_BUILD_ROOT/usr/local/sbin/sector_ssl_cert_gen

# config
install -d $RPM_BUILD_ROOT/opt/sector/{conf,tmp}
install -m 0644 conf/{client,master,master_acl,replica,slave,slave_acl,topology}.conf $RPM_BUILD_ROOT/opt/sector/conf

export SECTOR_HOME=/opt/sector

install -d $RPM_BUILD_ROOT/opt/sector/conf/users
install -m 0644 conf/users/{anonymous,root,test} $RPM_BUILD_ROOT/opt/sector/conf/users


# sserver
install -m 0755 security/sserver $RPM_BUILD_ROOT/usr/local/sbin/sector_security

# master
install -m 0755 master/start_master $RPM_BUILD_ROOT/usr/local/sbin/sector_master
install -m 0755 master/start_all $RPM_BUILD_ROOT/usr/local/sbin/sector_start_all
install -m 0755 master/stop_all $RPM_BUILD_ROOT/usr/local/sbin/sector_stop_all

# slave
install -m 0755 slave/start_slave $RPM_BUILD_ROOT/usr/local/sbin/sector_slave
install -d $RPM_BUILD_ROOT/opt/sector/slave/sphere
install -m 0755 slave/sphere/streamhash.so $RPM_BUILD_ROOT/opt/sector/slave/sphere

#doc
install -d $RPM_BUILD_ROOT/opt/sector/doc
install -m 0644 README.txt $RPM_BUILD_ROOT/opt/sector/README.txt
install -m 0644 release_note.txt $RPM_BUILD_ROOT/opt/sector/release_note.txt

# headers and libs
install -d $RPM_BUILD_ROOT/usr/include
install -m 0644 include/{sector,sphere,conf}.h $RPM_BUILD_ROOT/usr/include
install -m 0644 lib/lib{client,common,rpc,security,udt}.a $RPM_BUILD_ROOT/%libs_dir
install -m 0644 Makefile.common $RPM_BUILD_ROOT/opt/sector


%files -n sector_sphere
# libs
%libs_dir/libclient.so
%libs_dir/libcommon.so
%libs_dir/librpc.so
%libs_dir/libsecurity.so
%libs_dir/libudt.so

# tools
/usr/local/bin/sector_cp
/usr/local/bin/sector_download
/usr/local/bin/sector_ls
/usr/local/bin/sector_mkdir
/usr/local/bin/sector_mv
/usr/local/bin/sector_pipe
/usr/local/bin/sector_rm
/usr/local/bin/sector_shutdown
/usr/local/bin/sector_stat
/usr/local/bin/sector_sysinfo
/usr/local/bin/sector_upload
/usr/local/bin/sphere_stream
/usr/local/bin/sector_fuse

/usr/local/sbin/sector_ssl_cert_gen

# config
/opt/sector/conf/client.conf
/opt/sector/tmp
/opt/sector/conf/master_acl.conf
/opt/sector/conf/slave_acl.conf
/opt/sector/conf/users/anonymous
/opt/sector/conf/users/root
/opt/sector/conf/users/test
/opt/sector/conf/master.conf
/opt/sector/conf/replica.conf
/opt/sector/conf/topology.conf
/opt/sector/conf/slave.conf
/opt/sector/slave/sphere/streamhash.so

#service dameon
/usr/local/sbin/sector_security
/usr/local/sbin/sector_master
/usr/local/sbin/sector_start_all
/usr/local/sbin/sector_stop_all
/usr/local/sbin/sector_slave

#doc
/opt/sector/doc/
/opt/sector/README.txt
/opt/sector/release_note.txt

%files -n sector_sphere-devel
%libs_dir/libclient.a
%libs_dir/libcommon.a
%libs_dir/librpc.a
%libs_dir/libsecurity.a
%libs_dir/libudt.a
/usr/include/sector.h
/usr/include/sphere.h
/usr/include/conf.h
/opt/sector/Makefile.common
