/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 03/21/2011
*****************************************************************************/

#include <fstream>
#include <iostream>
#include <ctime>

#include "common.h"
#include "sectorfs.h"
#include "fusedircache.h"
using namespace std;

Sector SectorFS::g_SectorClient;
Session SectorFS::g_SectorConfig;
map<string, FileTracker*> SectorFS::m_mOpenFileList;
pthread_mutex_t SectorFS::m_OpenFileLock = PTHREAD_MUTEX_INITIALIZER;
bool SectorFS::g_bConnected = false;

#define CONN_CHECK( fn ) \
{\
   if (!g_bConnected) {\
      time_t t = time(0); \
      std::string asStr = ctime(&t);\
      std::cout << asStr.substr( 0, asStr.length() - 1 ) << ' ' << __PRETTY_FUNCTION__ << " Not connected - restarting " << fn << std::endl; \
      restart();\
   }\
   if (!g_bConnected) \
   {\
      time_t t = time(0); \
      std::string asStr = ctime(&t);\
      std::cout << asStr.substr( 0, asStr.length() - 1 ) << ' ' << __PRETTY_FUNCTION__ << " connection restart failed " << fn << std::endl; \
      return -1;\
   } \
}

#define ERR_MSG( msg ) \
{\
      time_t t = time(0); \
      std::string asStr = ctime(&t);\
      std::cout << asStr.substr( 0, asStr.length() - 1 ) << ' ' << __PRETTY_FUNCTION__ << ' ' << msg << std::endl; \
}


void* SectorFS::init(struct fuse_conn_info * /*conn*/)
{
   const ClientConf& conf = g_SectorConfig.m_ClientConf;

   ERR_MSG("Starting sector-fuse");

   g_SectorClient.init();
   g_SectorClient.configLog(conf.m_strLog.c_str(), false, conf.m_iLogLevel);
   g_SectorClient.setMaxCacheSize(conf.m_llMaxCacheSize);
   DirCache::instance();

   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = conf.m_sMasterAddr.begin(); i != conf.m_sMasterAddr.end(); ++ i)
   {
      if (g_SectorClient.login(i->m_strIP, i->m_iPort, conf.m_strUserName,
                               conf.m_strPassword, conf.m_strCertificate.c_str()) >= 0)
      {
         master_conn = true;
         break;
      }
   }
   if (!master_conn)
      return NULL;

   g_bConnected = true;
   return NULL;
}

void SectorFS::destroy(void *)
{
   g_SectorClient.logout();
   g_SectorClient.close();
   DirCache::destroy();
   ERR_MSG("End sector-fuse");
}

int SectorFS::getattr(const char* path, struct stat* st)
{
   CONN_CHECK( path );

   SNode s;

   int rv = DirCache::instance().get(path, g_SectorClient, s);
   if( rv < 0 )
   {
      ERR_MSG( path << ' ' << rv );
      checkConnection(rv);
      return translateErr(rv);
   }

   if( rv == 1 )
   {
      int r = g_SectorClient.stat(path, s);
      if (r < 0)
      {        
         ERR_MSG( path << ' ' << r );
         DirCache::clearLastUnresolvedStat();
         checkConnection(r);
         return translateErr(r);
      }
   }

   if (s.m_bIsDir)
      st->st_mode = S_IFDIR | 0755;
   else
      st->st_mode = S_IFREG | 0755;
   st->st_nlink = 1;
   st->st_uid = 0;
   st->st_gid = 0;
   st->st_size = s.m_llSize;
   st->st_blksize = g_iBlockSize;
   st->st_blocks = st->st_size / st->st_blksize;
   if ((st->st_size % st->st_blksize) != 0)
      ++ st->st_blocks;
   st->st_atime = st->st_mtime = st->st_ctime = s.m_llTimeStamp;

   if (st->st_size == 0) DirCache::clearLastUnresolvedStat();

   return 0;
}

int SectorFS::fgetattr(const char* path, struct stat* st , struct fuse_file_info *)
{
   return getattr(path, st);
}

int SectorFS::mknod(const char *, mode_t, dev_t)
{
   return 0;
}

int SectorFS::mkdir(const char* path, mode_t /*mode*/)
{
   CONN_CHECK( path );

   DirCache::clear();

   int r = g_SectorClient.mkdir(path);
   if (r < 0)
   {
      ERR_MSG(path << ' ' << r );
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::unlink(const char* path)
{
   CONN_CHECK( path );

   DirCache::clear();

   // If the file has been opened, close it first.
   if (lookup(path) != NULL)
      release(path, NULL);

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      ERR_MSG( path << ' ' << r );
      checkConnection(r);
      return -1;
   }

   return 0;
}

int SectorFS::rmdir(const char* path)
{
   CONN_CHECK( path );

   DirCache::clear();

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      ERR_MSG( path << ' ' << r );
      checkConnection(r);
      return -1;
   }

   return 0;
}

int SectorFS::rename(const char* src, const char* dst)
{
   CONN_CHECK( src << ' ' << dst );

   DirCache::clear();

   // If the file has been opened, close it first.
   if (lookup(src) != NULL)
      release(src, NULL);

   int r = g_SectorClient.move(src, dst);
   if (r < 0)
   {
      ERR_MSG( "source " << src << " dest " << dst << " error code " << r );
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::statfs(const char* /*path*/, struct statvfs* buf)
{
   CONN_CHECK( "sysinfo" );

   SysStat s;
   int r = g_SectorClient.sysinfo(s);
   if (r < 0)
   {
      ERR_MSG( "sysinfo" );
      checkConnection(r);
      return translateErr(r);
   }

   buf->f_namemax = 256;
   buf->f_bsize = g_iBlockSize;
   buf->f_frsize = buf->f_bsize;
   buf->f_blocks = (s.m_llAvailDiskSpace + s.m_llTotalFileSize) / buf->f_bsize;
   buf->f_bfree = buf->f_bavail = s.m_llAvailDiskSpace / buf->f_bsize;
   buf->f_files = s.m_llTotalFileNum;
   buf->f_ffree = 0xFFFFFFFFULL;

   return 0;
}

int SectorFS::utime(const char* path, struct utimbuf* ubuf)
{
   CONN_CHECK( path );

   int r = g_SectorClient.utime(path, ubuf->modtime);
   if (r < 0)
   {
      ERR_MSG( path << ' ' << r );
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::utimens(const char* path, const struct timespec tv[2])
{
   CONN_CHECK( path );

   int r = g_SectorClient.utime(path, tv[1].tv_sec);
   if (r < 0)
   {
      ERR_MSG( path << ' ' << r );
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::opendir(const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t /*offset*/, struct fuse_file_info* /*info*/)
{
   CONN_CHECK( path );

   vector<SNode> filelist;
   int r = g_SectorClient.list(path, filelist);
   if (r < 0)
   {
      ERR_MSG( path << ' ' << r );
      checkConnection(r);
      return translateErr(r);
   }

   DirCache::instance().add(path, filelist);

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      struct stat st;
      if (i->m_bIsDir)
         st.st_mode = S_IFDIR | 0755;
      else
         st.st_mode = S_IFREG | 0755;
      st.st_nlink = 1;
      st.st_uid = 0;
      st.st_gid = 0;
      st.st_size = i->m_llSize;
      st.st_blksize = g_iBlockSize;
      st.st_blocks = st.st_size / st.st_blksize;
      if ((st.st_size % st.st_blksize) != 0)
         ++ st.st_blocks;
      st.st_atime = st.st_mtime = st.st_ctime = i->m_llTimeStamp;

      filler(buf, i->m_strName.c_str(), &st, 0);
   }

   struct stat st;
   st.st_mode = S_IFDIR | 0755;
   st.st_nlink = 1;
   st.st_uid = 0;
   st.st_gid = 0;
   st.st_size = 4096;
   st.st_blksize = g_iBlockSize;
   st.st_blocks = st.st_size / st.st_blksize;
   if ((st.st_size % st.st_blksize) != 0)
      ++ st.st_blocks;
   st.st_atime = st.st_mtime = st.st_ctime = 0;
   filler(buf, ".", &st, 0);
   filler(buf, "..", &st, 0);

   return 0;
}

int SectorFS::fsyncdir(const char *, int, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::releasedir(const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::chmod(const char *, mode_t)
{
   return 0;
}

int SectorFS::chown(const char *, uid_t, gid_t)
{
   return 0;
}

int SectorFS::create(const char* path, mode_t, struct fuse_file_info* info)
{
   CONN_CHECK( path );

   DirCache::clear();

   SNode s;
   if (g_SectorClient.stat(path, s) < 0)
   {
      fuse_file_info option;
      option.flags = O_WRONLY;
      open(path, &option);
      release(path, &option);
   }

   int r = open(path, info);
   if (r < 0) {
      ERR_MSG( path );
      return -1;
   }
   return r;
}

int SectorFS::truncate(const char* path, off_t /*size*/)
{
   // If the file is already openned, call the truncate() API of the SectorFiel handle.
   SectorFile* h = lookup(path);
   if (NULL != h)
   {
      //return h->truncate(size);
   }

   CONN_CHECK( path );

   DirCache::clear();

   // Otherwise open the file and perform trunc.
   fuse_file_info option;
   option.flags = O_WRONLY | O_TRUNC;
   open(path, &option);
   release(path, &option);

   return 0;
}

int SectorFS::ftruncate(const char* path, off_t offset, struct fuse_file_info *)
{
   return truncate(path, offset);
}

int SectorFS::open(const char* path, struct fuse_file_info* fi)
{
   CONN_CHECK( path );

   DirCache::clear();

   // TODO: file option should be checked.
   bool owner = false;
   FileTracker* ft = NULL;

   FileTracker::State state = FileTracker::NEXIST;
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t != m_mOpenFileList.end())
   {
      state = t->second->m_State;
      t->second->m_iCount ++;
      if (FileTracker::OPEN == state)
      {
         pthread_mutex_unlock(&m_OpenFileLock);
         return 0;
      }
      else
      {
         ft = t->second;
      }
   }
   else
   {
      ft = new FileTracker;
      ft->m_strName = path;
      ft->m_iCount = 1;
      ft->m_State = FileTracker::OPENING;
      ft->m_pHandle = NULL;
      m_mOpenFileList[path] = ft;
      owner = true;
   }
   pthread_mutex_unlock(&m_OpenFileLock);

   if (!owner)
   {
      // A spin loop to wait for the file to be opened/closed by another thread.
      // TODO: add signal support. This should happen rarely anyway.
      while (true)
      {
         { // mutex proteced block.
            CGuard fl(m_OpenFileLock);
            if ((FileTracker::CLOSING != ft->m_State) && (FileTracker::OPENING != ft->m_State))
            {
               // Another thread has opened the file, no need to open again.
               if (FileTracker::OPEN == ft->m_State)
                  return 0;

               if (FileTracker::CLOSED == ft->m_State)
                  ft->m_State = FileTracker::OPENING;

               break;
            }
         } // end of mutex protection.

         usleep(1);
      }
   }

   // Others are waiting for this thread to open the file.
   SectorFile* f = g_SectorClient.createSectorFile();

   int permission = SF_MODE::READ;
   if (fi->flags & O_WRONLY)
      permission = SF_MODE::WRITE;
   else if (fi->flags & O_RDWR)
      permission = SF_MODE::READ | SF_MODE::WRITE;

   if (fi->flags & O_TRUNC)
      permission |= SF_MODE::TRUNC;

   if (fi->flags & O_APPEND)
      permission |= SF_MODE::APPEND;

   int r = f->open(path, permission);
   if (r < 0)
   {
      pthread_mutex_lock(&m_OpenFileLock);
      ft->m_State = FileTracker::CLOSED;
      ft->m_iCount --;
      if (0 == ft->m_iCount)
      {
         m_mOpenFileList.erase(path);
         delete ft;
      }
      pthread_mutex_unlock(&m_OpenFileLock);

      ERR_MSG( path << ' ' << r );
      return -1;
   }

   pthread_mutex_lock(&m_OpenFileLock);
   ft->m_pHandle = f;
   ft->m_State = FileTracker::OPEN;
   pthread_mutex_unlock(&m_OpenFileLock);
   return 0;
}

int SectorFS::read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* /*info*/)
{
   CONN_CHECK( path );

   SectorFile* h = lookup(path);
   if (NULL == h) {
      ERR_MSG( "Attempt to read unopened file " << path );
      return -EBADF;
   }

   // FUSE read buffer is too small; we use prefetch buffer to improve read performance
   int r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);
   if (r == 0) {
      r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);
      if (r < 0) {
          ERR_MSG( " Reread fail with error code " << r << " file " << path <<
           " size " << size << " offset " << offset );
          return -1;
      } else {
          ERR_MSG( "Reread successful, read bytes  " << r << " file " << path <<
           " size " << size << " offset " << offset);
      }
   }
   return r;
}

int SectorFS::write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* /*info*/)
{
   CONN_CHECK( path );

   SectorFile* h = lookup(path);
   if (NULL == h) {
      ERR_MSG("Attempt to write to not opened file " << path <<
        " size " << size << " offset " << offset);
      return -EBADF;     
   }

   int r = h->write(buf, offset, size);
   if (r < 0) {
      ERR_MSG("Write give an error " << r << " file " << path <<
        " size " << size << " offset " << offset);
      return -1;
   }

   return r;
}

int SectorFS::flush (const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::fsync(const char *, int, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::release(const char* path, struct fuse_file_info* /*info*/)
{
   CONN_CHECK( __PRETTY_FUNCTION__ << " " <<  path   );

   DirCache::clear();

   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if ((t == m_mOpenFileList.end()) || (FileTracker::OPEN != t->second->m_State))
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      ERR_MSG("Error in release " << path );
      return -EBADF;
   }
   if (t->second->m_iCount > 1)
   {
      t->second->m_iCount --;
      pthread_mutex_unlock(&m_OpenFileLock);
      DirCache::clear();
      return 0;
   }
   FileTracker* ft = t->second;
   ft->m_State = FileTracker::CLOSING;
   pthread_mutex_unlock(&m_OpenFileLock);
   // File close/release may take some time, so do it out of mutex protection.
   ft->m_pHandle->close();
   g_SectorClient.releaseSectorFile(ft->m_pHandle);

   pthread_mutex_lock(&m_OpenFileLock);
   ft->m_pHandle = NULL;
   ft->m_State = FileTracker::CLOSED;
   t->second->m_iCount --;
   if (t->second->m_iCount == 0)
   {
      delete ft;
      m_mOpenFileList.erase(t);
   }
   pthread_mutex_unlock(&m_OpenFileLock);

   DirCache::clear();

   return 0;
}

int SectorFS::access(const char *, int)
{
   return 0;
}

int SectorFS::lock(const char *, struct fuse_file_info *, int, struct flock *)
{
   return 0;
}

int SectorFS::translateErr(int err)
{
//   ERR_MSG("Error in SectorFS " << err);
   switch (err)
   {
   case SectorError::E_PERMISSION:
      return -EACCES;

   case SectorError::E_NOEXIST:
      return -ENOENT;

   case SectorError::E_EXIST:
      return -EEXIST;

   case SectorError::E_MASTER:
      return -EHOSTDOWN;

   case SectorError::E_CONNECTION:
      return -EHOSTDOWN;

   case SectorError::E_BUSY:
      return -EBUSY;
   }

   return -1;
}

int SectorFS::restart()
{
   ERR_MSG( "Restart" );
   if (g_bConnected) {
      ERR_MSG("Not restarted as connection is OK")
      return 0;
   }

   DirCache::clear();

   g_SectorClient.logout();
   g_SectorClient.close();
   init(NULL);
   return 0;
}

void SectorFS::checkConnection(int res)
{
   if ((res == SectorError::E_MASTER) || (res == SectorError::E_EXPIRED))
      g_bConnected = false;
}

SectorFile* SectorFS::lookup(const string& path)
{
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return NULL;
   }
   SectorFile* h = t->second->m_pHandle;
   pthread_mutex_unlock(&m_OpenFileLock);

   return h;
}
