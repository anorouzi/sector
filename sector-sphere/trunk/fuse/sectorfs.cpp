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

#include "common.h"
#include "sectorfs.h"
#include <iostream>
using namespace std;

Sector SectorFS::g_SectorClient;
Session SectorFS::g_SectorConfig;
map<string, FileTracker*> SectorFS::m_mOpenFileList;
pthread_mutex_t SectorFS::m_OpenFileLock = PTHREAD_MUTEX_INITIALIZER;
bool SectorFS::g_bConnected = false;

void* SectorFS::init(struct fuse_conn_info * /*conn*/)
{
   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = g_SectorConfig.m_ClientConf.m_sMasterAddr.begin(); i != g_SectorConfig.m_ClientConf.m_sMasterAddr.end(); ++ i)
   {
      if (g_SectorClient.init(i->m_strIP, i->m_iPort) >= 0)
      {
         master_conn = true;
         break;
      }
   }
   if (!master_conn)
      return NULL;

   if (g_SectorClient.login(g_SectorConfig.m_ClientConf.m_strUserName, g_SectorConfig.m_ClientConf.m_strPassword, g_SectorConfig.m_ClientConf.m_strCertificate.c_str()) < 0)
      return NULL;

   g_SectorClient.setMaxCacheSize(g_SectorConfig.m_ClientConf.m_llMaxCacheSize);

   g_bConnected = true;

   return NULL;
}

void SectorFS::destroy(void *)
{
   g_SectorClient.logout();
   g_SectorClient.close();
}

int SectorFS::getattr(const char* path, struct stat* st)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   SNode s;
   int r = g_SectorClient.stat(path, s);
   if (r < 0)
   {
      checkConnection(r);
      return translateErr(r);
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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   int r = g_SectorClient.mkdir(path);
   if (r < 0)
   {
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::unlink(const char* path)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   // If the file has been opened, close it first.
   if (lookup(path) != NULL)
      release(path, NULL);

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      checkConnection(r);
      return -1;
   }

   return 0;
}

int SectorFS::rmdir(const char* path)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   int r = g_SectorClient.remove(path);
   if (r < 0)
   {
      checkConnection(r);
      return -1;
   }

   return 0;
}

int SectorFS::rename(const char* src, const char* dst)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   // If the file has been opened, close it first.
   if (lookup(src) != NULL)
      release(src, NULL);

   int r = g_SectorClient.move(src, dst);
   if (r < 0)
   {
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::statfs(const char* /*path*/, struct statvfs* buf)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   SysStat s;
   int r = g_SectorClient.sysinfo(s);
   if (r < 0)
   {
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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   int r = g_SectorClient.utime(path, ubuf->modtime);
   if (r < 0)
   {
      checkConnection(r);
      return translateErr(r);
   }

   return 0;
}

int SectorFS::utimens(const char* path, const struct timespec tv[2])
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   int r = g_SectorClient.utime(path, tv[1].tv_sec);
   if (r < 0)
   {
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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   vector<SNode> filelist;
   int r = g_SectorClient.list(path, filelist);
   if (r < 0)
   {
      checkConnection(r);
      return translateErr(r);
   }

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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   SNode s;
   if (g_SectorClient.stat(path, s) < 0)
   {
      fuse_file_info option;
      option.flags = O_WRONLY;
      open(path, &option);
      release(path, &option);
   }

   return open(path, info);
}

int SectorFS::truncate(const char* path, off_t size)
{
   // If the file is already openned, call the truncate() API of the SectorFiel handle.
   SectorFile* h = lookup(path);
   if (NULL != h)
   {
      //return h->truncate(size);
   }

   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   SectorFile* h = lookup(path);
   if (NULL == h)
      return -EBADF;

   // FUSE read buffer is too small; we use prefetch buffer to improve read performance
   int r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);
   if (r == 0)
      r = h->read(buf, offset, size, g_SectorConfig.m_ClientConf.m_iFuseReadAheadBlock);

   return r;
}

int SectorFS::write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* /*info*/)
{
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   SectorFile* h = lookup(path);
   if (NULL == h)
      return -EBADF;

   return h->write(buf, offset, size);
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
   if (!g_bConnected) restart();
   if (!g_bConnected) return -1;

   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if ((t == m_mOpenFileList.end()) || (FileTracker::OPEN != t->second->m_State))
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return -EBADF;
   }
   if (t->second->m_iCount > 1)
   {
      t->second->m_iCount --;
      pthread_mutex_unlock(&m_OpenFileLock);
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
   if (g_bConnected)
      return 0;

   g_SectorClient.logout();
   g_SectorClient.close();

   bool master_conn = false;
   for (set<Address, AddrComp>::const_iterator i = g_SectorConfig.m_ClientConf.m_sMasterAddr.begin(); i != g_SectorConfig.m_ClientConf.m_sMasterAddr.end(); ++ i)
   {
      if (g_SectorClient.init(i->m_strIP, i->m_iPort) >= 0)
      {
         master_conn = true;
         break;
      }
   }
   if (!master_conn)
      return -1;

   if (g_SectorClient.login(g_SectorConfig.m_ClientConf.m_strUserName, g_SectorConfig.m_ClientConf.m_strPassword, g_SectorConfig.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   g_SectorClient.setMaxCacheSize(g_SectorConfig.m_ClientConf.m_llMaxCacheSize);

   g_bConnected = true;

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

