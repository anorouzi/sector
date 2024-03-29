#include <sectorfs.h>
#include <util.h>
#include <fstream>
#include <iostream>

using namespace std;

Session SectorFS::g_SectorConfig;
map<string, FileTracker*> SectorFS::m_mOpenFileList;
pthread_mutex_t SectorFS::m_OpenFileLock = PTHREAD_MUTEX_INITIALIZER;
bool SectorFS::g_bRunning = false;

void* SectorFS::init(struct fuse_conn_info *conn)
{
   if (Sector::init(g_SectorConfig.m_ClientConf.m_strMasterIP, g_SectorConfig.m_ClientConf.m_iMasterPort) < 0)
      return NULL;
   if (Sector::login(g_SectorConfig.m_ClientConf.m_strUserName, g_SectorConfig.m_ClientConf.m_strPassword, g_SectorConfig.m_ClientConf.m_strCertificate.c_str()) < 0)
      return NULL;

   g_bRunning = true;
   pthread_t heartbeat;
   pthread_create(&heartbeat, NULL, HeartBeat, NULL);
   pthread_detach(heartbeat);

   return NULL;
}

void SectorFS::destroy(void *)
{
   g_bRunning = false;

   Sector::logout();
   Sector::close();
}

int SectorFS::getattr(const char* path, struct stat* st)
{
   SNode s;
   int r = Sector::stat(path, s);
   if (r < 0)
      return translateErr(r);

   if (s.m_bIsDir)
      st->st_mode = S_IFDIR | 0755;
   else
      st->st_mode = S_IFREG | 0444;
   st->st_nlink = 1;
   st->st_uid = 0;
   st->st_gid = 0;
   st->st_size = s.m_llSize;
   st->st_blksize = 1024000;
   st->st_blocks = st->st_size / st->st_blksize + 1;
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

int SectorFS::mkdir(const char* path, mode_t mode)
{
   int r = Sector::mkdir(path);
   if (r < 0)
      return translateErr(r);

   return 0;
}

int SectorFS::unlink(const char* path)
{
   if (Sector::remove(path) < 0)
      return -1;

   return 0;
}

int SectorFS::rmdir(const char* path)
{
   if (Sector::remove(path) < 0)
      return -1;

   return 0;
}

int SectorFS::rename(const char* src, const char* dst)
{
   int r = Sector::move(src, dst);
   if (r < 0)
      return translateErr(r);

   return 0;
}

int SectorFS::statfs(const char* path, struct statvfs* buf)
{
   SysStat s;
   int r = Sector::sysinfo(s);
   if (r < 0)
      return translateErr(r);

   buf->f_namemax = 256;
   buf->f_bsize = 1024;
   buf->f_frsize = buf->f_bsize;
   buf->f_blocks = (s.m_llAvailDiskSpace + s.m_llTotalFileSize) / buf->f_bsize;
   buf->f_bfree = buf->f_bavail = s.m_llAvailDiskSpace / buf->f_bsize;
   buf->f_files = s.m_llTotalFileNum;
   buf->f_ffree = 0xFFFFFFFFULL;

   return 0;
}

int SectorFS::utime(const char* path, struct utimbuf* ubuf)
{
    return Sector::utime(path, ubuf->modtime);;
}

int SectorFS::utimens(const char* path, const struct timespec tv[2])
{
   return Sector::utime(path, tv[1].tv_sec);
}

int SectorFS::opendir(const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* info)
{
   vector<SNode> filelist;
   int r = Sector::list(path, filelist);
   if (r < 0)
      return translateErr(r);

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      struct stat st;
      if (i->m_bIsDir)
         st.st_mode = S_IFDIR | 0755;
      else
         st.st_mode = S_IFREG | 0444;
      st.st_nlink = 1;
      st.st_uid = 0;
      st.st_gid = 0;
      st.st_size = i->m_llSize;
      st.st_blksize = 1024000;
      st.st_blocks = st.st_size / st.st_blksize + 1;
      st.st_atime = st.st_mtime = st.st_ctime = i->m_llTimeStamp;

      filler(buf, i->m_strName.c_str(), &st, 0);
   }

   struct stat st;
   st.st_mode = S_IFDIR | 0755;
   st.st_nlink = 1;
   st.st_uid = 0;
   st.st_gid = 0;
   st.st_size = 4096;
   st.st_blksize = 1024000;
   st.st_blocks = st.st_size / st.st_blksize + 1;
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
   SectorFile f;
   int r = f.open(path, SF_MODE::WRITE);
   if (r < 0)
      return translateErr(r);
   f.close();

   return open(path, info);
}

int SectorFS::truncate(const char *, off_t)
{
   return 0;
}

int SectorFS::ftruncate(const char* path, off_t offset, struct fuse_file_info *)
{
   return truncate(path, offset);
}

int SectorFS::open(const char* path, struct fuse_file_info* fi)
{
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator i = m_mOpenFileList.find(path);
   if (i != m_mOpenFileList.end())
   {
      i->second->m_iCount ++;
      pthread_mutex_unlock(&m_OpenFileLock);
      return 0;
   }

   SectorFile* f = new SectorFile;
   int r = f->open(path, SF_MODE::READ | SF_MODE::WRITE);
   if (r < 0)
   {
      delete f;
      pthread_mutex_unlock(&m_OpenFileLock);
      return translateErr(r);
   }

   FileTracker* t = new FileTracker;
   t->m_strName = path;
   t->m_iCount = 1;
   t->m_pHandle = f;

   m_mOpenFileList[path] = t;

   pthread_mutex_unlock(&m_OpenFileLock);
   return 0;
}

int SectorFS::read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* info)
{
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return -EBADF;
   }
   SectorFile* h = t->second->m_pHandle;
   pthread_mutex_unlock(&m_OpenFileLock);

   h->seekg(offset);
   return h->read(buf, size);
}

int SectorFS::write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* info)
{
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return -EBADF;
   }
   SectorFile* h = t->second->m_pHandle;
   pthread_mutex_unlock(&m_OpenFileLock);

   h->seekp(offset);
   return h->write(buf, size);
}

int SectorFS::flush (const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::fsync(const char *, int, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::release(const char* path, struct fuse_file_info* info)
{
   pthread_mutex_lock(&m_OpenFileLock);
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return -EBADF;
   }

   t->second->m_iCount --;

   if (t->second->m_iCount > 0)
   {
      pthread_mutex_unlock(&m_OpenFileLock);
      return 0;
   }

   t->second->m_pHandle->close();
   delete t->second->m_pHandle;
   delete t->second;

   m_mOpenFileList.erase(t);

   pthread_mutex_unlock(&m_OpenFileLock);
   return 0;
}

int SectorFS::access(const char *, int)
{
   return 0;
}

int SectorFS::lock(const char *, struct fuse_file_info *, int cmd, struct flock *)
{
   return 0;
}

int SectorFS::translateErr(int sferr)
{
   switch (sferr)
   {
   case SectorError::E_PERMISSION:
      return -EACCES;

   case SectorError::E_NOEXIST:
      return -ENOENT;

   case SectorError::E_EXIST:
      return -EEXIST;
   }

   return -1;
}

void* SectorFS::HeartBeat(void*)
{
   while (g_bRunning)
   {
      SNode attr;
      Sector::stat("/", attr);

      sleep(60);
   }

   return NULL;
}
