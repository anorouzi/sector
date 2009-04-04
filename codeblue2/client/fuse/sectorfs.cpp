#include <sectorfs.h>
#include <util.h>
#include <fstream>
#include <iostream>

using namespace std;

Session SectorFS::g_SectorConfig;
map<string, FileTracker*> SectorFS::m_mOpenFileList;


void* SectorFS::init(struct fuse_conn_info *conn)
{
   if (Sector::init(g_SectorConfig.m_ClientConf.m_strMasterIP, g_SectorConfig.m_ClientConf.m_iMasterPort) < 0)
      return NULL;
   if (Sector::login(g_SectorConfig.m_ClientConf.m_strUserName, g_SectorConfig.m_ClientConf.m_strPassword, g_SectorConfig.m_ClientConf.m_strCertificate.c_str()) < 0)
      return NULL;

   return NULL;
}

void SectorFS::destroy(void *)
{
   Sector::logout();
   Sector::close();
}

int SectorFS::getattr(const char* path, struct stat* st)
{
   SNode s;
   if (Sector::stat(path, s) < 0)
      return -1;

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
   if (Sector::mkdir(path) < 0)
      return -1;

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
   if (Sector::move(src, dst) < 0)
      return -1;

   return 0;
}

int SectorFS::statfs(const char* path, struct statvfs* buf)
{
   SysStat s;
   if (Sector::sysinfo(s) < 0)
      return -1;

   buf->f_namemax = 256;
   buf->f_bsize = 1024000;
   buf->f_frsize = buf->f_bsize;
   buf->f_bfree = buf->f_bavail = s.m_llAvailDiskSpace;
   buf->f_files = buf->f_ffree = s.m_llTotalFileNum;

   return 0;
}

int SectorFS::utime(const char *, struct utimbuf *)
{
   return 0;
}

int SectorFS::utimens(const char *, const struct timespec tv[2])
{
   return 0;
}

int SectorFS::opendir(const char *, struct fuse_file_info *)
{
   return 0;
}

int SectorFS::readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* info)
{
   vector<SNode> filelist;
   if (Sector::list(path, filelist) < 0)
      return -1;

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

int SectorFS::create(const char *, mode_t, struct fuse_file_info *)
{
   return 0;
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
   map<string, FileTracker*>::iterator i = m_mOpenFileList.find(path);
   if (i != m_mOpenFileList.end())
   {
      i->second->m_iCount ++;
      return 0;
   }

   SectorFile* f = new SectorFile;
   if (f->open(path, SF_MODE::READ | SF_MODE::WRITE) < 0)
   {
      delete f;
      return -1;
   }

   FileTracker* t = new FileTracker;
   t->m_strName = path;
   t->m_iCount = 1;
   t->m_pHandle = f;

   m_mOpenFileList[path] = t;

   return 0;
}

int SectorFS::read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* info)
{
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
      return -1;

   t->second->m_pHandle->seekg(offset);

   return t->second->m_pHandle->read(buf, size);
}

int SectorFS::write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* info)
{
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
      return -1;

   t->second->m_pHandle->seekp(offset);

   return t->second->m_pHandle->write(buf, size);
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
   map<string, FileTracker*>::iterator t = m_mOpenFileList.find(path);
   if (t == m_mOpenFileList.end())
      return -1;

   t->second->m_iCount --;

   if (t->second->m_iCount > 0)
      return 0;

   t->second->m_pHandle->close();
   delete t->second->m_pHandle;
   delete t->second;

   m_mOpenFileList.erase(t);

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
