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
   Yunhong Gu, last updated 03/16/2011
*****************************************************************************/

#include <osportable.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#ifndef WIN32
   #include <dirent.h>
   #include <unistd.h>
   #include <sys/vfs.h>
   #include <sys/statvfs.h>
#else
   #include <windows.h>
   #include <tchar.h>
   #include <strsafe.h>
   #include <direct.h>
#endif

using namespace std;

#ifdef WIN32
#if _WIN32_WINNT <= _WIN32_WINNT_WS03

const char *inet_ntop(int af, const void *src, char *dst, int cnt)
{
   if (af == AF_INET)
   {
      struct sockaddr_in in;
      memset(&in, 0, sizeof(in));
      n.sin_family = AF_INET;
      memcpy(&in.sin_addr, src, sizeof(struct in_addr));
      getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in), dst, cnt, NULL, 0, NI_NUMERICHOST);
      return dst;
   }
   else if (af == AF_INET6)
   {
      struct sockaddr_in6 in;
      memset(&in, 0, sizeof(in));
      in.sin6_family = AF_INET6;
      memcpy(&in.sin6_addr, src, sizeof(struct in_addr6));
      getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in6), dst, cnt, NULL, 0, NI_NUMERICHOST);
      return dst;
   }

   return NULL;
}

int inet_pton(int af, const char* s, void* d)
{
   addrinfo hints;
   addrinfo* res;

   memset(&hints, 0, sizeof(struct addrinfo));

   hints.ai_flags = AI_PASSIVE;
   hints.ai_family = af;
   hints.ai_socktype = SOCK_STREAM;

   if (0 != getaddrinfo(NULL, NULL, &hints, &res))
   {
      return -1;
   }

   if (af == AF_INET)
   {
      *sockaddr_in*)d = res->ai_addr;
   }
   else if (af == AF_INET6)
   {
      *sockaddr_in6*)d = res->ai_addr;
   }

   freeaddrinfo(res);

   return 0;
}

#endif
#endif


CMutex::CMutex()
{
#ifndef WIN32   // TODO: add error checking
   pthread_mutex_init(&m_lock, NULL);
#else
   ::InitializeCriticalSection (&m_lock);
#endif
}

CMutex::~CMutex()
{
#ifndef WIN32
   pthread_mutex_destroy(&m_lock);
#else
   ::DeleteCriticalSection (&m_lock);
#endif
}

bool CMutex::acquire()
{
   bool locked = true;
#ifndef WIN32
   int iret = pthread_mutex_lock(&m_lock);
   locked = (iret == 0);
#else
   ::EnterCriticalSection(&m_lock);
#endif
   return locked;
}

bool CMutex::release() 
{
   bool unlocked = true;
#ifndef WIN32
   int iret = pthread_mutex_unlock(&m_lock);
   unlocked = (iret == 0);
#else
   ::LeaveCriticalSection(&m_lock);
#endif
   return unlocked;
}

bool CMutex::trylock()
{
   bool locked = false;
#ifndef WIN32
   int iret = pthread_mutex_trylock(&m_lock);
   locked = (iret == 0);
#else
   BOOL result = ::TryEnterCriticalSection (&m_lock);
   locked = (result == TRUE);
#endif
   return locked;
}

CGuardEx::CGuardEx(CMutex& mutex): 
m_lock(mutex)
{
   m_Locked = m_lock.acquire();
}

CGuardEx::~CGuardEx()
{
   if (m_Locked)
   {
      m_lock.release();
      m_Locked = false;
   }
}

CCond::CCond()
{
#ifndef WIN32
   pthread_cond_init(&m_Cond, NULL);
#else
   m_Cond = ::CreateEvent(NULL, false, false, NULL);
#endif
}

CCond::~CCond()
{
#ifndef WIN32
   pthread_cond_destroy(&m_Cond);
#else
   ::CloseHandle(m_Cond);
#endif
}

bool CCond::signal()
{
#ifndef WIN32
   return (pthread_cond_signal(&m_Cond) == 0);
#else
   return (::SetEvent(m_Cond) == TRUE);
#endif
}

bool CCond::broadcast()
{
#ifndef WIN32
   int rc = pthread_cond_broadcast(&m_Cond);
   return (rc == 0);
#else
   return (::SetEvent(m_Cond) == TRUE);
#endif
}

#ifndef WIN32
timeval& CCond::adjust (timeval & t)
{
   const int ONE_MILLION = 1000000;
   if (t.tv_usec < 0)
   {
       t.tv_usec += ONE_MILLION ;
       t.tv_sec -- ;
   } 
   else if (t.tv_usec > ONE_MILLION) 
   {
      t.tv_usec -= ONE_MILLION ;
      t.tv_sec ++ ;
   }
   return t;
}
#endif

bool CCond::wait(CMutex & mutex)
{
#ifndef WIN32
   int rc = pthread_cond_wait(&m_Cond, &mutex.m_lock);
   return (rc == 0);
#else
   mutex.release();  // mimic Posix behavior
   DWORD dw = WaitForSingleObject(m_Cond, INFINITE);
   mutex.acquire();  // mimic Posix behavior
   return (dw == WAIT_OBJECT_0);
#endif
}

bool CCond::wait (CMutex & mutex, unsigned long msecs, bool * timedout)
{
#ifndef WIN32
   timeval t;
   gettimeofday(&t, NULL);
   t.tv_sec += (msecs / 1000);
   t.tv_usec += (msecs % 1000);
   adjust (t);
   // Convert from timeval to timespec
   timespec ts;
   ts.tv_sec  = t.tv_sec;
   ts.tv_nsec = t.tv_usec * 1000;   // convert micro-seconds to nano-seconds

   int rc = pthread_cond_timedwait(&m_Cond, &mutex.m_lock, &ts);
   if (timedout)
      *timedout = (rc == ETIMEDOUT);
   return (rc == 0);
#else
   mutex.release();  // mimic Posix behavior
   DWORD dw = WaitForSingleObject(m_Cond, msecs);
   mutex.acquire();  // mimic Posix behavior
   if (timedout)
      *timedout = (dw == WAIT_TIMEOUT);
   return (dw == WAIT_OBJECT_0);
#endif
}


RWLock::RWLock()
{
#ifndef WIN32
   pthread_rwlock_init(&m_Lock, NULL);
#else
   InitializeSRWLock(m_Lock);
#endif
}

RWLock::~RWLock()
{
#ifndef WIN32
   pthread_rwlock_destroy(&m_Lock);
#else

#endif
}

int RWLock::acquire_shared()
{
#ifndef WIN32
   if (0 == pthread_rwlock_rdlock(&m_Lock))
      return 0;
   return -1;
#else
   AcquireSRWLockShared(m_Lock);
   return 0;
#endif
}

int RWLock::acquire_exclusive()
{
#ifndef WIN32
   if (0 == pthread_rwlock_wrlock(&m_Lock))
      return 0;
   return -1;
#else
   AcquireSRWLockExclusive(m_Lock);
   return 0;
#endif
}

int RWLock::release_shared()
{
#ifndef WIN32
   if (0 == pthread_rwlock_unlock(&m_Lock))
      return 0;
   return -1;
#else
   ReleaseSRWLockShared(m_Lock);
   return 0;
#endif
}

int RWLock::release_exclusive()
{
#ifndef WIN32
   if (0 == pthread_rwlock_unlock(&m_Lock))
      return 0;
   return -1;
#else
   ReleaseSRWLockExclusive(m_Lock);
   return 0;
#endif
}

RWGuard::RWGuard(RWLock& lock, const RWLockState state):
m_Lock(lock),
m_iLocked(-1),
m_LockState(state)
{
   if (m_LockState == RW_READ)
      m_iLocked = m_Lock.acquire_shared();
   else
      m_iLocked = m_Lock.acquire_exclusive();
}

RWGuard::~RWGuard()
{
   if (m_iLocked != 0)
      return;

   if (m_LockState == RW_READ)
      m_Lock.release_shared();
   else
      m_Lock.release_exclusive();
};



int LocalFS::mkdir(const std::string& path)
{
#ifndef WIN32
   if ((-1 == ::mkdir(path.c_str(), S_IRWXU)) && (errno != EEXIST))
      return -1;
   return 0;
#else
   if ((-1 == ::_mkdir(path.c_str())) && (errno != EEXIST))
      return -1;
   return 0;
#endif
}

int LocalFS::rmdir(const std::string& path)
{
   string cmd;

#ifndef WIN32
   cmd = "rm -rf " + reviseSysCmdPath(path);
#else
   cmd = string("rmdir /Q /S \"") + reviseSysCmdPath(path);
#endif

   system(cmd.c_str());

   return 0;
}

int LocalFS::clean_dir(const std::string& path)
{
   string cmd;

#ifndef WIN32
   cmd = "rm -rf " + reviseSysCmdPath(path) + "/*";
#else
   cmd = string("rmdir /Q /S \"") + reviseSysCmdPath(path) + "\*";
#endif

   system(cmd.c_str());

   return 0;
}

int LocalFS::list_dir(const std::string& path, vector<SNode>& filelist)
{
#ifndef WIN32
   dirent **namelist;
   int n = scandir(path.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      return -1;

   filelist.clear();

   for (int i = 0; i < n; ++ i)
   {
      // skip "." and ".."
      if ((strcmp(namelist[i]->d_name, ".") == 0) || (strcmp(namelist[i]->d_name, "..") == 0))
      {
         free(namelist[i]);
         continue;
      }

      SNode s;
      if (stat(path + "/" + namelist[i]->d_name, s) >= 0)
         filelist.push_back(s);

      free(namelist[i]);
   }
   free(namelist);

   return 0;
#else
   WIN32_FIND_DATA ffd;
   TCHAR szDir[MAX_PATH];
   size_t length_of_arg;
   HANDLE hFind = INVALID_HANDLE_VALUE;
   DWORD dwError=0;

   StringCchLength(path.c_str(), MAX_PATH, &length_of_arg);

   if (length_of_arg > (MAX_PATH - 3))
      return -1;

   StringCchCopy(szDir, MAX_PATH, path.c_str());
   StringCchCat(szDir, MAX_PATH, TEXT("\\*"));

   // Find the first file in the directory.
   hFind = FindFirstFile(szDir, &ffd);
   if (INVALID_HANDLE_VALUE == hFind)
      return dwError;

   // List all the files in the directory with some info about them.
   do
   {
      SNode s;
      if (stat(path + "/" + ffd.cFileName, s) >= 0)
         filelist.push_back(s);
   }
   while (FindNextFile(hFind, &ffd) != 0);

   return 0;
#endif
}

int LocalFS::stat(const std::string& path, SNode& sn)
{
#ifndef WIN32
   struct stat64 st;
   if (-1 == stat64(path.c_str(), &st))
      return -1;
#else
   struct _stat64 st;
   if (-1 == _stat64(path.c_str(), &st))
      return -1;
#endif

   size_t pos = path.rfind('/');
   if (pos == string::npos)
      sn.m_strName = path;
   else
      sn.m_strName = path.substr(pos + 1, path.length() - pos);

   sn.m_bIsDir = S_ISDIR(st.st_mode) ? 1 : 0;
   sn.m_llTimeStamp = st.st_mtime;
   sn.m_llSize = st.st_size;

   Address addr;
   addr.m_strIP = "127.0.0.1";
   addr.m_iPort = 0;
   sn.m_sLocation.insert(addr);

   return 0;
}

string LocalFS::reviseSysCmdPath(const string& path)
{
   string rpath;

#ifndef WIN32
   for (const char *p = path.c_str(), *q = path.c_str() + path.length(); p != q; ++ p)
   {
      if ((*p == 32) || (*p == 34) || (*p == 38) || (*p == 39))
         rpath.append(1, char(92));
      rpath.append(1, *p);
   }
#else
   rpath = path;
#endif

   return rpath;
}

int LocalFS::get_dir_space(const string& path, int64_t& avail)
{
#ifndef WIN32
   struct statfs64 slavefs;
   statfs64(path.c_str(), &slavefs);
   avail = slavefs.f_bfree * slavefs.f_bsize;
   return 0;
#else
   return GetDiskFreeSpaceEx(path.c_str(), &avail, NULL, NULL);
#endif
}
