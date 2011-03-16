/*****************************************************************************
Copyright 2010 Sergio Ruiz.

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
   Sergio Ruiz, last updated 10/03/2010
*****************************************************************************/

#ifndef __OS_PORTABLE_H__
#define __OS_PORTABLE_H__

#ifndef WIN32
   #include <sys/time.h>
   #include <sys/uio.h>
   #include <pthread.h>
   #include <errno.h>
#else
   #include <windows.h>
#endif
#include <cstdlib>
#include <sector.h>
#include <string>
#include <vector>


#ifndef WIN32
   #include <stdint.h>

   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

#ifdef WIN32
    // Windows compability
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <sys/timeb.h>

    inline int gettimeofday(struct timeval *tp, void *tzp)
    {
        struct _timeb timebuffer;

        errno_t err = _ftime_s (&timebuffer);
        tp->tv_sec = (long)timebuffer.time;
        tp->tv_usec = timebuffer.millitm * 1000;

        return err;
    }

    typedef HANDLE pthread_t;
    typedef HANDLE pthread_mutex_t;
    typedef HANDLE pthread_cond_t;
    typedef DWORD pthread_key_t;

    #define unlink _unlink
    #define snprintf _snprintf_s
    #define atoll _atoi64
    #define stat64 _stat64

    #define     S_ISDIR(m)  (((m)&_S_IFMT) == _S_IFDIR)
    #define     S_ISTYPE(mode, mask)  (((mode) & _S_IFMT) == (mask))
    #define     S_ISREG(mode)    S_ISTYPE((mode), _S_IFREG)

    #if _WIN32_WINNT <= _WIN32_WINNT_WS03
    const char *inet_ntop(int af, const void *src, char *dst, int cnt);
    int inet_pton(int af, const char* s, void* d);
    #endif
#endif


class CMutex
{
friend class CCond;

public:
    CMutex()
    {
#ifndef WIN32   // TODO: add error checking
       pthread_mutex_init(&m_lock, NULL);
#else
       ::InitializeCriticalSection (&m_lock);
#endif
    }

    ~CMutex()
    {
#ifndef WIN32
        pthread_mutex_destroy(&m_lock);
#else
        ::DeleteCriticalSection (&m_lock);
#endif
    }

    inline bool acquire() {
        bool locked = true;
#ifndef WIN32
        int iret = pthread_mutex_lock(&m_lock);
        locked = (iret == 0);
#else
        ::EnterCriticalSection(&m_lock);
#endif
        return locked;
    }

    inline bool release() {
        bool unlocked = true;
#ifndef WIN32
        int iret = pthread_mutex_unlock(&m_lock);
        unlocked = (iret == 0);
#else
        ::LeaveCriticalSection(&m_lock);
#endif
        return unlocked;
    }

    inline bool trylock() {
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


private:
#ifdef WIN32
   CRITICAL_SECTION m_lock;           // mutex to be protected
#else
   pthread_mutex_t m_lock;            // allow public access for now, to use with pthread_cond_t
#endif

   CMutex& operator=(const CMutex&);
};

class CGuardEx
{
public:
    // Automatically lock in constructor
    CGuardEx(CMutex& mutex)
    : m_lock(mutex) {
        m_Locked = m_lock.acquire();
    }

    // Automatically unlock in destructor
    ~CGuardEx()    {
        if (m_Locked) {
            m_lock.release();
            m_Locked = false;
        }
    }

private:
   CMutex& m_lock;            // Alias name of the mutex to be protected
   bool m_Locked;             // Locking status

   CGuardEx& operator=(const CGuardEx&);
};

class CCond
{
public:
    CCond() {
#ifndef WIN32
        pthread_cond_init(&m_Cond, NULL);
#else
        m_Cond = ::CreateEvent(NULL, false, false, NULL);
#endif
    }
 
    ~CCond() {
#ifndef WIN32
        pthread_cond_destroy(&m_Cond);
#else
        ::CloseHandle(m_Cond);
#endif
    }
 
    bool signal() {
#ifndef WIN32
        return (pthread_cond_signal(&m_Cond) == 0);
#else
        return (::SetEvent(m_Cond) == TRUE);
#endif
    }
 
    bool broadcast() {
#ifndef WIN32
        int rc = pthread_cond_broadcast(&m_Cond);
        return (rc == 0);
#else
        return (::SetEvent(m_Cond) == TRUE);
#endif
    }

#ifndef WIN32
    #define ONE_MILLION    1000000
    inline timeval& adjust (timeval & t)
    {
        if (t.tv_usec < 0) {
            t.tv_usec += ONE_MILLION ;
            t.tv_sec -- ;
        } else if (t.tv_usec > ONE_MILLION) {
            t.tv_usec -= ONE_MILLION ;
            t.tv_sec ++ ;
        }
        return t;
    }
#endif

    bool wait (CMutex & mutex) {  // wait forever
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

    bool wait (CMutex & mutex, unsigned long msecs, bool * timedout = NULL) {
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

private:
#ifndef WIN32
   pthread_cond_t m_Cond;
#else
   HANDLE m_Cond;
#endif

   CCond& operator=(const CCond&);
};

enum RWLockState {RW_READ, RW_WRITE};

class RWLock
{
public:
   RWLock();
   ~RWLock();

   int acquire_shared();
   int acquire_exclusive();
   int release_shared();
   int release_exclusive();

private:
#ifndef WIN32
   pthread_rwlock_t m_Lock;
#else
   PSRWLOCK m_Lock;
#endif
};

class RWGuard
{
public:
   RWGuard(RWLock& lock, const RWLockState = RW_READ);
   ~RWGuard();

private:
   RWLock& m_Lock;
   int m_iLocked;
   RWLockState m_LockState;
};

class LocalFS
{
public:
   static int mkdir(const std::string& path);
   static int rmdir(const std::string& path);
   static int clean_dir(const std::string& path);
   static int list_dir(const std::string& path, std::vector<SNode>& filelist);
   static int stat(const std::string& path, SNode& s);

private:
   static std::string reviseSysCmdPath(const std::string& path);
};


#endif
