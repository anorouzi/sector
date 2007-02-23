#ifndef WIN32
   #include <sys/time.h>
   #include <time.h>
   #include <pthread.h>
#endif
#include <util.h>

using namespace CodeBlue;

int64_t Time::getTime()
{
   #ifndef WIN32
      timeval t;
      gettimeofday(&t, 0);
      return (int64_t)t.tv_sec * 1000000 + t.tv_usec;
   #else
      LARGE_INTEGER ccf;
      if (QueryPerformanceFrequency(&ccf))
      {
         LARGE_INTEGER cc;
         QueryPerformanceCounter(&cc);
         return (int64_t)((cc.QuadPart / ccf.QuadPart) * 1000000 + (cc.QuadPart % ccf.QuadPart) / (ccf.QuadPart / 1000000));
      }
      else
      {
         uint64_t ft;
         GetSystemTimeAsFileTime((FILETIME *)&ft);
         return (int64_t)(ft / 10) + ((ft % 10000000) / 10);
      }
   #endif
}

void Sync::initMutex(pthread_mutex_t& mutex)
{
   #ifndef WIN32
      pthread_mutex_init(&mutex, NULL);
   #else
      mutex = CreateMutex(NULL, false, NULL);
   #endif
}

void Sync::releaseMutex(pthread_mutex_t& mutex)
{
   #ifndef WIN32
      pthread_mutex_destroy(&mutex);
   #else
      CloseHandle(mutex);
   #endif
}

void Sync::initCond(pthread_cond_t& cond)
{
   #ifndef WIN32
      pthread_cond_init(&cond, NULL);
   #else
      cond = CreateEvent(NULL, false, false, NULL);
   #endif
}

void Sync::releaseCond(pthread_cond_t& cond)
{
   #ifndef WIN32
      pthread_cond_destroy(&cond);
   #else
      CloseHandle(cond);
   #endif
}

void Sync::enterCS(pthread_mutex_t& mutex)
{
   #ifndef WIN32
      pthread_mutex_lock(&mutex);
   #else
      WaitForSingleObject(mutex, INFINITE);
   #endif
}

void Sync::leaveCS(pthread_mutex_t& mutex)
{
   #ifndef WIN32
      pthread_mutex_unlock(&mutex);
   #else
      ReleaseMutex(mutex);
   #endif
}
