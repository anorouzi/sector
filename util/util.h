#ifndef __UTIL_H__
#define __UTIL_H__


#ifdef WIN32

   #include <windows.h>

   // Windows compability
   typedef HANDLE pthread_t;
   typedef HANDLE pthread_mutex_t;
   typedef HANDLE pthread_cond_t;
   typedef DWORD pthread_key_t;
   typedef int socklen_t;

   // Explicitly define 32-bit and 64-bit numbers
   typedef __int32 int32_t;
   typedef __int64 int64_t;
   typedef unsigned __int32 uint32_t;
   #if _MSC_VER >= 1300
      typedef unsigned __int64 uint64_t;
   #else
      // VC 6.0 does not support unsigned __int64: may bring potential problems.
      typedef __int64 uint64_t;
   #endif

#else

   #include <sys/types.h>

   #define closesocket ::close

#endif


enum PROTOCOL {PUDT, PTCP};

namespace CodeBlue
{

class Time
{
public:
   static int64_t getTime();
};

class Sync
{
public:
   static void initMutex(pthread_mutex_t& mutex);
   static void releaseMutex(pthread_mutex_t& mutex);
   static void initCond(pthread_cond_t& cond);
   static void releaseCond(pthread_cond_t& cond);
   static void enterCS(pthread_mutex_t& mutex);
   static void leaveCS(pthread_mutex_t& mutex);
};

class Guard
{
};

} 
//namespace 


#endif
