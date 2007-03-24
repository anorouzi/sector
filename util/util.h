/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef __UTIL_H__
#define __UTIL_H__

namespace cb
{

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

}; //namespace 

#endif
