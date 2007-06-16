/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#ifndef WIN32
   #include <sys/time.h>
   #include <time.h>
   #include <pthread.h>
#endif
#include <util.h>

using namespace cb;

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
