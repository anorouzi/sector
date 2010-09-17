/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/


#ifndef __SECTOR_THREAD_POOL_H__
#define __SECTOR_THREAD_POOL_H__

#ifndef WIN32
    #include <pthread.h>
#endif
#include <queue>

#include "common.h"

#ifdef WIN32
    #ifdef COMMON_EXPORTS
        #define COMMON_API __declspec(dllexport)
    #else
        #define COMMON_API __declspec(dllimport)
    #endif
#else
    #define COMMON_API
#endif

class COMMON_API ThreadJobQueue
{
public:
   ThreadJobQueue();
   ~ThreadJobQueue();

public:
   int push(void* param);
   void* pop();

   int release(int num);

private:
   std::queue<void*> m_qJobs;

   CMutex m_QueueLock;
   pthread_cond_t m_QueueCond;
};

#endif
