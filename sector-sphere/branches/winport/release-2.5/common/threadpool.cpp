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


#include "threadpool.h"

using namespace std;

ThreadJobQueue::ThreadJobQueue()
{
#ifndef WIN32   // <slr>
   pthread_cond_init(&m_QueueCond, NULL);
#else
   m_QueueCond = CreateEvent(NULL, false, false, NULL);
#endif
}

ThreadJobQueue::~ThreadJobQueue()
{
#ifndef WIN32
   pthread_cond_destroy(&m_QueueCond);
#else
   CloseHandle(m_QueueCond);
#endif
}

int ThreadJobQueue::push(void* param)
{
   CMutexGuard guard (m_QueueLock);

   m_qJobs.push(param);
#ifndef WIN32
   pthread_cond_signal(&m_QueueCond);
#else
   SetEvent(m_QueueCond);
#endif

   return 0;
}

void* ThreadJobQueue::pop()
{
   CMutexGuard guard (m_QueueLock);

   while (m_qJobs.empty())
#ifndef WIN32
      pthread_cond_wait(&m_QueueCond, &m_QueueLock.m_Mutex);
#else
   {
      m_QueueLock.release();
      WaitForSingleObject(m_QueueCond, INFINITE);
      m_QueueLock.acquire();
   }
#endif

   void* param = m_qJobs.front();
   m_qJobs.pop();

   return param;
}

int ThreadJobQueue::release(int num)
{
   for (int i = 0; i < num; ++ i)
      push(NULL);

   return 0;
}
