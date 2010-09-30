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
   pthread_mutex_init(&m_QueueLock, NULL);
   pthread_cond_init(&m_QueueCond, NULL);
}

ThreadJobQueue::~ThreadJobQueue()
{
   pthread_mutex_destroy(&m_QueueLock);
   pthread_cond_destroy(&m_QueueCond);
}

int ThreadJobQueue::push(void* param)
{
   pthread_mutex_lock(&m_QueueLock);

   m_qJobs.push(param);

   pthread_cond_signal(&m_QueueCond);
   pthread_mutex_unlock(&m_QueueLock);

   return 0;
}

void* ThreadJobQueue::pop()
{
   pthread_mutex_lock(&m_QueueLock);

   while (m_qJobs.empty())
      pthread_cond_wait(&m_QueueCond, &m_QueueLock);

   void* param = m_qJobs.front();
   m_qJobs.pop();

   pthread_mutex_unlock(&m_QueueLock);

   return param;
}

int ThreadJobQueue::release(int num)
{
   for (int i = 0; i < num; ++ i)
      push(NULL);

   return 0;
}
