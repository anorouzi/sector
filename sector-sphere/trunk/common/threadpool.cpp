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

#include <iostream>
#include "threadpool.h"

using namespace std;
using namespace sector;

ThreadJobQueue::ThreadJobQueue():
m_mJobs(),
m_vKeyMap(),
m_iTotalJob(0),
m_iRRSeed(0),
m_Lock()
{
}

ThreadJobQueue::~ThreadJobQueue()
{
}

int ThreadJobQueue::push(void* param, int tag)
{
   // In general case, there should be a default queue to accept requests,
   // even if no processing thread is listening. However, in Sector,
   // we skip ths step as the situation should not happen.
   if (m_mJobs.empty())
     return -1;

   Job job;
   job.m_pParam = param;
   if (tag >= 0)
   {
      job.m_iTag = tag;
   }
   else
   {
      // When a negative tag value is provided, jobs assignment is round robin.
      job.m_iTag = m_iRRSeed ++;
   }

   m_Lock.acquire();
   int key = m_vKeyMap[job.m_iTag % m_mJobs.size()];
   map<int, JobQueue*>::iterator ptr = m_mJobs.find(key);
   assert(ptr != m_mJobs.end());
   m_Lock.release();

   CGuardEx tg(ptr->second->m_QueueLock);
   if (static_cast<int>(ptr->second->m_qJobs.size()) >= ptr->second->m_iLimit)
      return -1;
   ptr->second->m_qJobs.push(job);
   ptr->second->m_QueueCond.signal();

   ++ m_iTotalJob;
   return 0;
}

void* ThreadJobQueue::pop(int key)
{
   m_Lock.acquire();
   map<int, JobQueue*>::iterator ptr = m_mJobs.find(key);
   assert(ptr != m_mJobs.end());
   m_Lock.release();

   JobQueue& q = *ptr->second;
   CGuardEx tg(q.m_QueueLock);
   while (q.m_qJobs.empty())
      q.m_QueueCond.wait(q.m_QueueLock);

   Job job = q.m_qJobs.front();
   q.m_qJobs.pop();

   -- m_iTotalJob;
   return job.m_pParam;
}

int ThreadJobQueue::registerThread(int key, int limit)
{
   CGuardEx tg(m_Lock);

   if (m_mJobs.find(key) != m_mJobs.end())
      return -1;

cout << "register " << key << endl;
   JobQueue* q = new JobQueue;
   if (limit > 0)
      q->m_iLimit = limit;
   m_mJobs[key] = q;
   m_vKeyMap.push_back(key);

cout << "returned...............................\n";
   return 0;
}

void ThreadJobQueue::release()
{
   CGuardEx tg(m_Lock);

   for (map<int, JobQueue*>::iterator i = m_mJobs.begin(); i != m_mJobs.end(); ++ i)
   {
      // If the job parameter is NULL, this is a signal for the thread to quit.
      CGuardEx tg(i->second->m_QueueLock);
      i->second->m_qJobs.push(NULL);
      i->second->m_QueueCond.signal();
   }
}
