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
   Yunhong Gu, last updated 10/04/2010
*****************************************************************************/


#ifndef __SECTOR_THREAD_POOL_H__
#define __SECTOR_THREAD_POOL_H__

#include <assert.h>
#include <queue>
#include <osportable.h>

// TODO: this may be changed to thread_util.h
// add start/detach/join thread os-portable routine

// add routine to assign meaningful names to threads, for easier debug

namespace sector
{

struct Job
{
   Job(void* param = NULL): m_pParam(param), m_llTTL(-1), m_iPriority(0), m_iTag(-1) {}

   void* m_pParam;	// job paramters, job specific.
   int64_t m_llTTL;	// each job may be assigned a TTL. Expired job will be discarded without further processing.
   int m_iPriority;	// the thread job queue is a priority queue, higher priority jobs will be scheduled first.
   int m_iTag;		// app specified value used to assign this job to a queue.
};

struct JobQueue
{
   std::queue<Job> m_qJobs;
   CMutex m_QueueLock;
   CCond m_QueueCond;
};

class ThreadJobQueue
{
public:
   ThreadJobQueue();
   ~ThreadJobQueue();

public:
   int push(void* param, int tag);
   void* pop(int key);

   // A thread should register a unique ID, usually its own thread ID.
   // Thus it will be assigned its own job queue.
   int registerThread(int key);
   void release();

   int getNumOfJob() { return m_iTotalJob; }

private:
   std::vector<int> m_vKeyMap;
   std::map<int, JobQueue> m_mJobs;

   int m_iTotalJob;
   int m_iRRSeed;	// Used to send jobs to queues in a round-robin fashion.
};

}  // namespace sector

#endif

