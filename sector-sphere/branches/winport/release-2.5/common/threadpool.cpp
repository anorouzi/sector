/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 01/04/2010
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
