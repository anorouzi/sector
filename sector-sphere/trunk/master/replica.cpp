/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 03/17/2011
*****************************************************************************/


#include <cassert>

#include "common.h"
#include "replica.h"

using namespace std;
using namespace sector;

ReplicaJob::ReplicaJob():
m_iPriority(BACKGROUND),
m_llTimeStamp(CTimer::getTime()),
m_llSize(0),
m_bForceReplicate(false)
{
};

Replication::Replication():
m_llTotalFileSize(0)
{
   m_MultiJobList.resize(MAX_PRIORITY);
   resetIter();
}

Replication::~Replication()
{
}

int Replication::insert(const ReplicaJob& rep)
{
   assert(rep.m_iPriority < MAX_PRIORITY);

   m_MultiJobList[rep.m_iPriority].push_back(rep);
   m_llTotalFileSize += rep.m_llSize;
   m_iTotalJob ++;
   return 0;
}

void Replication::resetIter()
{
   m_CurrIter.m_iPriority = PRI[0];
   m_CurrIter.m_ListIter = m_MultiJobList[PRI[0]].begin();
}

void Replication::deleteCurr()
{
   m_llTotalFileSize -= m_CurrIter.m_ListIter->m_llSize;
   m_iTotalJob --;

   JobList::iterator old = m_CurrIter.m_ListIter;
   m_CurrIter.m_ListIter ++;
   m_MultiJobList[m_CurrIter.m_iPriority].erase(old);
   nextIter();
}

int Replication::next(ReplicaJob& job)
{
   if (m_MultiJobList[m_CurrIter.m_iPriority].empty())
      return -1;

   job = *m_CurrIter.m_ListIter;
   nextIter();
   return 0;
}

int Replication::getTotalNum() const
{
   return m_iTotalJob;
}

int64_t Replication::getTotalSize() const
{
   return m_llTotalFileSize;
}

void Replication::nextIter()
{
   if (m_CurrIter.m_ListIter == m_MultiJobList[m_CurrIter.m_iPriority].end())
   {
      // find next non-empty job list.
      bool found_next = false;
      for (int i = m_CurrIter.m_iPriority + 1; i < MAX_PRIORITY; ++ i)
      {
         if (!m_MultiJobList[i].empty())
         {
            m_CurrIter.m_iPriority = i;
            m_CurrIter.m_ListIter = m_MultiJobList[m_CurrIter.m_iPriority].begin();
            found_next = false;
         }
      }
      if (!found_next)
      {
         resetIter();
      }
   }
}
