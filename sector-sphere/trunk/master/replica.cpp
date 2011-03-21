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


#include <replica.h>
#include <common.h>

using namespace std;

ReplicaJob::ReplicaJob():
m_iPriority(0),
m_llTimeStamp(CTimer::getTime()),
m_llSize(0),
m_bForceReplicate(false)
{
};

Replication::Replication():
m_llTotalFileSize(0)
{
}

Replication::~Replication()
{
}

int Replication::push(const ReplicaJob& rep)
{
   m_qReplicaJobs.push(rep);
   m_llTotalFileSize += rep.m_llSize;
   return 0;
}

int Replication::pop(ReplicaJob& rep)
{
   if (m_qReplicaJobs.empty())
      return -1;

   rep = m_qReplicaJobs.top();
   m_qReplicaJobs.pop();
   return 0;
}

int Replication::getTotalNum()
{
   return m_qReplicaJobs.size();
}

int64_t Replication::getTotalSize()
{
   return m_llTotalFileSize;
}
