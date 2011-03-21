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


#ifndef __SECTOR_REPLICA_H__
#define __SECTOR_REPLICA_H__

#include <string>
#include <vector>
#include <queue>

class ReplicaJob
{
public:
   ReplicaJob();

public:
   std::string m_strSource;
   std::string m_strDest;
   int m_iPriority;
   int64_t m_llTimeStamp;
   int64_t m_llSize;
   bool m_bForceReplicate;
};

struct RJComp
{
   bool operator()(const ReplicaJob& r1, const ReplicaJob& r2)
   {
      if (r1.m_iPriority < r2.m_iPriority)
         return true;
      if (r1.m_llTimeStamp > r2.m_llTimeStamp)
         return true;
      return false;
   }
};

class Replication
{
public:
   Replication();
   ~Replication();

   int push(const ReplicaJob& rep);
   int pop(ReplicaJob& rep);

   int getTotalNum();
   int64_t getTotalSize();

private:
   std::priority_queue<ReplicaJob, std::vector<ReplicaJob>, RJComp> m_qReplicaJobs;
   int64_t m_llTotalFileSize;
};

#endif
