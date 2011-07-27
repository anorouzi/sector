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

#include <list>
#include <string>
#include <vector>

#include "common.h"

namespace sector
{

// Priority must increase by 1 and start from 0.
const int MAX_PRIORITY = 16;
const int PRI[] = {0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15};

enum ReplicaPriority {COPY = 0, BACKGROUND = 1};

struct ReplicaJob
{
   ReplicaJob();

   std::string m_strSource;
   std::string m_strDest;
   ReplicaPriority m_iPriority;
   int64_t m_llTimeStamp;
   int64_t m_llSize;
   bool m_bForceReplicate;
};

typedef std::list<ReplicaJob> JobList;

struct JobIterator
{
   int m_iPriority;
   JobList::iterator m_ListIter;
};

class Replication
{
public:
   Replication();
   ~Replication();

   int insert(const ReplicaJob& rep);
   void resetIter();
   void deleteCurr();
   int next(ReplicaJob& rep);

   int getTotalNum() const;
   int64_t getTotalSize() const;

private:
   std::vector<JobList> m_MultiJobList;
   JobIterator m_CurrIter;
   int64_t m_llTotalFileSize;
   int m_iTotalJob;

private:
   void nextIter();
};

} // namespace sector

#endif
