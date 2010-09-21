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

#include <common.h>
#include "transaction.h"

using namespace std;


TransManager::TransManager():
m_iTransID(1)
{
}

TransManager::~TransManager()
{
}

int TransManager::create(const int type, const int key, const int cmd, const string& file, const int mode)
{
   CMutexGuard tl(m_TLLock);

   Transaction t;
   t.m_iTransID = m_iTransID ++;
   t.m_iType = type;
   t.m_llStartTime = CTimer::getTime();
   t.m_strFile = file;
   t.m_iMode = mode;
   t.m_iUserKey = key;
   t.m_iCommand = cmd;

   m_mTransList[t.m_iTransID] = t;

   return t.m_iTransID;
}

int TransManager::addSlave(int transid, int slaveid)
{
   CMutexGuard tl(m_TLLock);

   m_mTransList[transid].m_siSlaveID.insert(slaveid);

   return transid;
}

int TransManager::retrieve(int transid, Transaction& trans)
{
   CMutexGuard tl(m_TLLock);

   map<int, Transaction>::iterator i = m_mTransList.find(transid);

   if (i == m_mTransList.end())
      return -1;

   trans = i->second;
   return transid;
}

int TransManager::retrieve(int slaveid, vector<int>& trans)
{
   CMutexGuard tl(m_TLLock);

   for (map<int, Transaction>::iterator i = m_mTransList.begin(); i != m_mTransList.end(); ++ i)
   {
      if (i->second.m_siSlaveID.find(slaveid) != i->second.m_siSlaveID.end())
      {
         trans.push_back(i->first);
      }
   }

   return trans.size();
}

int TransManager::updateSlave(int transid, int slaveid)
{
   CMutexGuard tl(m_TLLock);

   m_mTransList[transid].m_siSlaveID.erase(slaveid);
   int ret = m_mTransList[transid].m_siSlaveID.size();
   if (ret == 0)
      m_mTransList.erase(transid);

   return ret;
}

int TransManager::getUserTrans(int key, vector<int>& trans)
{
   CMutexGuard tl(m_TLLock);

   for (map<int, Transaction>::iterator i = m_mTransList.begin(); i != m_mTransList.end(); ++ i)
   {
      if (key == i->second.m_iUserKey)
      {
         trans.push_back(i->first);
      }
   }

   return trans.size();
}

unsigned int TransManager::getTotalTrans()
{
   CMutexGuard tl(m_TLLock);

   return m_mTransList.size();
}

int TransManager::addWriteResult(int transid, int slaveid, const std::string& result)
{
   CMutexGuard tl(m_TLLock);

   m_mTransList[transid].m_mResults[slaveid] = result;

   return 0;
}