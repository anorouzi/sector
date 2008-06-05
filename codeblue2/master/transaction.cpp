/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 06/04/2008
*****************************************************************************/

#include <common.h>
#include "transaction.h"

using namespace std;

TransManager::TransManager():
m_iTransID(0)
{
}

TransManager::~TransManager()
{
}

int TransManager::insert(const int slave, const string& file, const int mode, const string& user, const int cmd)
{
   Transaction t;
   t.m_iTransID = m_iTransID ++;
   t.m_llStartTime = CTimer::getTime();
   t.m_strFile = file;
   t.m_iMode = mode;
   t.m_strUser = user;
   t.m_iCommand = cmd;

   m_mTransList[t.m_iTransID] = t;

   return t.m_iTransID;
}

int TransManager::retrieve(int transid, Transaction& trans)
{
   map<int, Transaction>::iterator i = m_mTransList.find(transid);

   if (i == m_mTransList.end())
      return -1;

   trans = i->second;
   return transid;
}

int TransManager::update(int transid)
{
   m_mTransList.erase(transid);
   return 0;
}
