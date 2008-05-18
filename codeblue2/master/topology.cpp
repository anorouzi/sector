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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/13/2008
*****************************************************************************/

#include <topology.h>
#include <unistd.h>
#include <sys/time.h>

using namespace std;

int SlaveList::insert(SlaveNode& sn)
{
   sn.m_iNodeID = m_iNodeID ++;

   m_mSlaveList[sn.m_iNodeID] = sn;

   int cid = sn.m_iClusterID;

   map<int, Cluster>::iterator i = m_mCluster.find(cid);

   if (i != m_mCluster.end())
   {
      i->second.m_sNodes.insert(sn.m_iNodeID);
   }
   else
   {
      Cluster c;
      c.m_iClusterID = cid;
      c.m_sNodes.insert(sn.m_iNodeID);
      m_mCluster[cid] = c;
   }

   Address addr;
   addr.m_strIP = sn.m_strIP;
   addr.m_iPort = sn.m_iPort;
   m_mAddrList[addr] = sn.m_iNodeID;

   return 1;
}

int SlaveList::remove(int nodeid)
{
   map<int, SlaveNode>::iterator i = m_mSlaveList.find(nodeid);

   if (i == m_mSlaveList.end())
      return -1;

   int cid = i->second.m_iClusterID;

   m_mCluster[cid].m_sNodes.erase(nodeid);
   if (m_mCluster[cid].m_sNodes.empty())
     m_mCluster.erase(cid);

   Address addr;
   addr.m_strIP = i->second.m_strIP;
   addr.m_iPort = i->second.m_iPort;
   m_mAddrList.erase(addr);

   m_mSlaveList.erase(i);

   return 1;
}

int SlaveList::chooseReplicaNode(std::set<int>& loclist, SlaveNode& sn)
{
   set<int> avail;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (loclist.find(i->first) != loclist.end())
         avail.insert(i->first);
   }

   if (avail.empty())
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   int r = int(avail.size() * rand() / (RAND_MAX + 1.0));

   set<int>::iterator n = avail.begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   sn = m_mSlaveList[*n];

   return 1;
}

int SlaveList::chooseIONode(std::set<int>& loclist, const Address& client, const int& io, SlaveNode& sn)
{
   set<int> avail;

   if (!loclist.empty())
      avail = loclist;
   else
   {
      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
         avail.insert(i->first);
   }

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   int r = int(loclist.size() * rand() / (RAND_MAX + 1.0));

   set<int>::iterator n = avail.begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   sn = m_mSlaveList[*n];

   return 1;
}

int SlaveList::chooseReplicaNode(std::set<Address, AddrComp>& loclist, SlaveNode& sn)
{
   set<int> avail;
   for (set<Address>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      avail.insert(m_mAddrList[*i]);
   }

   return chooseReplicaNode(avail, sn);
}

int SlaveList::chooseIONode(std::set<Address, AddrComp>& loclist, const Address& client, const int& io, SlaveNode& sn)
{
   set<int> avail;
   for (set<Address>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      avail.insert(m_mAddrList[*i]);
   }

   return chooseIONode(avail, client, io, sn);
}
