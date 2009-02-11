/*****************************************************************************
Copyright © 2006 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/06/2009
*****************************************************************************/


#ifndef __SECTOR_SLAVEMGMT_H__
#define __SECTOR_SLAVEMGMT_H__

#include <topology.h>

class SlaveManager
{
public:
   SlaveManager();
   ~SlaveManager();

public:
   int init(const char* topoconf);

   int insert(SlaveNode& sn);
   int remove(int nodeid);

public:
   int chooseReplicaNode(std::set<int>& loclist, SlaveNode& sn, const int64_t& filesize);
   int chooseIONode(std::set<int>& loclist, const Address& client, int mode, std::map<int, Address>& loc, int replica);
   int chooseReplicaNode(std::set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize);
   int chooseIONode(std::set<Address, AddrComp>& loclist, const Address& client, int mode, std::map<int, Address>& loc, int replica);

public:
   unsigned int getTotalSlaves();
   uint64_t getTotalDiskSpace();
   void updateClusterStat();

private:
   void updateclusterstat_(Cluster& c);
   void updateclusterio_(Cluster& c, std::map<std::string, int64_t>& data_in, std::map<std::string, int64_t>& data_out, int64_t& total);

public:
   std::map<Address, int, AddrComp> m_mAddrList;
   std::map<int, SlaveNode> m_mSlaveList;

   Topology m_Topology;
   Cluster m_Cluster;

private:
   int m_iNodeID;

   pthread_mutex_t m_SlaveLock;
};

#endif
