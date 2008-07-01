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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/29/2008
*****************************************************************************/


#ifndef __SECTOR_TOPOLOGY_H__
#define __SECTOR_TOPOLOGY_H__

#include <string>
#include <set>
#include <map>
#include <vector>
#include <index.h>

struct SlaveNode
{
   int m_iNodeID;

   std::string m_strIP;
   int m_iPort;
   std::string m_strPublicIP;

   int64_t m_llMaxDiskSpace;
   int64_t m_llUsedDiskSpace;

   int64_t m_llLastUpdateTime;
   int m_iRetryNum;

   int m_iCurrWorkLoad;
   int m_iStatus;
};

struct Cluster
{
   int m_iClusterID;

   int m_iTotalNodes;
   int m_llTotalDiskSpace;
   int m_llUsedDiskSpace;

   std::map<int, Cluster> m_mSubCluster;
   std::set<int> m_sNodes;
};

class Topology
{
public:
   Topology();
   ~Topology();

public:
   int init(const char* topoconf);
   int lookup(const char* ip, std::vector<int>& path);

private:
   int parseIPRange(const char* ip, int& digit, int& mask);
   int parseTopo(const char* topo, std::vector<int>& tm);

private:
   int m_iLevel;

   struct TopoMap
   {
      int m_iIP;
      int m_iMask;
      std::vector<int> m_viPath;
   };

   std::vector<TopoMap> m_vTopoMap;
};

class SlaveList
{
public:
   SlaveList(): m_iNodeID(0) {}

public:
   int init(const char* topoconf);

   int insert(SlaveNode& sn);
   int remove(int nodeid);

public:
   int chooseReplicaNode(std::set<int>& loclist, SlaveNode& sn);
   int chooseIONode(std::set<int>& loclist, const Address& client, const int& io, SlaveNode& sn);
   int chooseReplicaNode(std::set<Address, AddrComp>& loclist, SlaveNode& sn);
   int chooseIONode(std::set<Address, AddrComp>& loclist, const Address& client, const int& io, SlaveNode& sn);

public:
   int getTotalSlaves();
   int64_t getTotalDiskSpace();

public:
   std::map<Address, int, AddrComp> m_mAddrList;
   std::map<int, SlaveNode> m_mSlaveList;

   Topology m_Topology;
   Cluster m_Cluster;

private:
   int m_iNodeID;
};

#endif
