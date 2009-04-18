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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/17/2009
*****************************************************************************/


#ifndef __SECTOR_TOPOLOGY_H__
#define __SECTOR_TOPOLOGY_H__

#include <string>
#include <set>
#include <map>
#include <vector>
#include <stdint.h>

struct Address
{
   std::string m_strIP;
   unsigned short int m_iPort;
};

struct AddrComp
{
   bool operator()(const Address& a1, const Address& a2) const
   {
      if (a1.m_strIP == a2.m_strIP)
         return a1.m_iPort < a2.m_iPort;
      return a1.m_strIP < a2.m_strIP;
   }
};

class SlaveNode
{
public:
   int m_iNodeID;

   //Address m_Addr;
   std::string m_strIP;
   std::string m_strPublicIP;
   int m_iPort;
   int m_iDataPort;

   int64_t m_llAvailDiskSpace;
   int64_t m_llTotalFileSize;

   int64_t m_llTimeStamp;
   int64_t m_llCurrMemUsed;
   int64_t m_llCurrCPUUsed;
   int64_t m_llTotalInputData;
   int64_t m_llTotalOutputData;
   std::map<std::string, int64_t> m_mSysIndInput;
   std::map<std::string, int64_t> m_mSysIndOutput;
   std::map<std::string, int64_t> m_mCliIndInput;
   std::map<std::string, int64_t> m_mCliIndOutput;

   int64_t m_llLastUpdateTime;
   int m_iRetryNum;
   int m_iStatus;		// 0: inactive 1: active-normal 2: active-disk full

   std::set<int> m_sBadVote;	// set of bad votes by other slaves
   int64_t m_llLastVoteTime;	// timestamp of last vote

   std::vector<int> m_viPath;

public:
   int deserialize(const char* buf, int size);
};

struct Cluster
{
   int m_iClusterID;
   std::vector<int> m_viPath;

   int m_iTotalNodes;
   int64_t m_llAvailDiskSpace;
   int64_t m_llTotalFileSize;

   int64_t m_llTotalInputData;
   int64_t m_llTotalOutputData;
   std::map<std::string, int64_t> m_mSysIndInput;
   std::map<std::string, int64_t> m_mSysIndOutput;
   std::map<std::string, int64_t> m_mCliIndInput;
   std::map<std::string, int64_t> m_mCliIndOutput;

   std::map<int, Cluster> m_mSubCluster;
   std::set<int> m_sNodes;
};

class Topology
{
friend class SlaveManager;

public:
   Topology();
   ~Topology();

public:
   int init(const char* topoconf);
   int lookup(const char* ip, std::vector<int>& path);

   unsigned int match(std::vector<int>& p1, std::vector<int>& p2);

      // Functionality:
      //    compute the distance between two IP addresses.
      // Parameters:
      //    0) [in] ip1: first IP address
      //    1) [in] ip2: second IP address
      // Returned value:
      //    0 if ip1 = ip2, 1 if on the same rack, etc.

   unsigned int distance(const char* ip1, const char* ip2);
   unsigned int distance(const Address& addr, const std::set<Address, AddrComp>& loclist);

   int getTopoDataSize();
   int serialize(char* buf, int& size);
   int deserialize(const char* buf, const int& size);

private:
   int parseIPRange(const char* ip, uint32_t& digit, uint32_t& mask);
   int parseTopo(const char* topo, std::vector<int>& tm);

private:
   unsigned int m_uiLevel;

   struct TopoMap
   {
      uint32_t m_uiIP;
      uint32_t m_uiMask;
      std::vector<int> m_viPath;
   };

   std::vector<TopoMap> m_vTopoMap;
};

#endif
