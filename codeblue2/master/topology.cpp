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
   Yunhong Gu [gu@lac.uic.edu], last updated 10/31/2008
*****************************************************************************/

#include <topology.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fstream>
#include <iostream>
using namespace std;

Topology::Topology():
m_uiLevel(1)
{
}

Topology::~Topology()
{
}

int Topology::init(const char* topoconf)
{
   ifstream ifs(topoconf);

   if (ifs.bad() || ifs.fail())
      return -1;

   char line[128];
   while (!ifs.eof())
   {
      ifs.getline(line, 128);

      if ((strlen(line) == 0) || (line[0] == '#'))
         continue;

      // 192.168.136.0/24	/1/1

      unsigned int p = 0;
      for (; p < strlen(line); ++ p)
      {
         if ((line[p] == ' ') || (line[p] == '\t'))
            break;
      }

      string ip = string(line).substr(0, p);

      for (; p < strlen(line); ++ p)
      {
         if ((line[p] != ' ') && (line[p] != '\t'))
            break;
      }

      string topo = string(line).substr(p + 1, strlen(line));

      TopoMap tm;
      if (parseIPRange(ip.c_str(), tm.m_iIP, tm.m_iMask) < 0)
         return -1;
      if (parseTopo(topo.c_str(), tm.m_viPath) <= 0)
         return -1;

      m_vTopoMap.insert(m_vTopoMap.end(), tm);

      if (m_uiLevel < tm.m_viPath.size())
         m_uiLevel = tm.m_viPath.size();
   }

   ifs.close();

   return m_vTopoMap.size();
}

int Topology::lookup(const char* ip, vector<int>& path)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip, &addr) < 0)
      return -1;

   int digitip = addr.s_addr;

   for (vector<TopoMap>::iterator i = m_vTopoMap.begin(); i != m_vTopoMap.end(); ++ i)
   {
      if ((digitip & i->m_iMask) == (i->m_iIP & i->m_iMask))
      {
         path = i->m_viPath;
         return 0;
      }
   }

   for (unsigned int i = 0; i < m_uiLevel; ++ i)
      path.insert(path.end(), 0);

   return -1;
}

int Topology::match(std::vector<int>& p1, std::vector<int>& p2)
{
   for (unsigned int i = 0; i < m_uiLevel; ++ i)
   {
      if (p1[i] != p2[i])
         return i;
   }

   return m_uiLevel;
}

int Topology::parseIPRange(const char* ip, int& digit, int& mask)
{
   char buf[128];
   unsigned int i = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i)
   {
      if ('/' == ip[i])
         break;

      buf[i] = ip[i];
   }
   buf[i] = '\0';

   in_addr addr;
   if (inet_pton(AF_INET, buf, &addr) < 0)
      return -1;

   digit = addr.s_addr;
   mask = 0xFFFFFFFF;

   if (i == strlen(ip))
      return 0;

   if ('/' != ip[i])
      return -1;
   ++ i;

   int j = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i, ++ j)
      buf[j] = ip[i];
   buf[j] = '\0';

   char* p;
   unsigned int bit = strtol(buf, &p, 10);

   if ((p == buf) || (bit > 32) || (bit < 0))
      return -1;

   if (bit < 32)
       mask = ((unsigned int)1 << bit) - 1;

   return 0;
}

int Topology::parseTopo(const char* topo, vector<int>& tm)
{
   char buf[32];
   strncpy(buf, topo, 32);
   int size = strlen(buf);

   for (int i = 0; i < size; ++ i)
   {
      if (buf[i] == '/')
         buf[i] = '\0';
   }

   for (int i = 0; i < size; )
   {
      tm.insert(tm.end(), atoi(buf + i));
      i += strlen(buf + i) + 1;
   }

   return tm.size();
}


int SlaveManager::init(const char* topoconf)
{
   m_Cluster.m_iClusterID = 0;
   m_Cluster.m_iTotalNodes = 0;
   m_Cluster.m_llAvailDiskSpace = 0;
   m_Cluster.m_llTotalFileSize = 0;
   m_Cluster.m_llTotalInputData = 0;
   m_Cluster.m_llTotalOutputData = 0;

   return m_Topology.init(topoconf);
}

int SlaveManager::insert(SlaveNode& sn)
{
   sn.m_iNodeID = m_iNodeID ++;
   sn.m_iStatus = 1;

   m_mSlaveList[sn.m_iNodeID] = sn;

   Address addr;
   addr.m_strIP = sn.m_strIP;
   addr.m_iPort = sn.m_iPort;
   m_mAddrList[addr] = sn.m_iNodeID;

   m_Topology.lookup(sn.m_strIP.c_str(), sn.m_viPath);
   map<int, Cluster>* sc = &(m_Cluster.m_mSubCluster);
   map<int, Cluster>::iterator pc;
   for (vector<int>::iterator i = sn.m_viPath.begin(); i != sn.m_viPath.end(); ++ i)
   {
      if ((pc = sc->find(*i)) == sc->end())
      {
         Cluster c;
         c.m_iClusterID = *i;
         c.m_iTotalNodes = 0;
         c.m_llAvailDiskSpace = 0;
         c.m_llTotalFileSize = 0;
         c.m_llTotalInputData = 0;
         c.m_llTotalOutputData = 0;

         (*sc)[*i] = c;

         pc = sc->find(*i);
      }

      pc->second.m_iTotalNodes ++;
      pc->second.m_llAvailDiskSpace += sn.m_llAvailDiskSpace;
      pc->second.m_llTotalFileSize += sn.m_llTotalFileSize;

      sc = &(pc->second.m_mSubCluster);
   }

   pc->second.m_sNodes.insert(sn.m_iNodeID);

   return 1;
}

int SlaveManager::remove(int nodeid)
{
   map<int, SlaveNode>::iterator sn = m_mSlaveList.find(nodeid);

   if (sn == m_mSlaveList.end())
      return -1;

   Address addr;
   addr.m_strIP = sn->second.m_strIP;
   addr.m_iPort = sn->second.m_iPort;
   m_mAddrList.erase(addr);

   vector<int> path;
   m_Topology.lookup(sn->second.m_strIP.c_str(), path);
   map<int, Cluster>* sc = &(m_Cluster.m_mSubCluster);
   map<int, Cluster>::iterator pc;
   for (vector<int>::iterator i = path.begin(); i != path.end(); ++ i)
   {
      if ((pc = sc->find(*i)) == sc->end())
      {
         // something wrong
         break;
      }

      pc->second.m_iTotalNodes --;
      pc->second.m_llAvailDiskSpace -= sn->second.m_llAvailDiskSpace;
      pc->second.m_llTotalFileSize -= sn->second.m_llTotalFileSize;

      sc = &(pc->second.m_mSubCluster);
   }

   pc->second.m_sNodes.erase(nodeid);

   m_mSlaveList.erase(sn);

   return 1;
}

int SlaveManager::chooseReplicaNode(set<int>& loclist, SlaveNode& sn, const int64_t& filesize)
{
   vector< set<int> > avail;
   avail.resize(m_Topology.m_uiLevel + 1);
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      // only nodes with more than 10GB disk space are chosen
      if (i->second.m_llAvailDiskSpace < (10000000000LL + filesize))
         continue;

      int level = 0;
      for (set<int>::iterator j = loclist.begin(); j != loclist.end(); ++ j)
      {
         if (i->first == *j)
         {
            level = -1;
            break;
         }

         int tmpl = m_Topology.match(i->second.m_viPath, m_mSlaveList[*j].m_viPath);
         if (tmpl > level)
            level = tmpl;
      }

      if (level >= 0)
         avail[level].insert(i->first);
   }

   set<int> candidate;
   for (unsigned int i = 0; i <= m_Topology.m_uiLevel; ++ i)
   {
      if (avail[i].size() > 0)
      {
         candidate = avail[i];
         break;
      }
   }

   if (candidate.empty())
      return -1;

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   int r = int(candidate.size() * rand() / (RAND_MAX + 1.0));

   set<int>::iterator n = candidate.begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   sn = m_mSlaveList[*n];

   return 1;
}

int SlaveManager::chooseIONode(set<int>& loclist, const Address& client, const int& io, SlaveNode& sn)
{
   set<int> avail;

   if (!loclist.empty())
      avail = loclist;
   else
   {
      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
      {
         // only nodes with more than 10GB disk space are chosen
         if (i->second.m_llAvailDiskSpace > 10000000000LL)
            avail.insert(i->first);
      }
   }

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

int SlaveManager::chooseReplicaNode(set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize)
{
   set<int> locid;
   for (set<Address>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseReplicaNode(locid, sn, filesize);
}

int SlaveManager::chooseIONode(set<Address, AddrComp>& loclist, const Address& client, const int& io, SlaveNode& sn)
{
   set<int> locid;
   for (set<Address>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseIONode(locid, client, io, sn);
}

unsigned int SlaveManager::getTotalSlaves()
{
   return m_mSlaveList.size();
}

uint64_t SlaveManager::getTotalDiskSpace()
{
   uint64_t size = 0;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      size += i->second.m_llAvailDiskSpace;
   }

   return size;
}

void SlaveManager::updateClusterStat(Cluster& c)
{
   if (c.m_mSubCluster.empty())
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;

      for (set<int>::iterator i = c.m_sNodes.begin(); i != c.m_sNodes.end(); ++ i)
      {
         c.m_llAvailDiskSpace += m_mSlaveList[*i].m_llAvailDiskSpace;
         c.m_llTotalFileSize += m_mSlaveList[*i].m_llTotalFileSize;
         c.m_llTotalInputData += m_mSlaveList[*i].m_llTotalInputData;
         c.m_llTotalOutputData += m_mSlaveList[*i].m_llTotalOutputData;
      }
   }
   else
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;

      for (map<int, Cluster>::iterator i = c.m_mSubCluster.begin(); i != c.m_mSubCluster.end(); ++ i)
      {
         updateClusterStat(i->second);

         c.m_llAvailDiskSpace += i->second.m_llAvailDiskSpace;
         c.m_llTotalFileSize += i->second.m_llTotalFileSize;
         c.m_llTotalInputData += i->second.m_llTotalInputData;
         c.m_llTotalOutputData += i->second.m_llTotalOutputData;
      }
   }
}
