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
   Yunhong Gu, last updated 11/28/2010
*****************************************************************************/


#include <slavemgmt.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <cstring>
#include <common.h>
#include <meta.h>

using namespace std;

SlaveManager::SlaveManager():
m_llLastUpdateTime(-1),
m_llSlaveMinDiskSpace(10000000000LL)
{
}

SlaveManager::~SlaveManager()
{
}

int SlaveManager::init(const char* topoconf)
{
   m_Cluster.m_iClusterID = 0;
   m_Cluster.m_iTotalNodes = 0;
   m_Cluster.m_llAvailDiskSpace = 0;
   m_Cluster.m_llTotalFileSize = 0;
   m_Cluster.m_llTotalInputData = 0;
   m_Cluster.m_llTotalOutputData = 0;
   m_Cluster.m_viPath.clear();

   if (m_Topology.init(topoconf) < 0)
      return -1;

   Cluster* pc = &m_Cluster;

   // insert 0/0/0/....
   for (unsigned int i = 0; i < m_Topology.m_uiLevel; ++ i)
   {
      Cluster c;
      c.m_iClusterID = 0;
      c.m_iTotalNodes = 0;
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_viPath = pc->m_viPath;
      c.m_viPath.push_back(0);

      pc->m_mSubCluster[0] = c;
      pc = &(pc->m_mSubCluster[0]);
   }

   for (vector<Topology::TopoMap>::iterator i = m_Topology.m_vTopoMap.begin(); i != m_Topology.m_vTopoMap.end(); ++ i)
   {
      pc = &m_Cluster;

      for (vector<int>::iterator l = i->m_viPath.begin(); l != i->m_viPath.end(); ++ l)
      {
         if (pc->m_mSubCluster.find(*l) == pc->m_mSubCluster.end())
         {
            Cluster c;
            c.m_iClusterID = *l;
            c.m_iTotalNodes = 0;
            c.m_llAvailDiskSpace = 0;
            c.m_llTotalFileSize = 0;
            c.m_llTotalInputData = 0;
            c.m_llTotalOutputData = 0;
            c.m_viPath = pc->m_viPath;
            c.m_viPath.push_back(*l);
            pc->m_mSubCluster[*l] = c;
         }
         pc = &(pc->m_mSubCluster[*l]);
      }
   }

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

int SlaveManager::setSlaveMinDiskSpace(const int64_t& byteSize)
{
   m_llSlaveMinDiskSpace = byteSize;
   return 0;
}

int SlaveManager::insert(SlaveNode& sn)
{
   CGuardEx sg(m_SlaveLock);

   int id = 0;
   Address addr;
   if (checkduplicateslave_(sn.m_strIP, sn.m_strStoragePath, id, addr))
      return -1;

   sn.m_llLastUpdateTime = CTimer::getTime();
   sn.m_llLastVoteTime = CTimer::getTime();
   if (sn.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
      sn.m_iStatus = SlaveStatus::NORMAL;
   else
      sn.m_iStatus = SlaveStatus::DISKFULL;
   m_Topology.lookup(sn.m_strIP.c_str(), sn.m_viPath);
   m_mSlaveList[sn.m_iNodeID] = sn;

   addr.m_strIP = sn.m_strIP;
   addr.m_iPort = sn.m_iPort;
   m_mAddrList[addr] = sn.m_iNodeID;

   Cluster* sc = &m_Cluster;
   map<int, Cluster>::iterator pc = sc->m_mSubCluster.end();
   for (vector<int>::iterator i = sn.m_viPath.begin(); ;)
   {
      pc = sc->m_mSubCluster.find(*i);
      if (pc == sc->m_mSubCluster.end())
      {
         //impossble
         break;
      }

      pc->second.m_iTotalNodes ++;
      if (sn.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         pc->second.m_llAvailDiskSpace += sn.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      pc->second.m_llTotalFileSize += sn.m_llTotalFileSize;

      if (++ i == sn.m_viPath.end())
         break;

      sc = &(pc->second);
   }

   if (pc != sc->m_mSubCluster.end())
   {
      pc->second.m_sNodes.insert(sn.m_iNodeID);
   }
   else
   {
      // IMPOSSIBLE
   }

   map<string, set<string> >::iterator i = m_mIPFSInfo.find(sn.m_strIP);
   if (i == m_mIPFSInfo.end())
      m_mIPFSInfo[sn.m_strIP].insert(Metadata::revisePath(sn.m_strStoragePath));
   else
      i->second.insert(sn.m_strStoragePath);

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

int SlaveManager::remove(int nodeid)
{
   CGuardEx sg(m_SlaveLock);

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
   map<int, Cluster>::iterator pc = sc->end();
   for (vector<int>::iterator i = path.begin(); i != path.end(); ++ i)
   {
      if ((pc = sc->find(*i)) == sc->end())
      {
         // something wrong
         break;
      }

      pc->second.m_iTotalNodes --;
      if (sn->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         pc->second.m_llAvailDiskSpace -= sn->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      pc->second.m_llTotalFileSize -= sn->second.m_llTotalFileSize;

      sc = &(pc->second.m_mSubCluster);
   }

   pc->second.m_sNodes.erase(nodeid);

   map<string, set<string> >::iterator i = m_mIPFSInfo.find(sn->second.m_strIP);
   if (i != m_mIPFSInfo.end())
   {
      i->second.erase(sn->second.m_strStoragePath);
      if (i->second.empty())
         m_mIPFSInfo.erase(i);
   }
   else
   {
      //something wrong
   }

   m_mSlaveList.erase(sn);

   m_llLastUpdateTime = CTimer::getTime();

   return 1;
}

bool SlaveManager::checkDuplicateSlave(const string& ip, const string& path, int32_t& id, Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   return checkduplicateslave_(ip, path, id, addr);
}

bool SlaveManager::checkduplicateslave_(const string& ip, const string& path, int32_t& id, Address& addr)
{
   map<string, set<string> >::iterator i = m_mIPFSInfo.find(ip);
   if (i == m_mIPFSInfo.end())
      return false;

   string revised_path = Metadata::revisePath(path);
   for (set<string>::iterator j = i->second.begin(); j != i->second.end(); ++ j)
   {
      // if there is overlap between the two storage paths, it means that there is a conflict
      // the new slave should be rejected in this case

      vector<string> dir1;
      vector<string> dir2;
      Metadata::parsePath(*j, dir1);
      Metadata::parsePath(path, dir2);

      int n = (dir1.size() < dir2.size()) ? dir1.size() : dir2.size();
      bool match = true;
      for (int i = 0; i < n; ++ i)
      {
         if (dir1[i] != dir2[i])
         {
            match = false;
            break;
         }
      }

      if (!match)
         continue;

      //TODO: optimize this search
      id = -1;
      for (map<int, SlaveNode>::const_iterator s = m_mSlaveList.begin(); s != m_mSlaveList.end(); ++ s)
      {
         if ((s->second.m_strIP == ip) && (s->second.m_strStoragePath == *j))
         {
            id = s->first;
            addr.m_strIP = s->second.m_strIP;
            addr.m_iPort = s->second.m_iPort;
            break;
         }
      }

      return true;
   }

   return false;
}

int SlaveManager::chooseReplicaNode(set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   CGuardEx sg(m_SlaveLock);
   return choosereplicanode_(loclist, sn, filesize, rep_dist, restrict_loc);
}

int SlaveManager::choosereplicanode_(set<int>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   vector< set<int> > avail;
   avail.resize(m_Topology.m_uiLevel + 2);

   // find the topology of current replicas
   vector< vector<int> > locpath;
   for (set<int>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      map<int, SlaveNode>::iterator p = m_mSlaveList.find(*i);
      if (p == m_mSlaveList.end())
         continue;
      locpath.push_back(p->second.m_viPath);
   }

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      // skip bad&lost slaves
      if (i->second.m_iStatus != SlaveStatus::NORMAL)
         continue;

      // only nodes with more than minimum availale disk space are chosen
      if (i->second.m_llAvailDiskSpace < (m_llSlaveMinDiskSpace + filesize))
         continue;

      // cannot replicate to a node already having the data
      if (loclist.find(i->first) != loclist.end())
         continue;

      // if a location restriction is applied to the file, only limited nodes can be chosen
      if ((NULL != restrict_loc) && (!restrict_loc->empty()))
      {
         if (Topology::match(i->second.m_viPath, *restrict_loc) < restrict_loc->size())
            continue;
      }

      // calculate the distance from this slave node to the current replicas
      int level = m_Topology.max_distance(i->second.m_viPath, locpath);

      // if users define a replication distance, then only nodes within rep_dist can be chosen
      if ((rep_dist >= 0) && (level > rep_dist))
         continue;

      // level <= m_Topology.m_uiLevel + 1
      if (level >= 0)
         avail[level].insert(i->first);
   }

   set<int> candidate;

   // choose furthest node within replica distance
   for (int i = m_Topology.m_uiLevel + 1; i >= 0; -- i)
   {
      if (!avail[i].empty())
      {
         candidate = avail[i];
         break;
      }
   }

   if (candidate.empty())
      return SectorError::E_NODISK;

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   int r = int(candidate.size() * (double(rand()) / RAND_MAX));
   set<int>::iterator n = candidate.begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   sn = m_mSlaveList[*n];
   if (sn.m_iActiveTrans == 0)
      return 1;

   for (set<int>::iterator i = candidate.begin(); i != candidate.end(); ++ i)
   {
      if (m_mSlaveList[*i].m_iActiveTrans < sn.m_iActiveTrans)
      {
         sn = m_mSlaveList[*i];
         if (sn.m_iActiveTrans == 0)
            break;
      }
   }

   return 1;
}
 
int SlaveManager::chooseIONode(set<int>& loclist, int mode, vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist, const vector<int>* restrict_loc)
{
   CGuardEx sg(m_SlaveLock);

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_usec);

   sl.clear();

   if (m_mSlaveList.empty())
      return SectorError::E_NODISK;

   if (!loclist.empty())
   {
      SlaveNode sn;
      findNearestNode(loclist, option.m_strHintIP, sn);

      sl.push_back(sn);

      // if this is a READ_ONLY operation, one node is enough
      if ((mode & SF_MODE::WRITE) == 0)
         return sl.size();

      // the first node will be the closest to the client; the client writes to that node only
      for (set<int>::iterator i = loclist.begin(); i != loclist.end(); i ++)
      {
         if (*i == sn.m_iNodeID)
            continue;

         sl.push_back(m_mSlaveList[*i]);
      }
   }
   else
   {
      // no available nodes for READ_ONLY operation
      if ((mode & SF_MODE::WRITE) == 0)
         return 0;


      //TODO: optimize the node selection process; no need to scan all nodes

      set<int> avail;

      vector<int> path_limit;
      if (option.m_strCluster.c_str()[0] != '\0')
         Topology::parseTopo(option.m_strCluster.c_str(), path_limit);

      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
      {
         // skip bad & lost nodes
         if (i->second.m_iStatus != SlaveStatus::NORMAL)
            continue;

         // if client specifies a cluster ID, then only nodes on the cluster are chosen
         if (!path_limit.empty())
         {
            if (Topology::match(path_limit, i->second.m_viPath) < path_limit.size())
               continue;
         }

         // if there is location restriction on the file, check path as well
         if ((NULL != restrict_loc) && (!restrict_loc->empty()))
         {
            if (Topology::match(i->second.m_viPath, *restrict_loc) < restrict_loc->size())
               continue;
         }

         // only nodes with more than minimum available disk space are chosen
         if (i->second.m_llAvailDiskSpace > (m_llSlaveMinDiskSpace + option.m_llReservedSize))
            avail.insert(i->first);
      }

      if (avail.empty())
         return SectorError::E_NODISK;

      SlaveNode sn;
      findNearestNode(avail, option.m_strHintIP, sn);

      sl.push_back(sn);

      // otherwise choose more nodes for immediate replica
      for (int i = 0; i < option.m_iReplicaNum - 1; ++ i)
      {
         set<int> locid;
         for (vector<SlaveNode>::iterator j = sl.begin(); j != sl.end(); ++ j)
            locid.insert(j->m_iNodeID);

         if (choosereplicanode_(locid, sn, option.m_llReservedSize, rep_dist, restrict_loc) <= 0)
            break;

         sl.push_back(sn);
      }
   }

   return sl.size();
}

int SlaveManager::chooseReplicaNode(set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize, const int rep_dist, const vector<int>* restrict_loc)
{
   set<int> locid;
   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseReplicaNode(locid, sn, filesize, rep_dist, restrict_loc);
}

int SlaveManager::chooseIONode(set<Address, AddrComp>& loclist, int mode, vector<SlaveNode>& sl, const SF_OPT& option, const int rep_dist, const vector<int>* restrict_loc)
{
   set<int> locid;
   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      locid.insert(m_mAddrList[*i]);
   }

   return chooseIONode(locid, mode, sl, option, rep_dist, restrict_loc);
}

int SlaveManager::chooseSPENodes(const Address& /*client*/, vector<SlaveNode>& sl)
{
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      // skip bad&lost slaves
      if (i->second.m_iStatus != SlaveStatus::NORMAL)
         continue;

      // only nodes with more than minimum available disk space are chosen
      if (i->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace)
         continue;

      sl.push_back(i->second);

      //TODO:: add more creteria to choose nodes
   }

   return sl.size();
}

int SlaveManager::chooseLessReplicaNode(std::set<Address, AddrComp>& loclist, Address& addr)
{
   if (loclist.empty())
      return -1;

   int64_t min_avail_space = -1;

   for (set<Address, AddrComp>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      int slave_id = m_mAddrList[*i];
      SlaveNode sn = m_mSlaveList[slave_id];
      if ((sn.m_llAvailDiskSpace < min_avail_space) || (min_avail_space < 0))
         addr = *i;
   }

   return 0;
}

int SlaveManager::serializeTopo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   buf = NULL;
   size = m_Topology.getTopoDataSize();
   buf = new char[size];
   m_Topology.serialize(buf, size);

   return size;
}

int SlaveManager::updateSlaveList(vector<Address>& sl, int64_t& last_update_time)
{
   CGuardEx sg(m_SlaveLock);

   if (last_update_time < m_llLastUpdateTime)
   {
      sl.clear();
      for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
      {
         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         sl.push_back(addr);
      }
   }

   last_update_time = CTimer::getTime();

   return sl.size();
}

int SlaveManager::updateSlaveInfo(const Address& addr, const char* info, const int& len)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::iterator a = m_mAddrList.find(addr);
   if (a == m_mAddrList.end())
      return -1;

   map<int, SlaveNode>::iterator s = m_mSlaveList.find(a->second);
   if (s == m_mSlaveList.end())
   {
      //THIS SHOULD NOT HAPPEN
      return -1;
   }

   if (s->second.m_iStatus == SlaveStatus::DOWN)
   {
      // "lost" slaves must be restarted, as files might have been changed
      return -1;
   }

   s->second.m_llLastUpdateTime = CTimer::getTime();
   s->second.m_llTimeStamp = CTimer::getTime();
   s->second.deserialize(info, len);

   if (s->second.m_iStatus == SlaveStatus::BAD)
      return -1;

   if (s->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace)
      s->second.m_iStatus = SlaveStatus::DISKFULL;
   else
      s->second.m_iStatus = SlaveStatus::NORMAL;

   return 0;
}

int SlaveManager::updateSlaveTS(const Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::iterator a = m_mAddrList.find(addr);
   if (a == m_mAddrList.end())
      return -1;

   map<int, SlaveNode>::iterator s = m_mSlaveList.find(a->second);
   if (s == m_mSlaveList.end())
   {
      //THIS SHOULD NOT HAPPEN
      return -1;
   }

   s->second.m_llLastUpdateTime = CTimer::getTime();

   return 0;
}

int SlaveManager::checkBadAndLost(map<int, SlaveNode>& bad, map<int, SlaveNode>& lost, map<int, SlaveNode>& retry, map<int, SlaveNode>& dead, const int64_t& timeout, const int64_t& retrytime)
{
   CGuardEx sg(m_SlaveLock);

   bad.clear();
   lost.clear();
   retry.clear();
   dead.clear();

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (i->second.m_iStatus == SlaveStatus::DOWN)
      {
         // if the node is already marked down, try to restart or remove permanently
         if (CTimer::getTime() - i->second.m_llLastUpdateTime >= (uint64_t)retrytime)
         {
            dead[i->first] = i->second;
         }
         else
         {
            retry[i->first] = i->second;
         }

         continue;
      }

      // clear expired votes
      if (i->second.m_llLastVoteTime - CTimer::getTime() > 24LL * 60 * 3600 * 1000000)
      {
         i->second.m_sBadVote.clear();
         i->second.m_llLastVoteTime = CTimer::getTime();
      }

      // if received more than half votes, it is bad
      if (i->second.m_sBadVote.size() * 2 > m_mSlaveList.size())
      {
         bad[i->first] = i->second;
         i->second.m_iStatus = SlaveStatus::BAD;
      }

      // detect slave timeout
      if (CTimer::getTime() - i->second.m_llLastUpdateTime >= (uint64_t)timeout)
      {
         lost[i->first] = i->second;
         i->second.m_iStatus = SlaveStatus::DOWN;
      }
   }

   return 0;
}

int SlaveManager::serializeSlaveList(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   buf = new char [(4 + 4 + 64 + 4 + 4) * m_mSlaveList.size()];

   char* p = buf;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      *(int32_t*)p = i->first;
      p += 4;
      *(int32_t*)p = i->second.m_strIP.length() + 1;
      p += 4;
      strcpy(p, i->second.m_strIP.c_str());
      p += i->second.m_strIP.length() + 1;
      *(int32_t*)p = i->second.m_iPort;
      p += 4;
      *(int32_t*)p = i->second.m_iDataPort;
      p += 4;
   }

   size = p - buf;

   return m_mSlaveList.size();
}

int SlaveManager::deserializeSlaveList(int num, const char* buf, int /*size*/)
{
   const char* p = buf;
   for (int i = 0; i < num; ++ i)
   {
      SlaveNode sn;
      sn.m_iNodeID = *(int32_t*)p;
      p += 4;
      int32_t size = *(int32_t*)p;
      p += 4;
      sn.m_strIP = p;
      p += size;
      sn.m_iPort = *(int32_t*)p;
      p += 4;
      sn.m_iDataPort = *(int32_t*)p;
      p += 4;

      insert(sn);
   }

   updateClusterStat();

   return 0;
}

int SlaveManager::getSlaveID(const Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<Address, int, AddrComp>::const_iterator i = m_mAddrList.find(addr);

   if (i == m_mAddrList.end())
      return -1;

   return i->second;
}

int SlaveManager::getSlaveAddr(const int& id, Address& addr)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(id);

   if (i == m_mSlaveList.end())
      return -1;

   addr.m_strIP = i->second.m_strIP;
   addr.m_iPort = i->second.m_iPort;

   return 0;
}

int SlaveManager::voteBadSlaves(const Address& voter, int num, const char* buf)
{
   CGuardEx sg(m_SlaveLock);

   int vid = m_mAddrList[voter];
   for (int i = 0; i < num; ++ i)
   {
      Address addr;
      addr.m_strIP = buf + i * 68;
      addr.m_iPort = *(int*)(buf + i * 68 + 64);

      int slave = m_mAddrList[addr];
      m_mSlaveList[slave].m_sBadVote.insert(vid);
   }

   return 0;
}

unsigned int SlaveManager::getNumberOfClusters()
{
   CGuardEx sg(m_SlaveLock);

   return m_Cluster.m_mSubCluster.size();
}

unsigned int SlaveManager::getNumberOfSlaves()
{
   CGuardEx sg(m_SlaveLock);

   return m_mSlaveList.size();
}

int SlaveManager::serializeClusterInfo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   size = 4 + m_Cluster.m_mSubCluster.size() * 40;
   buf = new char[size];

   *(int32_t*)buf = m_Cluster.m_mSubCluster.size();

   char* p = buf + 4;
   for (map<int, Cluster>::iterator i = m_Cluster.m_mSubCluster.begin(); i != m_Cluster.m_mSubCluster.end(); ++ i)
   {
      *(int32_t*)p = i->second.m_iClusterID;
      *(int32_t*)(p + 4) = i->second.m_iTotalNodes;
      *(int64_t*)(p + 8) = i->second.m_llAvailDiskSpace;
      *(int64_t*)(p + 16) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 24) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 32) = i->second.m_llTotalOutputData;

      p += 40;
   }

   return size;
}

int SlaveManager::serializeSlaveInfo(char*& buf, int& size)
{
   CGuardEx sg(m_SlaveLock);

   size = 4;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      size += i->second.m_strStoragePath.length() + 1;
   }
   size += m_mSlaveList.size() * 92;

   buf = new char[size];

   *(int32_t*)buf = m_mSlaveList.size();

   char* p = buf + 4;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      *(int32_t*)p = i->first;
      strcpy(p + 4, i->second.m_strIP.c_str());
      *(int32_t*)(p + 20) = i->second.m_iPort;
      int64_t avail_size = 0;
      if (i->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         avail_size = i->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
      *(int64_t*)(p + 24) = avail_size;
      *(int64_t*)(p + 32) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 40) = i->second.m_llCurrMemUsed;
      *(int64_t*)(p + 48) = i->second.m_llCurrCPUUsed;
      *(int64_t*)(p + 56) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 64) = i->second.m_llTotalOutputData;
      *(int64_t*)(p + 72) = i->second.m_llTimeStamp;
      *(int64_t*)(p + 80) = i->second.m_iStatus;
      *(int64_t*)(p + 84) = i->second.m_viPath[0];
      *(int64_t*)(p + 88) = i->second.m_strStoragePath.length() + 1;
      p+= 92;
      strcpy(p, i->second.m_strStoragePath.c_str());
      p+= i->second.m_strStoragePath.length() + 1;
   }

   return size;
}

uint64_t SlaveManager::getTotalDiskSpace()
{
   CGuardEx sg(m_SlaveLock);

   uint64_t size = 0;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if ((i->second.m_iStatus == SlaveStatus::DOWN) || (i->second.m_iStatus == SlaveStatus::BAD))
         continue;

      if (i->second.m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
         size += i->second.m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
   }

   return size;
}

void SlaveManager::updateClusterStat()
{
   CGuardEx sg(m_SlaveLock);

   updateclusterstat_(m_Cluster);
}

void SlaveManager::updateclusterstat_(Cluster& c)
{
   if (c.m_mSubCluster.empty())
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_mSysIndInput.clear();
      c.m_mSysIndOutput.clear();

      for (set<int>::iterator i = c.m_sNodes.begin(); i != c.m_sNodes.end(); ++ i)
      {
         SlaveNode* s = &m_mSlaveList[*i];

         if (s->m_iStatus == SlaveStatus::DOWN)
            continue;

         if (s->m_llAvailDiskSpace > m_llSlaveMinDiskSpace)
            c.m_llAvailDiskSpace += s->m_llAvailDiskSpace - m_llSlaveMinDiskSpace;
         c.m_llTotalFileSize += s->m_llTotalFileSize;
         updateclusterio_(c, s->m_mSysIndInput, c.m_mSysIndInput, c.m_llTotalInputData);
         updateclusterio_(c, s->m_mSysIndOutput, c.m_mSysIndOutput, c.m_llTotalOutputData);
      }
   }
   else
   {
      c.m_llAvailDiskSpace = 0;
      c.m_llTotalFileSize = 0;
      c.m_llTotalInputData = 0;
      c.m_llTotalOutputData = 0;
      c.m_mSysIndInput.clear();
      c.m_mSysIndOutput.clear();
	    
      for (map<int, Cluster>::iterator i = c.m_mSubCluster.begin(); i != c.m_mSubCluster.end(); ++ i)
      {
         updateclusterstat_(i->second);

         c.m_llAvailDiskSpace += i->second.m_llAvailDiskSpace;
         c.m_llTotalFileSize += i->second.m_llTotalFileSize;
         updateclusterio_(c, i->second.m_mSysIndInput, c.m_mSysIndInput, c.m_llTotalInputData);
         updateclusterio_(c, i->second.m_mSysIndOutput, c.m_mSysIndOutput, c.m_llTotalOutputData);
      }
   }
}

void SlaveManager::updateclusterio_(Cluster& c, map<string, int64_t>& data_in, map<string, int64_t>& data_out, int64_t& total)
{
   for (map<string, int64_t>::iterator p = data_in.begin(); p != data_in.end(); ++ p)
   {
      vector<int> path;
      m_Topology.lookup(p->first.c_str(), path);
      if (m_Topology.match(c.m_viPath, path) == c.m_viPath.size())
         continue;

      map<string, int64_t>::iterator n = data_out.find(p->first);
      if (n == data_out.end())
         data_out[p->first] = p->second;
      else
         n->second += p->second;

      total += p->second;
   }
}

int SlaveManager::getSlaveListByRack(map<int, Address>& sl, const string& topopath)
{
   CGuardEx sg(m_SlaveLock);

   vector<int> path;
   if (m_Topology.parseTopo(topopath.c_str(), path) < 0)
      return -1;

   sl.clear();
   unsigned int len = path.size();

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (len > i->second.m_viPath.size())
         continue;

      bool match = true;
      for (unsigned int p = 0; p < len; ++ p)
      {
         if (path[p] != i->second.m_viPath[p])
         {
            match = false;
            break;
         }
      }

      if (match)
      {
         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         sl[i->first] = addr;
      }
   }

   return sl.size();
}

int SlaveManager::checkStorageBalance(map<int64_t, Address>& lowdisk)
{
   CGuardEx sg(m_SlaveLock);

   if (m_mSlaveList.empty())
      return 0;

   lowdisk.clear();

   uint64_t size = 0;
   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (i->second.m_iStatus == SlaveStatus::NORMAL)
         size += i->second.m_llAvailDiskSpace;
   }

   int64_t avg = size / m_mSlaveList.size();

   //TODO: using "target" value as key may cause certain low disk node to be ignored.
   // such node may be included again in the next round of check.
   // a better method can be used in the future.

   for (map<int, SlaveNode>::iterator i = m_mSlaveList.begin(); i != m_mSlaveList.end(); ++ i)
   {
      if (i->second.m_llAvailDiskSpace <= m_llSlaveMinDiskSpace)
      {
         int64_t target;
         if (avg > m_llSlaveMinDiskSpace)
            target = avg  - i->second.m_llAvailDiskSpace;
         else
            target = m_llSlaveMinDiskSpace - i->second.m_llAvailDiskSpace;

         lowdisk[target].m_strIP = i->second.m_strIP;
         lowdisk[target].m_iPort = i->second.m_iPort;
      }
   }

   return lowdisk.size();
}

int SlaveManager::findNearestNode(std::set<int>& loclist, const std::string& ip, SlaveNode& sn)
{
   if (loclist.empty())
      return SectorError::E_NODISK;

   // find nearest node, if equal distance, choose a random one
   int dist = -1;
   map<int, vector<int> > dist_vec;
   for (set<int>::iterator i = loclist.begin(); i != loclist.end(); ++ i)
   {
      int d = m_Topology.distance(ip.c_str(), m_mSlaveList[*i].m_strIP.c_str());

      dist_vec[d].push_back(*i);

      if ((d < dist) || (dist < 0))
         dist = d;
   }

   // if no slave node found, return 0
   if (dist < 0)
      return SectorError::E_NODISK;

   // chose nearest node first then least busy node, a random one if the first two conditions equal
   // TODO: this code can be slightly optimized

   int r = int(dist_vec[dist].size() * (double(rand()) / RAND_MAX)) % dist_vec[dist].size();
   vector<int>::iterator n = dist_vec[dist].begin();
   for (int i = 0; i < r; ++ i)
      n ++;

   sn = m_mSlaveList[*n];
   if (sn.m_iActiveTrans == 0)
      return 0;

   for (vector<int>::iterator i = dist_vec[dist].begin(); i != dist_vec[dist].end(); ++ i)
   {
      if (m_mSlaveList[*i].m_iActiveTrans < sn.m_iActiveTrans)
      {
         sn = m_mSlaveList[*i];
         if (sn.m_iActiveTrans == 0)
            break;
      }
   }

   return 0;
}

void SlaveManager::incActTrans(const int& slaveid)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(slaveid);
   if (i != m_mSlaveList.end())
      ++ i->second.m_iActiveTrans;
}

void SlaveManager::decActTrans(const int& slaveid)
{
   CGuardEx sg(m_SlaveLock);

   map<int, SlaveNode>::iterator i = m_mSlaveList.find(slaveid);
   if ((i != m_mSlaveList.end()) && (i->second.m_iActiveTrans > 0))
      -- i->second.m_iActiveTrans;
}
