/*****************************************************************************
Copyright (c) 2005 - 2010, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 04/25/2010
*****************************************************************************/


#ifndef __SECTOR_SLAVEMGMT_H__
#define __SECTOR_SLAVEMGMT_H__

#include "common.h"
#include "topology.h"

#ifdef WIN32
    #ifdef MASTER_EXPORTS
        #define MASTER_API __declspec(dllexport)
    #else
        #define MASTER_API __declspec(dllimport)
    #endif
#else
    #define MASTER_API
#endif


class MASTER_API SlaveManager
{
public:
   SlaveManager();
   ~SlaveManager();

public:
   int init(const char* topoconf);

   int setSlaveMinDiskSpace(const int64_t& byteSize);

   int insert(SlaveNode& sn);
   int remove(int nodeid);

   bool checkDuplicateSlave(const std::string& ip, const std::string& path);

public:
   int chooseReplicaNode(std::set<int>& loclist, SlaveNode& sn, const int64_t& filesize);
   int chooseIONode(std::set<int>& loclist, const Address& client, int mode, std::vector<SlaveNode>& sl, int replica);
   int chooseReplicaNode(std::set<Address, AddrComp>& loclist, SlaveNode& sn, const int64_t& filesize);
   int chooseIONode(std::set<Address, AddrComp>& loclist, const Address& client, int mode, std::vector<SlaveNode>& sl, int replica);
   int chooseSPENodes(const Address& client, std::vector<SlaveNode>& sl);

public:
   int serializeTopo(char*& buf, int& size);
   int updateSlaveList(std::vector<Address>& sl, int64_t& last_update_time);
   int updateSlaveInfo(const Address& addr, const char* info, const int& len);
   int increaseRetryCount(const Address& addr);
   int checkBadAndLost(std::map<int, Address>& bad, std::map<int, Address>& lost);
   int serializeSlaveList(char*& buf, int& size);
   int deserializeSlaveList(int num, const char* buf, int size);
   int getSlaveID(const Address& addr);
   int getSlaveAddr(const int& id, Address& addr);
   int voteBadSlaves(const Address& voter, int num, const char* buf);
   unsigned int getNumberOfClusters();
   unsigned int getNumberOfSlaves();
   int serializeClusterInfo(char* buf, int& size);
   int serializeSlaveInfo(char* buf, int& size);

public:
   uint64_t getTotalDiskSpace();
   void updateClusterStat();

private:
   void updateclusterstat_(Cluster& c);
   void updateclusterio_(Cluster& c, std::map<std::string, int64_t>& data_in, std::map<std::string, int64_t>& data_out, int64_t& total);

private:
   std::map<Address, int, AddrComp> m_mAddrList;		// list of slave addresses
   std::map<int, SlaveNode> m_mSlaveList;			// list of slaves
   std::set<int> m_siBadSlaves;					// list of bad slaves

   Topology m_Topology;						// slave system topology definition
   Cluster m_Cluster;						// topology structure

   std::map<std::string, std::set<std::string> > m_mIPFSInfo;	// storage path on each slave node; used to avoid conflict

   int64_t m_llLastUpdateTime;					// last update time on the slave list

   int64_t m_llSlaveMinDiskSpace;				// minimum available disk space per slave node

private:
   CMutex m_SlaveLock;
};

#endif
