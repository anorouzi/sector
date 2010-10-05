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

#ifndef __SECTOR_MASTER_H__
#define __SECTOR_MASTER_H__

#include <sector.h>
#include <gmp.h>
#include <log.h>
#include <index.h>
#include <index2.h>
#include <vector>
#include <ssltransport.h>
#include <topology.h>
#include <routing.h>
#include <slavemgmt.h>
#include <transaction.h>
#include <user.h>
#include <threadpool.h>
	
struct SlaveAddr
{
   std::string m_strAddr;				// slave IP address
   std::string m_strBase;				// slave executable "start_slave" path
};

class MasterConf
{
public:
   MasterConf();

   int init(const std::string& path);

public:
   int m_iServerPort;                   // server port
   std::string m_strSecServIP;          // security server IP
   int m_iSecServPort;                  // security server port
   int m_iMaxActiveUser;                // maximum active user
   std::string m_strHomeDir;            // data directory
   int m_iReplicaNum;                   // number of replicas of each file
   MetaForm m_MetaType;                 // form of metadata
   int m_iSlaveTimeOut;                 // slave timeout threshold
   int m_iSlaveRetryTime;               // time to reload a lost slave
   int64_t m_llSlaveMinDiskSpace;       // minimum available disk space allowed on each slave
   int m_iClientTimeOut;                // client timeout threshold
   int m_iLogLevel;                     // level of logs, higher = more verbose, 0 = no log
};

class Master
{
public:
   Master();
   ~Master();

public:
   int init();
   int join(const char* ip, const int& port);
   int run();
   int stop();

private:
   static void* utility(void* s);

   ThreadJobQueue m_ServiceJobQueue;			// job queue for service thread pool
   struct ServiceJobParam
   {
      std::string ip;
      int port;
      SSLTransport* ssl;
   };
   static void* service(void* s);
   static void* serviceEx(void* p);
   int processSlaveJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);
   int processUserJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);
   int processMasterJoin(SSLTransport& s, SSLTransport& secconn, const std::string& ip);

   ThreadJobQueue m_ProcessJobQueue;
   struct ProcessJobParam
   {
      std::string ip;
      int port;
      User* user;
      int key;
      int id;
      SectorMsg* msg;
   };
   static void* process(void* s);
   static void* processEx(void* p);
   int processSysCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processFSCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processDCCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processDBCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int processMCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   int sync(const char* fileinfo, const int& size, const int& type);
   int processSyncCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);

private:
   int removeSlave(const int& id, const Address& addr);

private:
   inline void reject(const std::string& ip, const int port, int id, int32_t code);

private: // replication
   static void* replica(void* s);

   pthread_mutex_t m_ReplicaLock;
   pthread_cond_t m_ReplicaCond;

   // string format: <src file>,<dst file>
   std::vector<std::string> m_vstrToBeReplicated;	// list of files to be replicated/copied
   std::set<std::string> m_sstrOnReplicate;		// list of files currently being replicated

   int createReplica(const std::string& src, const std::string& dst);
   int removeReplica(const std::string& filename, const Address& addr);

   int populateSpecialRep(const std::string& conf, std::map<std::string, int>& special);

   int processWriteResults(const std::string& filename, std::map<int, std::string> results);

   int chooseDataToMove(std::vector<std::string>& path, const Address& addr, const int64_t& target_size);

private:
   CGMP m_GMP;						// GMP messenger

   std::string m_strSectorHome;				// $SECTOR_HOME directory, for code and configuration file location
   MasterConf m_SysConfig;				// master configuration
   std::string m_strHomeDir;				// home data directory, for system metadata

   SectorLog m_SectorLog;				// sector log

   Metadata* m_pMetadata;                               // metadata

   int m_iMaxActiveUser;				// maximum number of active users allowed
   UserManager m_UserManager;				// user management

   SlaveManager m_SlaveManager;				// slave management
   std::vector<Address> m_vSlaveList;			// list of slave addresses
   int64_t m_llLastUpdateTime;				// last update time for the slave list;

   TransManager m_TransManager;				// transaction management

   enum Status {INIT, RUNNING, STOPPED} m_Status;	// system status

   char* m_pcTopoData;					// serialized topology data
   int m_iTopoDataSize;					// size of the topology data

   Routing m_Routing;					// master routing module
   uint32_t m_iRouterKey;				// identification for this master

private:
   std::map<std::string, SlaveAddr> m_mSlaveAddrRec;	// slave and its executale path
   void loadSlaveAddr(const std::string& file);

private:
   int64_t m_llStartTime;
   int serializeSysStat(char*& buf, int& size);

   #ifdef DEBUG
   int processDebugCmd(const std::string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg);
   #endif
};

#endif
