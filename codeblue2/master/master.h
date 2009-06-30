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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/05/2009
*****************************************************************************/


#ifndef __SECTOR_MASTER_H__
#define __SECTOR_MASTER_H__

#include <gmp.h>
#include <transport.h>
#include <log.h>
#include <sysstat.h>
#include <conf.h>
#include <index.h>
#include <vector>
#include <ssltransport.h>
#include <topology.h>
#include <routing.h>
#include <slavemgmt.h>
#include <transaction.h>
#include <user.h>


struct SlaveAddr
{
   std::string m_strAddr;
   std::string m_strBase;
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
   static void* service(void* s);
   struct Param
   {
      std::string ip;
      int port;
      Master* self;
      SSLTransport* ssl;
   };
   static void* serviceEx(void* p);

   static void* process(void* s);

private:
   inline void reject(char* ip, int port, int id, int32_t code);

private:
   static void* replica(void* s);

   pthread_mutex_t m_ReplicaLock;
   pthread_cond_t m_ReplicaCond;

   // string format: <src file>,<dst file>
   std::vector<std::string> m_vstrToBeReplicated;	// list of files to be replicated/copied
   std::set<std::string> m_sstrOnReplicate;		// list of files currently being replicated

   void checkReplica(std::map<std::string, SNode>& currdir, const std::string& currpath, std::vector<std::string>& replica);
   int createReplica(const std::string& src, const std::string& dst);

private:
   CGMP m_GMP;						// GMP messenger

   MasterConf m_SysConfig;				// master configuration
   std::string m_strHomeDir;				// home data directory, for system metadata

   SectorLog m_SectorLog;				// sector log

   SysStat m_SysStat;					// system statistics

   int m_iMaxActiveUser;				// maximum number of active users allowed
   std::map<int, ActiveUser> m_mActiveUser;		// list of active users

   Index m_Metadata;					// in-memory metadata
   SlaveManager m_SlaveManager;				// slave management
   TransManager m_TransManager;				// transaction management

   enum Status {INIT, RUNNING, STOPPED} m_Status;	// system status

   char* m_pcTopoData;					// serialized topology data
   int m_iTopoDataSize;					// size of the topology data

private:
   std::map<std::string, SlaveAddr> m_mSlaveAddrRec;
   void loadSlaveAddr(const std::string& file);

private: // master routing
   Routing m_Routing;
   uint32_t m_iRouterKey;
};

#endif
