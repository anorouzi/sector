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
   Yunhong Gu [gu@lac.uic.edu], last updated 06/28/2008
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
#include <transaction.h>

class ActiveUser
{
public:
   int deserialize(std::vector<string>& dirs, const std::string& buf);
   bool match(const std::string& path, int rwx);

public:
   string m_strName;
   string m_strIP;
   int m_iPort;
   int32_t m_iKey;
   int64_t m_llLastRefreshTime;
   std::vector<string> m_vstrReadList;
   std::vector<string> m_vstrWriteList;
   std::vector<string> m_vstrExecList;
};

struct SlaveAddr
{
   string m_strAddr;
   string m_strBase;
};

class Master
{
public:
   Master();
   ~Master();

public:
   int init();
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
   static void* processEx(void* p);

   inline void reject(char* ip, int port, int id, int32_t code);

private:
   std::set<std::string> m_sstrOnReplicate;

   void checkReplica(std::map<std::string, SNode>& currdir, const std::string& currpath, std::vector<std::string>& replica);
   int createReplica(const string& path);
   int removeReplica(const string& path);

private:
   CGMP m_GMP;

   MasterConf m_SysConfig;
   std::string m_strHomeDir;

   SectorLog m_SectorLog;
   SysStat m_SysStat;

   int m_iMaxActiveUser;
   std::map<int, ActiveUser> m_mActiveUser;

   Index m_Metadata;
   pthread_mutex_t m_MetaLock;

   SlaveManager m_SlaveManager;

   TransManager m_TransManager;

   enum Status {INIT, RUNNING, STOPPED} m_Status;

private:
   std::map<string, SlaveAddr> m_mSlaveAddrRec;
   void loadSlaveAddr(string file);
};

#endif
