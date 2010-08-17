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
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/


#ifndef __SECTOR_CLIENT_H__
#define __SECTOR_CLIENT_H__

#include <gmp.h>
#include <datachn.h>
#include <routing.h>
#include <sector.h>
#include "fscache.h"
#ifndef WIN32
   #include <pthread.h>
#endif

class FSClient;
class DCClient;

class Client
{
friend class FSClient;
friend class DCClient;

public:
   Client();
   ~Client();

public:
   int init(const std::string& server, const int& port);
   int login(const std::string& username, const std::string& password, const char* cert = NULL);
   int login(const std::string& serv_ip, const int& serv_port);
   int logout();
   int close();

   int list(const std::string& path, std::vector<SNode>& attr);
   int stat(const std::string& path, SNode& attr);
   int mkdir(const std::string& path);
   int move(const std::string& oldpath, const std::string& newpath);
   int remove(const std::string& path);
   int rmr(const std::string& path);
   int copy(const std::string& src, const std::string& dst);
   int utime(const std::string& path, const int64_t& ts);

public:
   //int createTable();
   //int listTable();
   //int deleteTable();

public:
   int sysinfo(SysStat& sys);
   int shutdown(const int& type, const std::string& param = "");
   int fsck(const std::string& path);

   #ifdef DEBUG
   int sendDebugCode(const int32_t& slave_id, const int32_t& code);
   int sendDebugCode(const std::string& slave_addr, const int32_t& code);
   #endif

public:
   FSClient* createFSClient();
   DCClient* createDCClient();
   //DBClient* createDBClient();
   int releaseFSClient(FSClient* sf);
   int releaseDCClient(DCClient* sp);
   //int releaseDBClient(DBClient* sd);

public:
   int setMaxCacheSize(const int64_t ms);

protected:
   int updateMasters();
   int lookup(const std::string& path, Address& serv_addr);
   int lookup(const int32_t& key, Address& serv_addr);
   int deserializeSysStat(SysStat& sys, char* buf, int size);
   int retrieveMasterInfo(std::string& certfile);

protected:
   std::string m_strUsername;           // user account name
   std::string m_strPassword;           // user password
   std::string m_strCert;               // master certificate

   std::set<Address, AddrComp> m_sMasters;      // masters
   pthread_mutex_t m_MasterSetLock;

   Routing m_Routing;                   // master routing module

   std::string m_strServerHost;		// original master server domain name
   std::string m_strServerIP;		// original master server IP address
   int m_iServerPort;			// original master server port

protected:
   CGMP m_GMP;				// GMP
   DataChn m_DataChn;			// data channel
   int32_t m_iKey;			// user key

   // this is the global key/iv for this client. do not share this for all connections; a new connection should duplicate this
   unsigned char m_pcCryptoKey[16];
   unsigned char m_pcCryptoIV[8];

   Topology m_Topology;			// slave system topology

   SectorError m_ErrorInfo;		// error description

   Cache m_Cache;			// file client cache

private:
   int m_iCount;			// number of concurrent logins

protected: // the following are used for keeping alive with the masters
   bool m_bActive;
   pthread_t m_KeepAlive;
   pthread_cond_t m_KACond;
   pthread_mutex_t m_KALock;
#ifndef WIN32
   static void* keepAlive(void*);
#else
   static DWORD WINAPI keepAlive(LPVOID);
#endif

protected:
   pthread_mutex_t m_IDLock;
   int m_iID;					// seed of id for each file or process
   std::map<int, FSClient*> m_mFSList;		// list of open files
   std::map<int, DCClient*> m_mDCList;		// list of active process
};

#endif
