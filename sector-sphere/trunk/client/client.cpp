/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 03/15/2011
*****************************************************************************/

#ifndef WIN32
   #include <netdb.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <iostream>

#include "client.h"
#include "clientmgmt.h"
#include "common.h"
#include "crypto.h"
#include "dcclient.h"
#include "fsclient.h"
#include "message.h"
#include "ssltransport.h"
#include "tcptransport.h"

#ifdef WIN32
   #define pthread_self() GetCurrentThreadId()
#endif

using namespace std;
using namespace sector;

ClientMgmt Client::g_ClientMgmt;

Client::Client():
m_strUsername(""),
m_strPassword(""),
m_strCert(""),
m_strServerIP(""),
m_iKey(0),
m_iCount(0),
m_bActive(false),
m_iID(0)
{
   CGuard::createMutex(m_IDLock);
}

Client::~Client()
{
   CGuard::releaseMutex(m_IDLock);
}

int Client::init()
{
   if (m_iCount ++ > 0)
      return 0;

   UDTTransport::initialize();
   m_ErrorInfo.init();
   Crypto::generateKey(m_pcCryptoKey, m_pcCryptoIV);
   if (m_GMP.init(0) < 0)
      return SectorError::E_GMP;
   int dataport = 0;
   if (m_DataChn.init("", dataport) < 0)
      return SectorError::E_DATACHN;

   m_bActive = true;
#ifndef WIN32
   pthread_create(&m_KeepAlive, NULL, keepAlive, this);
#else
   m_KeepAlive = CreateThread(NULL, 0, keepAlive, this, 0, NULL);
#endif

   m_Log << LogStart(LogLevel::LEVEL_1) << "Sector client initialized" << LogEnd();
   return 0;
}

int Client::login(const std::string& serv_ip, const int& serv_port,
                  const string& username, const string& password, const char* cert)
{
   if (m_iKey > 0)
      return m_iKey;

   struct addrinfo* serv_addr;
   if (getaddrinfo(serv_ip.c_str(), NULL, NULL, &serv_addr) != 0)
      return SectorError::E_ADDR;
   char hostip[NI_MAXHOST];
   getnameinfo(serv_addr->ai_addr, serv_addr->ai_addrlen, hostip, sizeof(hostip), NULL, 0, NI_NUMERICHOST);
   m_strServerIP = hostip;
   freeaddrinfo(serv_addr);
   m_iServerPort = serv_port;

   string master_cert;
   if ((cert != NULL) && (0 != strlen(cert)))
      master_cert = cert;
   else if (retrieveMasterInfo(master_cert) < 0)
      return SectorError::E_CERTREFUSE;

   SSLTransport::init();

   int result;
   SSLTransport secconn;
   if ((result = secconn.initClientCTX(master_cert.c_str())) < 0)
      return result;
   if ((result = secconn.open(NULL, 0)) < 0)
      return result;

   if ((result = secconn.connect(m_strServerIP.c_str(), m_iServerPort)) < 0)
   {
      m_strServerIP = "";
      m_iServerPort = 0;
      return result;
   }

   CliLoginReq login_req;
   login_req.m_iType = 2;
   login_req.m_iVersion = SectorVersion;
   login_req.m_strUser = username;
   login_req.m_strPasswd = password;
   login_req.m_iKey = m_iKey;
   login_req.m_iPort = m_GMP.getPort();
   login_req.m_iDataPort = m_DataChn.getPort();
   memcpy(login_req.m_pcCryptoKey, m_pcCryptoKey, 16);
   memcpy(login_req.m_pcCryptoIV, m_pcCryptoIV, 8);

   login_req.serialize();
   secconn.sendmsg(login_req);

   CliLoginRes login_res;
   if (secconn.recvmsg(login_res) < 0)
   {
      return -1;
   }
   login_res.deserialize();

   m_iKey = login_res.m_iCliKey;
   // Login error.
   if (m_iKey < 0)
      return m_iKey;

   m_Topology = login_res.m_Topology;

   Address addr;
   addr.m_strIP = m_strServerIP;
   addr.m_iPort = m_iServerPort;
   m_Routing.insert(login_res.m_iRouterKey, addr);

   for (map<uint32_t, Address>::const_iterator i = login_res.m_mMasters.begin();
        i != login_res.m_mMasters.end(); ++ i)
   {
      if (m_Routing.insert(i->first, i->second) > 0)
      {
         // These masters have not been connected yet.
         RouterState state;
         state.m_bConnected = false;
         m_Routing.setState(i->first, state);
      }
   }

   secconn.close();
   SSLTransport::destroy();

   // Record these for future re-reconnect, if necessary
   // TODO: do not record password in clear text, may cause security issue.
   m_strUsername = username;
   m_strPassword = password;
   m_strCert = master_cert;

   m_Log << LogStart(LogLevel::LEVEL_1) << "Sector client successfully login to "
         << m_strServerIP << ":" << m_iServerPort
         << LogEnd();

   return m_iKey;
}

int Client::login(const string& serv_ip, const int& serv_port)
{
   Address addr;
   addr.m_strIP = serv_ip;
   addr.m_iPort = serv_port;

   // Check if the master exists.
   int rid = m_Routing.getRouterID(addr);
   if (rid < 0)
      return 0;
   RouterState state;
   m_Routing.getState(rid, state);
   if (state.m_bConnected)
      return 0;

   // Cannot re-connect if the client has not connected to a master before.
   if (m_iKey < 0)
      return -1;

   SSLTransport::init();

   int result = -1;
   SSLTransport secconn;
   if ((result = secconn.initClientCTX(m_strCert.c_str())) < 0)
      return result;
   if ((result = secconn.open(NULL, 0)) < 0)
      return result;
   if ((result = secconn.connect(serv_ip.c_str(), serv_port)) < 0)
      return result;

   CliLoginReq login_req;
   login_req.m_iType = 2;
   login_req.m_iVersion = SectorVersion;
   login_req.m_strUser = m_strUsername;
   login_req.m_strPasswd = m_strPassword;
   login_req.m_iKey = m_iKey;
   login_req.m_iPort = m_GMP.getPort();
   login_req.m_iDataPort = m_DataChn.getPort();
   memcpy(login_req.m_pcCryptoKey, m_pcCryptoKey, 16);
   memcpy(login_req.m_pcCryptoIV, m_pcCryptoIV, 8);

   login_req.serialize();
   secconn.sendmsg(login_req);

   CliLoginRes login_res;
   if (secconn.recvmsg(login_res) < 0)
   {
      return -1;
   }
   login_res.deserialize();   

   if (login_res.m_iKey < 0)
   {
      return -1;
   }

   secconn.close();
   SSLTransport::destroy();

   return 0;
}

int Client::logout()
{
   map<uint32_t, Address> masters;
   m_Routing.getListOfMasters(masters);

   for (map<uint32_t, Address>::iterator i = masters.begin(); i != masters.end(); ++ i)
   {
      SectorMsg msg;
      msg.setKey(m_iKey);
      msg.setType(2);
      msg.m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg);
   }

   m_iKey = 0;
   return 0;
}

int Client::close()
{
   if (-- m_iCount == 0)
   {
      m_KALock.acquire();
      m_bActive = false;
      m_KACond.signal();
      m_KALock.release();

#ifndef WIN32
      pthread_join(m_KeepAlive, NULL);
#else
      WaitForSingleObject(m_KeepAlive, INFINITE);
#endif

      m_strServerIP = "";
      m_iServerPort = 0;
      m_iKey = 0;
      m_GMP.close();
      UDTTransport::release();
   }

   return 0;
}

int Client::list(const string& path, vector<SNode>& attr)
{
   string revised_path = Metadata::revisePath(path);

   SectorMsg msg;
   msg.resize(65536);
   msg.setType(101);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   Address serv;
   if (lookup(revised_path, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   string filelist = msg.getData();

   attr.clear();

   unsigned int s = 0;
   while (s < filelist.length())
   {
      int t = filelist.find(';', s);
      SNode sn;
      sn.deserialize(filelist.substr(s, t - s).c_str());
      attr.insert(attr.end(), sn);
      s = t + 1;
   }

   return attr.size();
}

int Client::stat(const string& path, SNode& attr)
{
   string revised_path = Metadata::revisePath(path);

   SectorMsg msg;
   msg.resize(65536);
   msg.setType(102);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   Address serv;
   if (lookup(revised_path, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   attr.deserialize(msg.getData());

   // check local cache: updated files may not be sent to the master yet
   m_Cache.stat(path, attr);

   return 0;
}

int Client::mkdir(const string& path)
{
   string revised_path = Metadata::revisePath(path);

   SectorMsg msg;
   msg.setType(103);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   Address serv;
   if (lookup(revised_path, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::move(const string& oldpath, const string& newpath)
{
   string src = Metadata::revisePath(oldpath);
   string dst = Metadata::revisePath(newpath);

   SectorMsg msg;
   msg.setType(104);
   msg.setKey(m_iKey);

   int32_t size = src.length() + 1;
   msg.setData(0, (char*)&size, 4);
   msg.setData(4, src.c_str(), src.length() + 1);
   size = dst.length() + 1;
   msg.setData(4 + src.length() + 1, (char*)&size, 4);
   msg.setData(4 + src.length() + 1 + 4, dst.c_str(), dst.length() + 1);

   Address serv;
   if (lookup(src, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::remove(const string& path)
{
   string revised_path = Metadata::revisePath(path);

   SectorMsg msg;
   msg.setType(105);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);

   Address serv;
   if (lookup(revised_path, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::rmr(const string& path)
{
   SNode attr;
   int r = stat(path.c_str(), attr);
   if (r < 0)
      return r;

   if (attr.m_bIsDir)
   {
      vector<SNode> subdir;
      list(path, subdir);

      for (vector<SNode>::iterator i = subdir.begin(); i != subdir.end(); ++ i)
      {
         if (i->m_bIsDir)
            rmr(path + "/" + i->m_strName);
         else
            remove(path + "/" + i->m_strName);
      }
   }

   return remove(path);
}

int Client::copy(const string& src, const string& dst)
{
   string rsrc = Metadata::revisePath(src);
   string rdst = Metadata::revisePath(dst);

   SectorMsg msg;
   msg.setType(106);
   msg.setKey(m_iKey);

   int32_t size = rsrc.length() + 1;
   msg.setData(0, (char*)&size, 4);
   msg.setData(4, rsrc.c_str(), rsrc.length() + 1);
   size = rdst.length() + 1;
   msg.setData(4 + rsrc.length() + 1, (char*)&size, 4);
   msg.setData(4 + rsrc.length() + 1 + 4, rdst.c_str(), rdst.length() + 1);

   Address serv;
   if (lookup(rsrc, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::utime(const string& path, const int64_t& ts)
{
   string revised_path = Metadata::revisePath(path);

   SectorMsg msg;
   msg.setType(107);
   msg.setKey(m_iKey);
   msg.setData(0, revised_path.c_str(), revised_path.length() + 1);
   msg.setData(revised_path.length() + 1, (char*)&ts, 8);

   Address serv;
   if (lookup(revised_path, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::sysinfo(SysStat& sys)
{
   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(3);
   msg.m_iDataLength = SectorMsg::m_iHdrSize;

   Address serv;
   if (lookup(m_iKey, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   deserializeSysStat(sys, msg.getData(), msg.m_iDataLength);

   for (vector<SysStat::MasterStat>::iterator i = sys.m_vMasterList.begin(); i != sys.m_vMasterList.end(); ++ i)
   {
      if (i->m_strIP.length() == 0)
      {
         i->m_strIP = serv.m_strIP;
         break;
      }
   }

   return 0;
}

int Client::shutdown(const int& type, const string& param)
{
   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(8);

   int32_t t = type;
   if ((t < 0) || (t > 4))
      return SectorError::E_INVALID;

   msg.setData(0, (char*)&t, 4);
   int32_t size = param.length() + 1;
   msg.setData(4, (char*)&size, 4);
   msg.setData(8, param.c_str(), size);

   Address serv;
   if (lookup(m_iKey, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   if (type == 1)
   {
      // shutdown masters
      msg.setType(9);
      msg.m_iDataLength = SectorMsg::m_iHdrSize;
      if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
         return SectorError::E_CONNECTION;
   }

   return 0;
}

int Client::fsck(const string& /*path*/)
{
   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(9);

   Address serv;
   if (lookup(m_iKey, serv) < 0)
      return SectorError::E_MASTER;

   if (m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg) < 0)
      return SectorError::E_MASTER;

   if (msg.getType() < 0)
      return *(int32_t*)(msg.getData());

   return 0;
}

int Client::setMaxCacheSize(const int64_t ms)
{
   return m_Cache.setMaxCacheSize(ms);
}

int Client::updateMasters()
{
   SectorMsg msg;
   msg.setKey(m_iKey);

   map<uint32_t, Address> al;
   m_Routing.getListOfMasters(al);
   for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
   {
      msg.setType(5);

      if (m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg) >= 0)
      {
         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         uint32_t key = i->first;

         // reset routing information
         m_Routing.init();
         m_Routing.insert(key, addr);

         int n = *(int32_t*)msg.getData();
         int p = 4;
         for (int m = 0; m < n; ++ m)
         {
            key = *(int32_t*)(msg.getData() + p);
            p += 4;
            addr.m_strIP = msg.getData() + p;
            p += addr.m_strIP.length() + 1;
            addr.m_iPort = *(int32_t*)(msg.getData() + p);
            p += 4;

            m_Routing.insert(key, addr);
         }

         // masters updated, no need to query further
         return n + 1;
      }
   }

   return SectorError::E_MASTER;
}

#ifndef WIN32
void* Client::keepAlive(void* param)
#else
DWORD WINAPI Client::keepAlive(LPVOID param)
#endif
{
   Client* self = (Client*)param;
   int64_t last_heart_beat_time = CTimer::getTime();
   int64_t last_gc_time = CTimer::getTime();
   #ifdef APPLE
      srandomdev();
   #else
      srand(static_cast<int>(last_heart_beat_time));
   #endif

   while (self->m_bActive)
   {
      self->m_KALock.acquire();
      self->m_KACond.wait(self->m_KALock, 1000);
      self->m_KALock.release();

      if (!self->m_bActive)
         break;

      int64_t currtime = CTimer::getTime();

      //check if there is any write data that needs to be flushed, flush at least once per second
      CGuard::enterCS(self->m_IDLock);
      for (map<int, FSClient*>::iterator i = self->m_mFSList.begin(); i != self->m_mFSList.end(); ++ i)
      {
         if (currtime - i->second->m_llLastFlushTime > 1000000)
            i->second->flush();
      }
      CGuard::leaveCS(self->m_IDLock);


      // send a heart beat to masters every 60 - 120 seconds
      int offset = random() % 60;
      if (CTimer::getTime() - last_heart_beat_time < (60 + offset) * 1000000ULL)
         continue;

      // make a copy of the master addresses because the RPC can take some time to complete and block other calls
      map<uint32_t, Address> masters;
      self->m_Routing.getListOfMasters(masters);

      // TODO: optimize with multi_rpc
      for (map<uint32_t, Address>::iterator i = masters.begin(); i != masters.end(); ++ i)
      {
         // send keep-alive msg to each logged in master
         SectorMsg msg;
         msg.setKey(self->m_iKey);
         msg.setType(6);
         msg.m_iDataLength = SectorMsg::m_iHdrSize;
         if ((self->m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg) < 0) ||
             (msg.getType() < 0))
         {
            // if the master is down or restarted, remove it from the client
            self->m_Routing.remove(i->first);
         }
      }

      last_heart_beat_time = CTimer::getTime();

      // clean broken connections, every hour
      if (CTimer::getTime() - last_gc_time > 3600000000LL)
      {
         self->m_DataChn.garbageCollect();
         last_gc_time = CTimer::getTime();
      }
   }

#ifndef WIN32
   return NULL;
#else
   return 0;
#endif
}

int Client::deserializeSysStat(SysStat& sys, char* buf, int size)
{
   if (size < 52)
      return SectorError::E_INVALID;

   sys.m_llStartTime = *(int64_t*)buf;
   sys.m_llAvailDiskSpace = *(int64_t*)(buf + 8);
   sys.m_llTotalFileSize = *(int64_t*)(buf + 16);
   sys.m_llTotalFileNum = *(int64_t*)(buf + 24);
   sys.m_llUnderReplicated = *(int64_t*)(buf + 32);

   char* p = buf + 40;
   int c = *(int32_t*)p;
   sys.m_vCluster.resize(c);
   p += 4;
   for (vector<SysStat::ClusterStat>::iterator i = sys.m_vCluster.begin(); i != sys.m_vCluster.end(); ++ i)
   {
      i->m_iClusterID = *(int32_t*)p;
      i->m_iTotalNodes = *(int32_t*)(p + 4);
      i->m_llAvailDiskSpace = *(int64_t*)(p + 8);
      i->m_llTotalFileSize = *(int64_t*)(p + 16);
      i->m_llTotalInputData = *(int64_t*)(p + 24);
      i->m_llTotalOutputData = *(int64_t*)(p + 32);

      p += 40;
   }

   int m = *(int32_t*)p;
   p += 4;
   sys.m_vMasterList.resize(m);
   for (vector<SysStat::MasterStat>::iterator i = sys.m_vMasterList.begin(); i != sys.m_vMasterList.end(); ++ i)
   {
      i->m_iID = *(int32_t*)p;
      p += 4;
      i->m_strIP = p;
      p += 16;
      i->m_iPort = *(int32_t*)p;
      p += 4;
   }

   sys.m_llTotalSlaves = *(int32_t*)p;
   p += 4;
   sys.m_vSlaveList.resize(sys.m_llTotalSlaves);
   for (vector<SysStat::SlaveStat>::iterator i = sys.m_vSlaveList.begin(); i != sys.m_vSlaveList.end(); ++ i)
   {
      i->m_iID = *(int32_t*)p;
      i->m_strIP = p + 4;
      i->m_iPort = *(int32_t*)(p + 20);
      i->m_llAvailDiskSpace = *(int64_t*)(p + 24);
      i->m_llTotalFileSize = *(int64_t*)(p + 32);
      i->m_llCurrMemUsed = *(int64_t*)(p + 40);
      i->m_llCurrCPUUsed = *(int64_t*)(p + 48);
      i->m_llTotalInputData = *(int64_t*)(p + 56);
      i->m_llTotalOutputData = *(int64_t*)(p + 64);
      i->m_llTimeStamp = *(int64_t*)(p + 72);
      i->m_iStatus = *(int64_t*)(p + 80);
      i->m_iClusterID = *(int64_t*)(p + 84);
      i->m_strDataDir = p + 92;

      p += 92 + i->m_strDataDir.length() + 1;
   }

   return 0;
}

int Client::lookup(const string& path, Address& serv_addr)
{
   if (m_Routing.lookup(path, serv_addr) < 0)
      return SectorError::E_MASTER;

   if (login(serv_addr.m_strIP, serv_addr.m_iPort) < 0)
   {
      int result = updateMasters();
      if (result < 0)
         return result;

       m_Routing.lookup(path, serv_addr);
       return login(serv_addr.m_strIP, serv_addr.m_iPort);
   }

   return 0;
}

int Client::lookup(const int32_t& key, Address& serv_addr)
{
   if (m_Routing.lookup(key, serv_addr) < 0)
      return SectorError::E_MASTER;

   if (login(serv_addr.m_strIP, serv_addr.m_iPort) < 0)
   {
      int result = updateMasters();
      if (result < 0)
         return result;

       m_Routing.lookup(key, serv_addr);
       return login(serv_addr.m_strIP, serv_addr.m_iPort);
   }

   return 0;
}

int Client::retrieveMasterInfo(string& certfile)
{
   TCPTransport t;
   int port = 0;
   t.open(port);
   if (t.connect(m_strServerIP.c_str(), m_iServerPort - 1) < 0)
      return SectorError::E_CONNECTION;

   certfile = "";
#ifndef WIN32
   certfile = "/tmp/master_node.cert";
#else
   certfile = "master_node.cert";
#endif

   fstream ofs(certfile.c_str(), ios::out | ios::binary | ios::trunc);
   if (ofs.fail())
      return -1;

   int32_t size = 0;
   t.recv((char*)&size, 4);
   int64_t recvsize = t.recvfile(ofs, 0, size);
   t.close();

   ofs.close();

   if (recvsize <= 0)
      return SectorError::E_BROKENPIPE;

   return 0;
}

int Client::configLog(const char* log_path, bool screen, int level)
{
   if (log_path != NULL)
   {
      // Check if the dir exists and create it if necessary.
      SNode s;
      if ((LocalFS::stat(log_path, s) < 0) &&
          (LocalFS::mkdir(log_path) < 0))
      {
         return -1;
      }
      m_Log.init(log_path);
   }

   m_Log.copyScreen(screen);
   m_Log.setLevel(level);
   return 0;
}

#ifdef DEBUG
int Client::sendDebugCode(const int32_t& slave_id, const int32_t& code)
{
   Address serv;
   if (lookup(m_iKey, serv) < 0)
      return SectorError::E_CONNECTION;

   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(code);
   int32_t type = 0;
   msg.setData(0, (char*)&type, 4);
   msg.setData(4, (char*)&slave_id, 4);
   return m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg);
}

int Client::sendDebugCode(const string& slave_addr, const int32_t& code)
{
   Address serv;
   if (lookup(m_iKey, serv) < 0)
      return SectorError::E_CONNECTION;

   SectorMsg msg;
   msg.setKey(m_iKey);
   msg.setType(code);
   int32_t type = 1;
   msg.setData(0, (char*)&type, 4);
   string ip = slave_addr.substr(0, slave_addr.find(':'));
   int32_t port = atoi(slave_addr.substr(slave_addr.find(':') + 1, slave_addr.length() - ip.length() - 1).c_str());
   msg.setData(4, ip.c_str(), ip.length() + 1);
   msg.setData(68, (char*)&port, 4);

   return m_GMP.rpc(serv.m_strIP.c_str(), serv.m_iPort, &msg, &msg);
}
#endif
