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
   Yunhong Gu, last updated 07/05/2010
*****************************************************************************/

#ifndef WIN32
   #include <netdb.h>
#else
   #include <winsock2.h>
   #include <ws2tcpip.h>
#endif
#include <ssltransport.h>
#include <tcptransport.h>
#include <crypto.h>
#include <common.h>
#include "client.h"
#include <fsclient.h>
#include <dcclient.h>

using namespace std;

Client::Client():
m_strUsername(""),
m_strPassword(""),
m_strCert(""),
m_strServerHost(""),
m_strServerIP(""),
m_iKey(0),
m_bVerbose(false),
m_iCount(0),
m_bActive(false),
m_iID(0)
{
   CGuard::createMutex(m_MasterSetLock);
   CGuard::createMutex(m_KALock);
   CGuard::createCond(m_KACond);
   CGuard::createMutex(m_IDLock);
}

Client::~Client()
{
   CGuard::releaseMutex(m_MasterSetLock);
   CGuard::releaseMutex(m_KALock);
   CGuard::releaseCond(m_KACond);
   CGuard::releaseMutex(m_IDLock);
}

int Client::init(const string& server, const int& port)
{
   if (m_iCount ++ > 0)
      return 0;

#ifdef WIN32
    WSADATA wsaData = {0};
    int iResult = 0;
    iResult = WSAStartup(MAKEWORD(2, 2), &wsaData);
    if (iResult != 0) 
    {
        printf("WSAStartup failed: %d\n", iResult);
        return -1;
    }
#endif

   m_ErrorInfo.init();

   struct addrinfo* result;
   if (getaddrinfo(server.c_str(), NULL, NULL, &result) != 0)
      return SectorError::E_ADDR;

   m_strServerHost = server;

   char hostip[NI_MAXHOST];
   getnameinfo((sockaddr *)result->ai_addr, result->ai_addrlen, hostip, sizeof(hostip), NULL, 0, NI_NUMERICHOST);
   m_strServerIP = hostip;
   freeaddrinfo(result);

   m_iServerPort = port;

   Crypto::generateKey(m_pcCryptoKey, m_pcCryptoIV);

   UDTTransport::initialize();

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

   return 0;
}

int Client::login(const string& username, const string& password, const char* cert)
{
   if (m_iKey > 0)
      return m_iKey;

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

   if ((result = secconn.connect(m_strServerHost.c_str(), m_iServerPort)) < 0)
      return result;

   int cmd = 2;
   secconn.send((char*)&cmd, 4);

   // send username and password
   char buf[128];
   strncpy(buf, username.c_str(), 64);
   secconn.send(buf, 64);
   strncpy(buf, password.c_str(), 128);
   secconn.send(buf, 128);

   secconn.send((char*)&m_iKey, 4);
   secconn.recv((char*)&m_iKey, 4);
   if (m_iKey < 0)
      return SectorError::E_SECURITY;

   int32_t port = m_GMP.getPort();
   secconn.send((char*)&port, 4);
   port = m_DataChn.getPort();
   secconn.send((char*)&port, 4);

   // send encryption key/iv
   secconn.send((char*)m_pcCryptoKey, 16);
   secconn.send((char*)m_pcCryptoIV, 8);

   int size = 0;
   secconn.recv((char*)&size, 4);
   if (size > 0)
   {
      char* tmp = new char[size];
      secconn.recv(tmp, size);
      m_Topology.deserialize(tmp, size);
   }

   Address addr;
   int key = 0;
   secconn.recv((char*)&key, 4);
   addr.m_strIP = m_strServerIP;
   addr.m_iPort = m_iServerPort;
   m_Routing.insert(key, addr);

   CGuard::enterCS(m_MasterSetLock);
   m_sMasters.insert(addr);
   CGuard::leaveCS(m_MasterSetLock);

   int num;
   secconn.recv((char*)&num, 4);
   for (int i = 0; i < num; ++ i)
   {
      char ip[64];
      int size = 0;
      secconn.recv((char*)&key, 4);
      secconn.recv((char*)&size, 4);
      secconn.recv(ip, size);
      addr.m_strIP = ip;
      secconn.recv((char*)&addr.m_iPort, 4);
      m_Routing.insert(key, addr);
   }

   int32_t tmp;
   secconn.recv((char*)&tmp, 4);

   secconn.close();
   SSLTransport::destroy();

   m_strUsername = username;
   m_strPassword = password;
   m_strCert = master_cert;

   return m_iKey;
}

int Client::login(const string& serv_ip, const int& serv_port)
{
   Address addr;
   addr.m_strIP = serv_ip;
   addr.m_iPort = serv_port;

   CGuard::enterCS(m_MasterSetLock);
   if (m_sMasters.find(addr) != m_sMasters.end())
   {
      CGuard::leaveCS(m_MasterSetLock);
      return 0;
   }
   CGuard::leaveCS(m_MasterSetLock);

   if (m_iKey < 0)
      return -1;

   SSLTransport::init();

   int result;
   SSLTransport secconn;
   if ((result = secconn.initClientCTX(m_strCert.c_str())) < 0)
      return result;
   if ((result = secconn.open(NULL, 0)) < 0)
      return result;

   if ((result = secconn.connect(serv_ip.c_str(), serv_port)) < 0)
      return result;

   int cmd = 2;
   secconn.send((char*)&cmd, 4);

   // send username and password
   char buf[128];
   strncpy(buf, m_strUsername.c_str(), 64);
   secconn.send(buf, 64);
   strncpy(buf, m_strPassword.c_str(), 128);
   secconn.send(buf, 128);

   secconn.send((char*)&m_iKey, 4);
   int32_t key = -1;
   secconn.recv((char*)&key, 4);
   if (key < 0)
      return SectorError::E_SECURITY;

   int32_t port = m_GMP.getPort();
   secconn.send((char*)&port, 4);
   port = m_DataChn.getPort();
   secconn.send((char*)&port, 4);

   // send encryption key/iv
   secconn.send((char*)m_pcCryptoKey, 16);
   secconn.send((char*)m_pcCryptoIV, 8);

   int32_t tmp;
   secconn.recv((char*)&tmp, 4);

   secconn.close();
   SSLTransport::destroy();

   CGuard::enterCS(m_MasterSetLock);
   m_sMasters.insert(addr);
   CGuard::leaveCS(m_MasterSetLock);

   return 0;
}

int Client::logout()
{
   CGuard::enterCS(m_MasterSetLock);
   for (set<Address, AddrComp>::iterator i = m_sMasters.begin(); i != m_sMasters.end(); ++ i)
   {
      SectorMsg msg;
      msg.setKey(m_iKey);
      msg.setType(2);
      msg.m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.rpc(i->m_strIP.c_str(), i->m_iPort, &msg, &msg);
   }
   m_sMasters.clear();
   CGuard::leaveCS(m_MasterSetLock);

   m_iKey = 0;
   return 0;
}

int Client::close()
{
   if (-- m_iCount == 0)
   {
      if (m_iKey > 0)
         logout();

#ifndef WIN32
      pthread_mutex_lock(&m_KALock);
      m_bActive = false;
      pthread_cond_signal(&m_KACond);
      pthread_mutex_unlock(&m_KALock);
      pthread_join(m_KeepAlive, NULL);
#else
      m_bActive = false;
      SetEvent(m_KACond);
      WaitForSingleObject(m_KeepAlive, INFINITE);
#endif

      m_strServerHost = "";
      m_strServerIP = "";
      m_iServerPort = 0;
      m_GMP.close();
      UDTTransport::release();

#ifdef WIN32
      WSACleanup();
#endif
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

   int n = (msg.m_iDataLength - SectorMsg::m_iHdrSize - 128) / 68;
   char* al = msg.getData() + 128;

   for (int i = 0; i < n; ++ i)
   {
      Address addr;
      addr.m_strIP = al + 68 * i;
      addr.m_iPort = *(int32_t*)(al + 68 * i + 64);
      attr.m_sLocation.insert(addr);
   }

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

   while (self->m_bActive)
   {
#ifndef WIN32
      timeval t;
      gettimeofday(&t, NULL);
      timespec ts;
      ts.tv_sec  = t.tv_sec + 1;
      ts.tv_nsec = t.tv_usec * 1000;

      pthread_mutex_lock(&self->m_KALock);
      pthread_cond_timedwait(&self->m_KACond, &self->m_KALock, &ts);
      pthread_mutex_unlock(&self->m_KALock);
#else
      WaitForSingleObject(self->m_KACond, 1000);
#endif

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


      // send a heart beat to masters every 60 seconds
      if (CTimer::getTime() - last_heart_beat_time < 60000000)
         continue;

      vector<Address> ml;
      CGuard::enterCS(self->m_MasterSetLock);
      for (set<Address, AddrComp>::iterator i = self->m_sMasters.begin(); i != self->m_sMasters.end(); ++ i)
         ml.push_back(*i);
      CGuard::leaveCS(self->m_MasterSetLock);

      for (vector<Address>::iterator i = ml.begin(); i != ml.end(); ++ i)
      {
         // send keep-alive msg to each logged in master
         SectorMsg msg;
         msg.setKey(self->m_iKey);
         msg.setType(6);
         msg.m_iDataLength = SectorMsg::m_iHdrSize;
         self->m_GMP.rpc(i->m_strIP.c_str(), i->m_iPort, &msg, &msg);
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

   char* p = buf + 32;
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
   t.open(NULL, 0);
   if (t.connect(m_strServerIP.c_str(), m_iServerPort - 1) < 0)
      return SectorError::E_CONNECTION;

   certfile = "";
#ifndef WIN32
   certfile = "/tmp/master_node.cert";
#else
   certfile = "master_node.cert";
#endif

   int32_t size = 0;
   t.recv((char*)&size, 4);
   int64_t recvsize = t.recvfile(certfile.c_str(), 0, size);
   t.close();

   if (recvsize <= 0)
      return SectorError::E_BROKENPIPE;

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
