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
   Yunhong Gu, last updated 03/30/2011
*****************************************************************************/

#include <iostream>
#include <signal.h>
#include <sstream>
#include <stack>

#include "common.h"
#include "master.h"
#include "ssltransport.h"
#include "tcptransport.h"
#include "topology.h"

using namespace std;
using namespace sector;

Master::Master():
m_pMetadata(NULL),
m_llLastUpdateTime(0),
m_pcTopoData(NULL),
m_iTopoDataSize(0),
m_pTopology(NULL)
{
   SSLTransport::init();
}

Master::~Master()
{
   m_SectorLog.close();
   delete m_pMetadata;
   delete [] m_pcTopoData;
   delete m_pTopology;

   SSLTransport::destroy();
}

int Master::init()
{
   if (ConfLocation::locate(m_strSectorHome) < 0)
   {
      cerr << "unable to read/parse configuration file.\n";
      return -1;
   }

   // read configuration from master.conf
   if (m_SysConfig.init(m_strSectorHome + "/conf/master.conf") < 0)
   {
      cerr << "unable to read/parse configuration file.\n";
      return -1;
   }

   SNode s;
   if (LocalFS::stat(m_strSectorHome + "/conf/topology.conf", s) < 0)
   {
      cerr << "Warning: no topology configuration found.\n";
   }

   m_pTopology = new Topology;
   m_pTopology->init((m_strSectorHome + "/conf/topology.conf").c_str());
   m_SlaveManager.init(m_pTopology);
   m_SlaveManager.setSlaveMinDiskSpace(m_SysConfig.m_llSlaveMinDiskSpace);
   m_SlaveManager.serializeTopo(m_pcTopoData, m_iTopoDataSize);

   // check local directories, create them if not exist
   m_strHomeDir = m_SysConfig.m_strHomeDir;

   m_SectorLog.init((m_strHomeDir + "/.log").c_str());
   m_SectorLog.setLevel(m_SysConfig.m_iLogLevel);

   if (LocalFS::stat(m_strHomeDir, s) < 0)
   {
      if (errno != ENOENT)
      {
         cerr << "unable to configure home directory.\n";
         return -1;
      }

      vector<string> dir;
      Metadata::parsePath(m_strHomeDir.c_str(), dir);

      string currpath = "/";
      for (vector<string>::iterator i = dir.begin(); i != dir.end(); ++ i)
      {
         currpath += *i;
         if (LocalFS::mkdir(currpath) < 0)
         {
            m_SectorLog.insert("unable to configure home directory.");
            return -1;
         }
         currpath += "/";
      }
   }

   LocalFS::mkdir(m_strHomeDir + ".metadata");
   LocalFS::mkdir(m_strHomeDir + ".tmp");
   LocalFS::mkdir(m_strHomeDir + ".log");

   if ((LocalFS::stat(m_strHomeDir + ".metadata", s) < 0)
      || (LocalFS::stat(m_strHomeDir + ".tmp", s) < 0)
      || (LocalFS::stat(m_strHomeDir + ".log", s) < 0))
   {
      cerr << "unable to create home directory " << m_strHomeDir << endl;
      return -1;
   }

   // currently only in-memory metadata is supported
   m_pMetadata = new Index;
   m_pMetadata->init(m_strHomeDir + ".metadata");

   // set and configure replication strategies
   m_pMetadata->setDefault(m_SysConfig.m_iReplicaNum, m_SysConfig.m_iReplicaDist);
   if (m_ReplicaConf.refresh(m_strSectorHome + "/conf/replica.conf"))
      m_pMetadata->refreshRepSetting("/", m_SysConfig.m_iReplicaNum, m_SysConfig.m_iReplicaDist, m_ReplicaConf.m_mReplicaNum, m_ReplicaConf.m_mReplicaDist, m_ReplicaConf.m_mRestrictedLoc);

   // load slave list and addresses
   loadSlaveStartInfo(m_strSectorHome + "/conf/slaves.list", m_sSlaveStartInfo);

   // add "slave" as a special user
   User* au = new User;
   au->m_strName = "system";
   au->m_iKey = 0;
   au->m_vstrReadList.insert(au->m_vstrReadList.begin(), "/");
   //au->m_vstrWriteList.insert(au->m_vstrWriteList.begin(), "/");
   m_UserManager.insert(au);

   // running...
   m_Status = RUNNING;

   // start GMP
   if (m_GMP.init(m_SysConfig.m_iServerPort) < 0)
   {
      cerr << "cannot initialize GMP.\n";
      return -1;
   }

   //connect security server to get ID
   SSLTransport secconn;
   if (secconn.initClientCTX((m_strSectorHome + "/conf/security_node.cert").c_str()) < 0)
   {
      cerr << "No security node certificate found.\n";
      return -1;
   }
   secconn.open(NULL, 0);
   if (secconn.connect(m_SysConfig.m_strSecServIP.c_str(), m_SysConfig.m_iSecServPort) < 0)
   {
      secconn.close();
      cerr << "Failed to find security server.\n";
      return -1;
   }

   int32_t cmd = 4;
   secconn.send((char*)&cmd, 4);
   secconn.recv((char*)&m_iRouterKey, 4);
   secconn.close();

   Address addr;
   addr.m_strIP = "";
   addr.m_iPort = m_SysConfig.m_iServerPort;
   m_Routing.insert(m_iRouterKey, addr);

   // start utility thread
#ifndef WIN32
   pthread_t utilserver;
   pthread_create(&utilserver, NULL, utility, this);
   pthread_detach(utilserver);

   // start service thread
   pthread_t svcserver;
   pthread_create(&svcserver, NULL, service, this);
   pthread_detach(svcserver);

   // start management/process thread
   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   // start replica thread
   pthread_t repserver;
   pthread_create(&repserver, NULL, replica, this);
   pthread_detach(repserver);
#else
    DWORD ThreadID = 0;
    HANDLE hThread = NULL;

    // start utility thread
    hThread = CreateThread(NULL, 0, utility, this, NULL, &ThreadID);
    if (hThread)
       CloseHandle(hThread);

    // start service thread
    hThread = CreateThread(NULL, 0, service, this, NULL, &ThreadID);
    if (hThread)
       CloseHandle(hThread);

    // start management/process thread
    hThread = CreateThread(NULL, 0, process, this, NULL, &ThreadID);
    if (hThread)
       CloseHandle(hThread);

    // start replica thread
    hThread = CreateThread(NULL, 0, replica, this, NULL, &ThreadID);
    if (hThread)
       CloseHandle(hThread);
#endif

   m_llStartTime = time(NULL);
   m_SectorLog.insert("Sector started.");

   return 1;
}

int Master::join(const char* ip, const int& port)
{
   // join the server
   string cert = m_strSectorHome + "/conf/master_node.cert";

   SSLTransport s;
   s.initClientCTX(cert.c_str());
   s.open(NULL, 0);
   if (s.connect(ip, port) < 0)
   {
      cerr << "unable to set up secure channel to the existing master.\n";
      return -1;
   }

   // send version number first
   s.send((char*)&SectorVersion, 4);

   // master join command type = 3;
   int cmd = 3;
   s.send((char*)&cmd, 4);
   int32_t key = -1;
   s.recv((char*)&key, 4);
   if (key < 0)
   {
      cerr << "security check failed. code: " << key << endl;
      return -1;
   }

   Address addr;
   addr.m_strIP = ip;
   addr.m_iPort = port;
   m_Routing.insert(key, addr);

   s.send((char*)&m_SysConfig.m_iServerPort, 4);
   s.send((char*)&m_iRouterKey, 4);

   // recv master list
   int num = 0;
   if (s.recv((char*)&num, 4) < 0)
      return -1;
   for (int i = 0; i < num; ++ i)
   {
      char ip[64];
      int port = 0;
      int id = 0;
      int size = 0;
      s.recv((char*)&id, 4);
      s.recv((char*)&size, 4);
      s.recv(ip, size);
      s.recv((char*)&port, 4);
      Address saddr;
      saddr.m_strIP = ip;
      saddr.m_iPort = port;
      m_Routing.insert(id, addr);
   }

   // recv slave list
   if (s.recv((char*)&num, 4) < 0)
      return -1;
   int size = 0;
   if (s.recv((char*)&size, 4) < 0)
      return -1;
   char* buf = new char [size];
   s.recv(buf, size);
   m_SlaveManager.deserializeSlaveList(num, buf, size);
   delete [] buf;

   // recv user list
   if (s.recv((char*)&num, 4) < 0)
      return -1;
   for (int i = 0; i < num; ++ i)
   {
      size = 0;
      s.recv((char*)&size, 4);
      char* ubuf = new char[size];
      s.recv(ubuf, size);
      User* u = new User;
      u->deserialize(ubuf, size);
      delete [] ubuf;
      m_UserManager.insert(u);
   }

   // recv metadata
   size = 0;
   s.recv((char*)&size, 4);
   string metafile = m_strHomeDir + ".tmp/master_meta.dat";
   s.recvfile(metafile.c_str(), 0, size);
   m_pMetadata->deserialize("/", metafile, NULL);
   LocalFS::erase(metafile);

   s.close();

   return 0;
}

int Master::run()
{
   while (m_Status == RUNNING)
   {
#ifndef WIN32
      sleep(60);
#else
      Sleep(60 * 1000);
#endif

      // check other masters
      vector<uint32_t> tbrm;

      map<uint32_t, Address> al;
      m_Routing.getListOfMasters(al);
      for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
      {
         if (i->first == m_iRouterKey)
            continue;

         SectorMsg msg;
         msg.setKey(0);
         msg.setType(1005); //master node probe msg
         msg.setData(0, (char*)&i->first, 4); // ask the other master to check its router ID, in case more than one are started on the same address
         if ((m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg) < 0) || (msg.getType() < 0))
         {
            m_SectorLog.insert(("Master lost " + i->second.m_strIP + ".").c_str());
            tbrm.push_back(i->first);

            // send the master drop info to all slaves
            SectorMsg msg;
            msg.setKey(0);
            msg.setType(1006);
            msg.setData(0, (char*)&i->first, 4);
            m_SlaveManager.updateSlaveList(m_vSlaveList, m_llLastUpdateTime);
            m_GMP.multi_rpc(m_vSlaveList, &msg);
         }
      }

      for (vector<uint32_t>::iterator i = tbrm.begin(); i != tbrm.end(); ++ i)
         m_Routing.remove(*i);


      // check each users, remove inactive ones
      vector<User*> iu;
      m_UserManager.checkInactiveUsers(iu, m_SysConfig.m_iClientTimeOut);
      for (vector<User*>::iterator i = iu.begin(); i != iu.end(); ++ i)
      {
         // release all transactions from this user
         vector<int> trans;
         m_TransManager.getUserTrans((*i)->m_iKey, trans);
         for (vector<int>::iterator t = trans.begin(); t != trans.end(); ++ t)
         {
            Transaction x;
            m_TransManager.retrieve(*t, x);
            for (set<int>::iterator s = x.m_siSlaveID.begin(); s != x.m_siSlaveID.end(); ++ s)
            {
               m_SlaveManager.decActTrans(*s);
               m_TransManager.updateSlave(*t, *s);
            }
         }

         m_SectorLog << LogStart(LogLevel::LEVEL_1) << "User " << (*i)->m_strName << " UID " << (*i)->m_iKey << " timeout. Kicked out." << LogEnd();
         delete *i;
      }
      iu.clear();


      if (m_Routing.getRouterID(m_iRouterKey) != 0)
         continue;

      // The following checks are only performed by the primary master


      // check each slave node
      // if probe fails, remove the metadata of the data on the node, and create new replicas

      map<int, SlaveNode> bad;
      map<int, SlaveNode> lost;
      map<int, SlaveNode> retry;
      map<int, SlaveNode> dead;
      m_SlaveManager.checkBadAndLost(bad, lost, retry, dead, m_SysConfig.m_iSlaveTimeOut * 1000000LL, m_SysConfig.m_iSlaveRetryTime * 1000000LL);

      for (map<int, SlaveNode>::iterator i = bad.begin(); i != bad.end(); ++ i)
      {
         m_SectorLog << LogStart(LogLevel::LEVEL_1) << " Bad slave detected " << i->second.m_strIP << " ID " << i->first << LogEnd();
         //TODO: create replica for files on the bad nodes, gradually move data out of those nodes
      }

      for (map<int, SlaveNode>::iterator i = lost.begin(); i != lost.end(); ++ i)
      {
         m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave lost " << i->second.m_strIP << " ID " << i->first << LogEnd();

         Address addr;
         addr.m_strIP = i->second.m_strIP;
         addr.m_iPort = i->second.m_iPort;
         removeSlave(i->first, addr);

         // if a slave is manually started, master may not know how to start it remotely
         if (i->second.m_strAddr.empty())
            continue;

         m_SectorLog.insert(("Restart slave " + i->second.m_strAddr + " " + i->second.m_strBase).c_str());

         SectorMsg newmsg;
         newmsg.setType(8);
         int msgid = 0;
         m_GMP.sendto(i->second.m_strIP, i->second.m_iPort, msgid, &newmsg);

         startSlave(i->second.m_strAddr, i->second.m_strBase, i->second.m_strOption);
      }

      for (map<int, SlaveNode>::iterator i = retry.begin(); i != retry.end(); ++ i)
      {
         SectorMsg newmsg;
         newmsg.setType(8);
         int msgid = 0;
         m_GMP.sendto(i->second.m_strIP, i->second.m_iPort, msgid, &newmsg);

         startSlave(i->second.m_strAddr, i->second.m_strBase, i->second.m_strOption);
      }

      for (map<int, SlaveNode>::iterator i = dead.begin(); i != dead.end(); ++ i)
      {
         m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave " << i->second.m_strIP << " ID " << i->first << " has been failed for a long time; Give it up now." << LogEnd();
         m_SlaveManager.remove(i->first);
      }

      // update cluster statistics
      m_SlaveManager.updateClusterStat();
   }

   return 0;
}

int Master::stop()
{
   m_Status = STOPPED;

   return 0;
}

#ifndef WIN32
   void* Master::utility(void* s)
#else
   DWORD WINAPI Master::utility(void* s)
#endif
{
   Master* self = (Master*)s;

   //the utility thread is used to allow clients to download certain information without login
   //such information may include the master certificate

   char* buf = new char[65536];
   ifstream ifs((self->m_strSectorHome + "/conf/master_node.cert").c_str());
   ifs.seekg(0, ios::end);
   int32_t size = ifs.tellg();
   ifs.seekg(0);
   ifs.read(buf, size);
   ifs.close();

#ifndef WIN32
   //ignore SIGPIPE
   sigset_t ps;
   sigemptyset(&ps);
   sigaddset(&ps, SIGPIPE);
   pthread_sigmask(SIG_BLOCK, &ps, NULL);
#endif

   TCPTransport util;
   int port = self->m_SysConfig.m_iServerPort - 1;
   util.open(port);
   util.listen();

   while (self->m_Status == RUNNING)
   {
      string ip;
      int port;
      TCPTransport* t = util.accept(ip, port);
      if (NULL == t)
         continue;

      t->send((char*)&size, 4);
      t->send(buf, size);
      t->close();
      delete t;
   }

   return NULL;
}

#ifndef WIN32
   void* Master::service(void* s)
#else
   DWORD WINAPI Master::service(void* s)
#endif
{
   Master* self = (Master*)s;

#ifndef WIN32
   //ignore SIGPIPE
   sigset_t ps;
   sigemptyset(&ps);
   sigaddset(&ps, SIGPIPE);
   pthread_sigmask(SIG_BLOCK, &ps, NULL);
#endif

   const int ServiceWorker = 4;
   for (int i = 0; i < ServiceWorker; ++ i)
   {
#ifndef WIN32
      pthread_t t;
      pthread_create(&t, NULL, serviceEx, self);
      pthread_detach(t);
#else
      DWORD ThreadID;
      HANDLE hThread = CreateThread(NULL, 0, serviceEx, self, NULL, &ThreadID);
      if (hThread)
         CloseHandle(hThread);
#endif
   }

   SSLTransport serv;
   if (serv.initServerCTX((self->m_strSectorHome + "/conf/master_node.cert").c_str(), (self->m_strSectorHome + "/conf/master_node.key").c_str()) < 0)
   {
      self->m_SectorLog.insert("WARNING: No master_node certificate or key found.");
      return NULL;
   }
   serv.open(NULL, self->m_SysConfig.m_iServerPort);
   serv.listen();

   while (self->m_Status == RUNNING)
   {
      char ip[64];
      int port;
      SSLTransport* s = serv.accept(ip, port);
      if (NULL == s)
         continue;

      ServiceJobParam* p = new ServiceJobParam;
      p->ip = ip;
      p->port = port;
      p->ssl = s;

      self->m_ServiceJobQueue.push(p);
   }

   return NULL;
}

#ifndef WIN32
   void* Master::serviceEx(void* param)
#else
   DWORD WINAPI Master::serviceEx(void* param)
#endif
{
   Master* self = (Master*)param;

   SSLTransport secconn;
   if (secconn.initClientCTX((self->m_strSectorHome + "/conf/security_node.cert").c_str()) < 0)
   {
      self->m_SectorLog.insert("No security node certificate found. All slave/client connection will be rejected.");
      return NULL;
   }

   while (self->m_Status == RUNNING)
   {
      ServiceJobParam* p = (ServiceJobParam*)self->m_ServiceJobQueue.pop();
      if (NULL == p)
         break;

      SSLTransport* s = p->ssl;
      string ip = p->ip;
      //int port = p->port;
      delete p;

      int32_t version;
      if (s->recv((char*)&version, 4) < 0)
      {
         s->close();
         delete s;
         continue;
      }

      if (version != SectorVersion)
      {
         // TODO: more advanced version check, currently they must be the same
         int32_t res = SectorError::E_VERSION;
         s->send((char*)&res, 4);
         s->close();
         delete s;
         continue;
      }

      int32_t cmd;
      if (s->recv((char*)&cmd, 4) < 0)
      {
         s->close();
         delete s;
         continue;
      }

      if (secconn.send((char*)&cmd, 4) < 0)
      {
         //if the permanent connection to the security server is broken, re-connect
         secconn.close();
         secconn.open(NULL, 0);
         if (secconn.connect(self->m_SysConfig.m_strSecServIP.c_str(), self->m_SysConfig.m_iSecServPort) < 0)
         {
            int32_t res = SectorError::E_NOSECSERV;
            s->send((char*)&res, 4);
            s->close();
            delete s;
            continue;
         }

         secconn.send((char*)&cmd, 4);
      }

      switch (cmd)
      {
      case 1: // slave node join
         self->processSlaveJoin(*s, secconn, ip);
         break;

      case 2: // user login
         self->processUserJoin(*s, secconn, ip);
         break;

      case 3: // master join
         self->processMasterJoin(*s, secconn, ip);
        break;
      }

      s->close();
      delete s;
   }

   secconn.close();

   return NULL;
}

int Master::processSlaveJoin(SSLTransport& slvconn,
                             SSLTransport& secconn, const string& ip)
{
   // recv local storage path, avoid same slave joining more than once
   int32_t size = 0;
   slvconn.recv((char*)&size, 4);
   string lspath = "";
   if (size > 0)
   {
      char* tmp = new char[size];
      slvconn.recv(tmp, size);
      lspath = Metadata::revisePath(tmp);
      delete [] tmp;
   }

   int32_t res = SectorError::E_SECURITY;
   char slaveIP[64];
   strcpy(slaveIP, ip.c_str());
   secconn.send(slaveIP, 64);
   secconn.recv((char*)&res, 4);

   if (lspath == "")
      res = SectorError::E_INVPARAM;
   else
   {
      int32_t id;
      Address addr;
      if (m_SlaveManager.checkDuplicateSlave(ip, lspath, id, addr))
      {
         // another slave is already using the storage
         // check if the current slave is still alive
         SectorMsg msg;
         msg.setType(1);
         if (m_GMP.rpc(addr.m_strIP, addr.m_iPort, &msg, &msg) >= 0)
            res = SectorError::E_REPSLAVE;
         else
         {
            removeSlave(id, addr);
            m_SlaveManager.remove(id);
         }
      }
   }

   slvconn.send((char*)&res, 4);

   if (res > 0)
   {
      SlaveNode sn;

      size = 0;
      slvconn.recv((char*)&size, 4);
      if (size > 0)
      {
         char* tmp = new char[size];
         slvconn.recv(tmp, size);
         sn.m_strBase = Metadata::revisePath(tmp);
         delete [] tmp;
      }

      sn.m_iNodeID = res;
      sn.m_strIP = ip;
      slvconn.recv((char*)&sn.m_iPort, 4);
      slvconn.recv((char*)&sn.m_iDataPort, 4);
      sn.m_strStoragePath = lspath;
      sn.m_llLastUpdateTime = CTimer::getTime();
      sn.m_llLastVoteTime = CTimer::getTime();

      slvconn.recv((char*)&(sn.m_llAvailDiskSpace), 8);

      int id;
      slvconn.recv((char*)&id, 4);
      if (id > 0)
         sn.m_iNodeID = id;

      stringstream tmp_meta_file;
      tmp_meta_file << m_strHomeDir << ".tmp/" << sn.m_iNodeID << ".dat";

      size = 0;
      slvconn.recv((char*)&size, 4);
      slvconn.recvfile(tmp_meta_file.str().c_str(), 0, size);

      Address addr;
      addr.m_strIP = ip;
      addr.m_iPort = sn.m_iPort;

      // accept existing data on the new slave and merge it with the master metadata
      Metadata* branch = NULL;
      branch = new Index;
      branch->init(tmp_meta_file.str());
      branch->deserialize("/", tmp_meta_file.str(), &addr);
      branch->refreshRepSetting("/", m_SysConfig.m_iReplicaNum, m_SysConfig.m_iReplicaDist, m_ReplicaConf.m_mReplicaNum, m_ReplicaConf.m_mReplicaDist, m_ReplicaConf.m_mRestrictedLoc);
      LocalFS::erase(tmp_meta_file.str());

      sn.m_llTotalFileSize = branch->getTotalDataSize("/");
      sn.m_llCurrMemUsed = 0;
      sn.m_llCurrCPUUsed = 0;
      sn.m_llTotalInputData = 0;
      sn.m_llTotalOutputData = 0;

      // locate slave start information, for automatic restart when necessary
      SlaveStartInfo ssi;
      ssi.m_strIP = sn.m_strIP;
      ssi.m_strStoragePath = sn.m_strStoragePath;
      ssi.m_strBase = sn.m_strBase;
      set<SlaveStartInfo, SSIComp>::iterator p = m_sSlaveStartInfo.find(ssi);
      if (p != m_sSlaveStartInfo.end())
      {
         sn.m_strAddr = p->m_strAddr;
         sn.m_strOption = p->m_strOption;
      }

      if (m_SlaveManager.insert(sn) < 0)
      {
         branch->clear();
         delete branch;
         m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave node " << ip << " join rejected." << LogEnd();
         return -1;
      }

      m_SlaveManager.updateClusterStat();

      // merge slave metadata with system metadata
      m_pMetadata->merge("/", branch, m_SysConfig.m_iReplicaNum);

      if (id < 0)
      {
         //this is the first master that the slave connect to; send these information to the slave

         // check number of files only, directories need not to be removed.
         size = branch->getTotalFileNum("/");
         if (size <= 0)
            slvconn.send((char*)&size, 4);
         else
         {
            string left_file = tmp_meta_file.str() + ".left";
            branch->serialize("/", left_file);
            SNode s;
            LocalFS::stat(left_file, s);
            size = s.m_llSize;
            slvconn.send((char*)&size, 4);
            if (size > 0)
               slvconn.sendfile(left_file.c_str(), 0, size);
            LocalFS::erase(left_file);

            m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave " << ip << " contains some files that are conflict with existing files." << LogEnd();
         }

         // send the list of masters to the new slave
         slvconn.send((char*)&m_iRouterKey, 4);
         int num = m_Routing.getNumOfMasters() - 1;
         slvconn.send((char*)&num, 4);
         map<uint32_t, Address> al;
         m_Routing.getListOfMasters(al);
         for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
         {
            if (i->first == m_iRouterKey)
               continue;

            slvconn.send((char*)&i->first, 4);
            size = i->second.m_strIP.length() + 1;
            slvconn.send((char*)&size, 4);
            slvconn.send(i->second.m_strIP.c_str(), size);
            slvconn.send((char*)&i->second.m_iPort, 4);
         }
      }

      branch->clear();
      delete branch;

      m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave node " << ip << ":" << sn.m_iPort << " joined." << LogEnd();
   }
   else
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Slave node " << ip << " join rejected." << LogEnd();
   }

   return 0;
}

int Master::processUserJoin(SSLTransport& cliconn,
                            SSLTransport& secconn, const string& ip)
{
   /* client uname, passwd and key */
   char user[64];
   cliconn.recv(user, 64);
   char password[128];
   cliconn.recv(password, 128);
   int32_t ukey;
   cliconn.recv((char*)&ukey, 4);

   /* forward to sec-server and get key from sec-server */
   secconn.send(user, 64);
   secconn.send(password, 128);
   char clientIP[64];
   strcpy(clientIP, ip.c_str());
   secconn.send(clientIP, 64);

   int32_t key = SectorError::E_SECURITY;
   secconn.recv((char*)&key, 4);

   if ((key > 0) && (ukey > 0))
      key = ukey;

   /* forward sec key to client */
   cliconn.send((char*)&key, 4);

   if (key > 0)
   {
      User* au = new User;
      au->m_strName = user;
      au->m_strIP = ip;
      au->m_iKey = key;
      au->m_llLastRefreshTime = CTimer::getTime();

      cliconn.recv((char*)&au->m_iPort, 4);
      cliconn.recv((char*)&au->m_iDataPort, 4);
      cliconn.recv((char*)au->m_pcKey, 16);
      cliconn.recv((char*)au->m_pcIV, 8);

      cliconn.send((char*)&m_iTopoDataSize, 4);
      if (m_iTopoDataSize > 0)
         cliconn.send(m_pcTopoData, m_iTopoDataSize);

      int32_t size = 0;
      char* buf = NULL;

      secconn.recv((char*)&size, 4);
      if (size > 0)
      {
         buf = new char[size];
         secconn.recv(buf, size);
         au->deserialize(au->m_vstrReadList, buf);
         delete [] buf;
      }

      secconn.recv((char*)&size, 4);
      if (size > 0)
      {
         buf = new char[size];
         secconn.recv(buf, size);
         au->deserialize(au->m_vstrWriteList, buf);
         delete [] buf;
      }

      int32_t exec;
      secconn.recv((char*)&exec, 4);
      au->m_bExec = exec;

      m_UserManager.insert(au);

      m_SectorLog << LogStart(LogLevel::LEVEL_1) << "User " << user << " ID " << key << " login from " << ip << LogEnd();

      if (ukey <= 0)
      {
         // send the list of masters to the new users
         cliconn.send((char*)&m_iRouterKey, 4);
         int num = m_Routing.getNumOfMasters() - 1;
         cliconn.send((char*)&num, 4);
         map<uint32_t, Address> al;
         m_Routing.getListOfMasters(al);
         for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
         {
            if (i->first == m_iRouterKey)
               continue;

            cliconn.send((char*)&i->first, 4);
            int size = i->second.m_strIP.length() + 1;
            cliconn.send((char*)&size, 4);
            cliconn.send(i->second.m_strIP.c_str(), size);
            cliconn.send((char*)&i->second.m_iPort, 4);
         }
      }

      // for synchronization only, message content is meaningless
      cliconn.send((char*)&key, 4);
   }
   else
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_1) << "User " << user << " login rejected from " << ip << LogEnd();
   }

   return 0;
}

int Master::processMasterJoin(SSLTransport& mstconn,
                              SSLTransport& secconn, const string& ip)
{
   char masterIP[64];
   strcpy(masterIP, ip.c_str());
   secconn.send(masterIP, 64);
   int32_t res = SectorError::E_SECURITY;
   secconn.recv((char*)&res, 4);

   if (res > 0)
      res = m_iRouterKey;
   mstconn.send((char*)&res, 4);

   if (res > 0)
   {
      int masterPort;
      int32_t key;
      mstconn.recv((char*)&masterPort, 4);
      mstconn.recv((char*)&key, 4);

      // send master list
      int num = m_Routing.getNumOfMasters() - 1;
      mstconn.send((char*)&num, 4);
      map<uint32_t, Address> al;
      m_Routing.getListOfMasters(al);
      for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
      {
         if (i->first == m_iRouterKey)
            continue;

         mstconn.send((char*)&i->first, 4);
         int size = i->second.m_strIP.length() + 1;
         mstconn.send((char*)&size, 4);
         mstconn.send(i->second.m_strIP.c_str(), size);
         mstconn.send((char*)&i->second.m_iPort, 4);
      }

      // send slave list
      char* buf = NULL;
      int32_t size = 0;
      num = m_SlaveManager.serializeSlaveList(buf, size);
      mstconn.send((char*)&num, 4);
      mstconn.send((char*)&size, 4);
      mstconn.send(buf, size);
      delete [] buf;

      // send user list
      num = 0;
      vector<char*> bufs;
      vector<int> sizes;
      m_UserManager.serializeUsers(num, bufs, sizes);
      mstconn.send((char*)&num, 4);
      for (int i = 0; i < num; ++ i)
      {
         mstconn.send((char*)&sizes[i], 4);
         mstconn.send(bufs[i], sizes[i]);
         delete [] bufs[i];
      }

      // send metadata
      string metafile = m_strHomeDir + ".tmp/master_meta.dat";
      m_pMetadata->serialize("/", metafile);

      SNode s;
      LocalFS::stat(metafile, s);
      size = s.m_llSize;
      mstconn.send((char*)&size, 4);
      mstconn.sendfile(metafile.c_str(), 0, size);
      LocalFS::erase(metafile);

      // send new master info to all existing masters
      for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
      {
         SectorMsg msg;
         msg.setKey(0);
         msg.setType(1001);
         msg.setData(0, (char*)&key, 4);
         msg.setData(4, masterIP, strlen(masterIP) + 1);
         msg.setData(68, (char*)&masterPort, 4);
         m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg);
      }

      Address addr;
      addr.m_strIP = masterIP;
      addr.m_iPort = masterPort;
      m_Routing.insert(key, addr);

      // send new master info to all slaves
      SectorMsg msg;
      msg.setKey(0);
      msg.setType(1001);
      msg.setData(0, (char*)&key, 4);
      msg.setData(4, masterIP, strlen(masterIP) + 1);
      msg.setData(68, (char*)&masterPort, 4);
      m_SlaveManager.updateSlaveList(m_vSlaveList, m_llLastUpdateTime);
      m_GMP.multi_rpc(m_vSlaveList, &msg);
   }

   return 0;
}

#ifndef WIN32
   void* Master::process(void* s)
#else
   DWORD WINAPI Master::process(void* s)
#endif
{
   Master* self = (Master*)s;

   const int ProcessWorker = 1;
   for (int i = 0; i < ProcessWorker; ++ i)
   {
#ifndef WIN32
      pthread_t t;
      pthread_create(&t, NULL, processEx, self);
      pthread_detach(t);
#else
      DWORD ThreadID;
      HANDLE hThread = CreateThread(NULL, 0, processEx, self, NULL, &ThreadID);
      if (hThread)
         CloseHandle(hThread);
#endif
   }

   while (self->m_Status == RUNNING)
   {
      string ip;
      int port;
      int32_t id;
      SectorMsg* msg = new SectorMsg;
      //msg->resize(65536);

      if (self->m_GMP.recvfrom(ip, port, id, msg) < 0)
         continue;

      int32_t key = msg->getKey();
      User* user = self->m_UserManager.lookup(key);
      if (NULL == user)
      {
         self->reject(ip, port, id, SectorError::E_EXPIRED);
         self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "Rejected Req from " << ip << " " << port << " " << id << " " << SectorError::E_EXPIRED << LogEnd();
         continue;
      }

      bool secure = false;

      if (key > 0)
      {
         if ((user->m_strIP == ip) && (user->m_iPort == port))
         {
            secure = true;
            user->m_llLastRefreshTime = CTimer::getTime();
         }
      }
      else if (key == 0)
      {
         Address addr;
         addr.m_strIP = ip;
         addr.m_iPort = port;
         if (self->m_SlaveManager.getSlaveID(addr) >= 0)
         {
            secure = true;
            self->m_SlaveManager.updateSlaveTS(addr);
         }
         else if (self->m_Routing.getRouterID(addr) >= 0)
            secure = true;
         else
         {
            //this may be a lost slave re-join the system (e.g., its network connection is down)
            // TODO: kill it and restart it
         }
      }

      if (!secure)
      {
         self->reject(ip, port, id, SectorError::E_SECURITY);
         self->m_SectorLog << LogStart(LogLevel::LEVEL_9) << "Rejected Req from " << user->m_strName << " " << user->m_iKey << " " << ip << " " << port << " " << id << " " << SectorError::E_SECURITY << LogEnd();
         continue;
      }

      ProcessJobParam* p = new ProcessJobParam;
      p->ip = ip;
      p->port = port;
      p->user = user;
      p->key = key;
      p->id = id;
      p->msg = msg;

      self->m_ProcessJobQueue.push(p);
   }

   return NULL;
}

#ifndef WIN32
   void* Master::processEx(void* param)
#else
   DWORD WINAPI Master::processEx(void* param)
#endif
{
   Master* self = (Master*)param;

   while (self->m_Status == RUNNING)
   {
      ProcessJobParam* p = (ProcessJobParam*)self->m_ProcessJobQueue.pop();
      if (NULL == p)
         break;

      switch (p->msg->getType() / 100)
      {
      case 0:
         self->processSysCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      case 1:
         self->processFSCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      case 2:
         self->processDCCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      case 3:
         self->processDBCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      case 10:
         self->processMCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      case 11:
         self->processSyncCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;

      #ifdef DEBUG
      case 99:
         self->processDebugCmd(p->ip, p->port, p->user, p->key, p->id, p->msg);
         break;
      #endif

      default:
         self->reject(p->ip, p->port, p->id, SectorError::E_UNKNOWN);
      }

      delete p->msg;
      delete p;
   }

   return NULL;
}

int Master::processSysCmd(const string& ip, const int port, const User* user, const int32_t key, int id, SectorMsg* msg)
{
   // internal system commands

   switch (msg->getType())
   {
   case 1: // slave reports transaction status and new files
   {
      int transid = *(int32_t*)msg->getData();
      int slaveid = *(int32_t*)(msg->getData() + 4);

      Transaction t;
      if (m_TransManager.retrieve(transid, t) < 0)
      {
         m_GMP.sendto(ip, port, id, msg);
         break;
      }

      int32_t change = *(int32_t*)(msg->getData() + 8);
      Address addr;
      addr.m_strIP = ip;
      addr.m_iPort = port;

      int num = *(int32_t*)(msg->getData() + 12);
      int pos = 16;
      for (int i = 0; i < num; ++ i)
      {
         int size = *(int32_t*)(msg->getData() + pos);
         string fileinfo = msg->getData() + pos + 4;
         pos += size + 4;

         m_SectorLog << LogStart(LogLevel::LEVEL_9) << "File update " << change << " " << fileinfo << LogEnd();

         // restore file information
         SNode sn;
         sn.deserialize(fileinfo.c_str());
         sn.m_sLocation.clear();
         sn.m_sLocation.insert(addr);

         //TODO: add log here

         if (change == FileChangeType::FILE_UPDATE_WRITE)
         {
            // because there are multiple replicas, wait until all replicas are updated
            m_TransManager.addWriteResult(transid, slaveid, fileinfo);

            // update the transaction data for write results
            m_TransManager.retrieve(transid, t);
         }
         else if (change == FileChangeType::FILE_UPDATE_NEW)
         {
            sn.m_iReplicaNum = m_ReplicaConf.getReplicaNum(sn.m_strName, m_SysConfig.m_iReplicaNum);
            sn.m_iReplicaDist = m_ReplicaConf.getReplicaDist(sn.m_strName, m_SysConfig.m_iReplicaDist);
            m_ReplicaConf.getRestrictedLoc(sn.m_strName, sn.m_viRestrictedLoc);
            m_pMetadata->create(sn);
         }
         else if (change == FileChangeType::FILE_UPDATE_REPLICA)
         {
            m_pMetadata->addReplica(sn.m_strName, sn.m_llTimeStamp, sn.m_llSize, addr);
            m_ReplicaLock.acquire();
            m_sstrOnReplicate.erase(Metadata::revisePath(sn.m_strName));
            m_ReplicaLock.release();
         }
      }

      if (num > 0)
      {
         // send file changes to all other masters
         if (m_Routing.getNumOfMasters() > 1)
         {
            SectorMsg newmsg;
            newmsg.setData(0, (char*)&change, 4);
            newmsg.setData(4, ip.c_str(), ip.length() + 1);
            newmsg.setData(68, (char*)&port, 4);
            newmsg.setData(72, msg->getData() + 12, msg->m_iDataLength - 12);
            sync(newmsg.getData(), newmsg.m_iDataLength, 1100);
         }
      }

      if ((t.m_iType == TransType::FILE) || (t.m_iType == TransType::REPLICA))
      {
         //TODO: slave should send another trans status report
         //leave this report dedicated to file status only

         // remove this slave from the transaction
         int r = m_TransManager.updateSlave(transid, slaveid);
         m_SlaveManager.decActTrans(slaveid);

         // unlock the file, if this is a file operation, and all slaves have completed
         // update transaction status, if this is a file operation; if it is sphere, a final sphere report will be sent, see #4.
         if (r == 0)
         {
            processWriteResults(t.m_strFile, t.m_mResults);
            m_pMetadata->unlock(t.m_strFile.c_str(), t.m_iUserKey, t.m_iMode);
            logUserActivity(user, "close", t.m_strFile.c_str(), 0, NULL, LogLevel::LEVEL_9);
         }
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      //TODO: feedback failed files, so that slave will delete them
      //if (r < 0)
      //   msg->setType(-msg->getType());
      m_GMP.sendto(ip, port, id, msg);
      m_ReplicaLock.acquire();
      if (m_ReplicaMgmt.getTotalNum() > 0)
         m_ReplicaCond.signal();
      m_ReplicaLock.release();

      break;
   }

   case 2: // client logout
   {
      m_SectorLog << LogStart(LogLevel::LEVEL_1) << "User " << user->m_strName << " ID " << user->m_iKey << " logout from " << ip << LogEnd();

      m_UserManager.remove(key);

      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 3: // sysinfo
   {
      if (!m_Routing.match(key, m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         break;
      }

      char* buf = NULL;
      int size = 0;
      serializeSysStat(buf, size);
      msg->setData(0, buf, size);
      delete [] buf;
      m_GMP.sendto(ip, port, id, msg);

      if (user->m_strName == "root")
      {
         //TODO: send current users, current transactions
      }

      logUserActivity(user, "sysinfo", NULL, 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 4: // sphere status & performance report
   {
      int transid = *(int32_t*)msg->getData();
      int slaveid = *(int32_t*)(msg->getData() + 4);

      Transaction t;
      if ((m_TransManager.retrieve(transid, t) < 0) || (t.m_iType != TransType::SPHERE))
      {
         m_GMP.sendto(ip, port, id, msg);
         break;
      }

      // the slave votes slow slaves
      int num = *(int*)(msg->getData() + 8);
      Address voter;
      voter.m_strIP = ip;
      voter.m_iPort = port;
      m_SlaveManager.voteBadSlaves(voter, num, msg->getData() + 12);

      m_TransManager.updateSlave(transid, slaveid);
      m_SlaveManager.decActTrans(slaveid);

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 5: //update master lists
   {
      //TODO: only return a list when the masters have changed

      int num = m_Routing.getNumOfMasters() - 1;
      msg->setData(0, (char*)&num, 4);
      int p = 4;
      map<uint32_t, Address> al;
      m_Routing.getListOfMasters(al);
      for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
      {
         if (i->first == m_iRouterKey)
            continue;

         msg->setData(p, (char*)&i->first, 4);
         int size = i->second.m_strIP.length() + 1;
         msg->setData(p + 4, i->second.m_strIP.c_str(), size);
         msg->setData(p + size + 4, (char*)&i->second.m_iPort, 4);
         p += size + 8;
      }

      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 6: // client keep-alive messages
   {
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 7: // unregister in-memory objects
   {
      int num = *(int32_t*)(msg->getData() + 8);
      int pos = 12;
      for (int i = 0; i < num; ++ i)
      {
         int size = *(int32_t*)(msg->getData() + pos);
         string path = msg->getData() + pos + 4;
         pos += size + 4;

         m_pMetadata->remove(path.c_str());

         // erase this from all other masters
         sync(path.c_str(), path.length() + 1, 1105);
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 8: // request to remove a slave or a group of slaves
   {
      if (user->m_strName != "root")
      {
         reject(ip, port, id, SectorError::E_AUTHORITY);
         break;
      }

      int32_t type = *(int32_t*)msg->getData();
      int32_t size = *(int32_t*)(msg->getData() + 4);
      *(msg->getData() + 8 + size) = '\0';
      string param = msg->getData() + 8;

      map<int, Address> sl;

      if (type == 1)
      {
         // shutdown all nodes
         m_SlaveManager.getSlaveListByRack(sl, "");
      }
      else if (type == 2)
      {
         // shutdown a node according to its ID
         int32_t id = atoi(param.c_str());
         Address addr;
         if (m_SlaveManager.getSlaveAddr(id, addr) >= 0)
            sl[id] = addr;

         // TODO: check if this is a master ID, and shutdown the master if necessary
      }
      else if (type == 3)
      {
         // shutdown a node according to the IP:port
         Address addr;
         int pos = param.find(':');
         addr.m_strIP = param.substr(0, pos);
         addr.m_iPort = atoi(param.substr(pos + 1, param.length() - pos - 1).c_str());
         int id = m_SlaveManager.getSlaveID(addr);
         if (id >= 0)
            sl[id] = addr;
      }
      else if (type == 4)
      {
         // shutdown a rack
         m_SlaveManager.getSlaveListByRack(sl, param);
      }
      else
      {
         reject(ip, port, id, SectorError::E_AUTHORITY);
         break;
      }

      //TODO: check active transcations, if a node is running a job, put it into a shutdown queue
      for (map<int, Address>::iterator i = sl.begin(); i != sl.end(); ++ i)
      {
         SectorMsg newmsg;
         newmsg.setType(8);
         int msgid = 0;
         m_GMP.sendto(i->second.m_strIP, i->second.m_iPort, msgid, &newmsg);

         removeSlave(i->first, i->second);
         m_SlaveManager.remove(i->first);

         m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Shutdown Slave " <<i->second.m_strIP << " " << i->second.m_iPort << " by request from " << ip << LogEnd();
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 9: // request to shutdown all masters
   {
      if (user->m_strName != "root")
      {
         reject(ip, port, id, SectorError::E_AUTHORITY);
         break;
      }

      // shutdown other masters
      map<uint32_t, Address> al;
      m_Routing.getListOfMasters(al);
      SectorMsg master_msg;
      master_msg.setKey(0);
      master_msg.setType(1009);
      for (map<uint32_t, Address>::iterator m = al.begin(); m != al.end(); ++ m)
      {
         if (m->first == m_iRouterKey)
            continue;

         m_GMP.rpc(m->second.m_strIP.c_str(), m->second.m_iPort, &master_msg, &master_msg);
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      m_Status = STOPPED;

      break;
   }

   case 10: // slave report status
   {
      // TODO: this may be piggyback to other messages.

      Address addr;
      addr.m_strIP = ip;
      addr.m_iPort = port;
      m_SlaveManager.updateSlaveInfo(addr, msg->getData(), msg->m_iDataLength);

      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}

int Master::processFSCmd(const string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg)
{
   // 100+ storage system

   switch (msg->getType())
   {
   case 101: // ls
   {
      if (!m_Routing.match(msg->getData(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "ls", msg->getData(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      int rwx = SF_MODE::READ;
      string dir = Metadata::revisePath(msg->getData());
      if (!user->match(dir, rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "ls", dir.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      int r = m_pMetadata->lookup(dir, attr);
      if (r < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "ls", dir.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      // !!list directory content only!!
      if (!attr.m_bIsDir)
      {
         reject(ip, port, id, SectorError::E_NOTDIR);
         logUserActivity(user, "ls", dir.c_str(), SectorError::E_NOTDIR, NULL, LogLevel::LEVEL_8);
         break;
      }

      vector<string> filelist;
      m_pMetadata->list(dir.c_str(), filelist);

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      int size = 0;
      for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         msg->setData(size, i->c_str(), i->length());
         size += i->length();
         msg->setData(size, ";", 1);
         size += 1;
      }
      msg->setData(size, "\0", 1);

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "ls", dir.c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 102: // stat
   {
      if (!m_Routing.match(msg->getData(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "stat", msg->getData(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      int rwx = SF_MODE::READ;
      string path = Metadata::revisePath(msg->getData());
      if (!user->match(path, rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "stat", path.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      int r = m_pMetadata->lookup(path, attr);
      if (r < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "stat", path.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      char* buf = NULL;
      attr.serialize(buf);
      msg->setData(0, buf, strlen(buf) + 1);
      delete [] buf;

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "stat", path.c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 103: // mkdir
   {
      if (!m_Routing.match(msg->getData(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "mkdir", msg->getData(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      int rwx = SF_MODE::WRITE;
      string path = Metadata::revisePath(msg->getData());
      if (!user->match(path, rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "mkdir", path.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      if (m_pMetadata->lookup(path, attr) >= 0)
      {
         // directory already exist
         reject(ip, port, id, SectorError::E_EXIST);
         logUserActivity(user, "mkdir", path.c_str(), SectorError::E_EXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      SF_OPT option;
      option.m_strHintIP = ip;

      set<int> empty;
      vector<SlaveNode> addr;
      if (m_SlaveManager.chooseIONode(empty, SF_MODE::WRITE, addr, option) <= 0)
      {
         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "mkdir", path.c_str(), SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
         break;
      }

      int msgid = 0;
      m_GMP.sendto(addr.begin()->m_strIP.c_str(), addr.begin()->m_iPort, msgid, msg);

      // create a new dir in metadata
      SNode sn;
      sn.m_strName = path;
      sn.m_bIsDir = true;
      m_pMetadata->create(sn);

      // send file changes to all other masters
      sync(path.c_str(), path.length() + 1, 1103);

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "mkdir", path.c_str(), 0, addr.begin()->m_strIP.c_str(), LogLevel::LEVEL_9);

      break;
   }

   case 104: // move a dir/file
   {
      string src = msg->getData() + 4;
      string dst = msg->getData() + 4 + src.length() + 1 + 4;

      src = Metadata::revisePath(src);
      dst = Metadata::revisePath(dst);

      string uplevel = dst.substr(0, dst.rfind('/') + 1);
      string sublevel = dst + src.substr(src.rfind('/'), src.length());

      if (!m_Routing.match(src.c_str(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "move", src.c_str(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode tmp;
      if ((uplevel.length() > 0) && (m_pMetadata->lookup(uplevel.c_str(), tmp) < 0))
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "move", dst.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }
      if (m_pMetadata->lookup(sublevel.c_str(), tmp) >= 0)
      {
         reject(ip, port, id, SectorError::E_EXIST);
         logUserActivity(user, "move", dst.c_str(), SectorError::E_EXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check user io permission, cannot move "/"
      int rwx = SF_MODE::READ | SF_MODE::WRITE;
      if ((src == "/") || (!user->match(src.c_str(), rwx)))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "move", src.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }
      rwx = SF_MODE::WRITE;
      if (!user->match(dst.c_str(), rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "move", dst.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode as, at;
      int rs = m_pMetadata->lookup(src.c_str(), as);
      int rt = m_pMetadata->lookup(dst.c_str(), at);
      set<Address, AddrComp> addrlist;
      m_pMetadata->lookup(src.c_str(), addrlist);

      if (rs < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "move", src.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }
      if ((rt >= 0) && (!at.m_bIsDir))
      {
         reject(ip, port, id, SectorError::E_EXIST);
         logUserActivity(user, "move", dst.c_str(), SectorError::E_EXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check if source file is busy
      // TODO: file should be moved individually, like rm, otherwise lock cannot be performed
      rwx = SF_MODE::WRITE;
      if (m_pMetadata->lock(src.c_str(), key, rwx) < 0)
      {
         reject(ip, port, id, SectorError::E_BUSY);
         logUserActivity(user, "move", src.c_str(), SectorError::E_BUSY, NULL, LogLevel::LEVEL_8);
         break;
      }
      m_pMetadata->unlock(src.c_str(), key, rwx);

      string newname = dst.substr(dst.rfind('/') + 1, dst.length());

      // move metadata and refresh the replica settings of the new file/dir
      if (rt < 0)
      {
         m_pMetadata->move(src.c_str(), uplevel.c_str(), newname.c_str());
         m_pMetadata->refreshRepSetting(uplevel + "/" + newname, m_SysConfig.m_iReplicaNum, m_SysConfig.m_iReplicaDist, m_ReplicaConf.m_mReplicaNum, m_ReplicaConf.m_mReplicaDist, m_ReplicaConf.m_mRestrictedLoc);
      }
      else
      {
         m_pMetadata->move(src.c_str(), dst.c_str());
         m_pMetadata->refreshRepSetting(dst, m_SysConfig.m_iReplicaNum, m_SysConfig.m_iReplicaDist, m_ReplicaConf.m_mReplicaNum, m_ReplicaConf.m_mReplicaDist, m_ReplicaConf.m_mRestrictedLoc);
      }

      msg->setData(0, src.c_str(), src.length() + 1);
      msg->setData(src.length() + 1, uplevel.c_str(), uplevel.length() + 1);
      msg->setData(src.length() + 1 + uplevel.length() + 1, newname.c_str(), newname.length() + 1);
      for (set<Address, AddrComp>::iterator i = addrlist.begin(); i != addrlist.end(); ++ i)
      {
         int msgid = 0;
         m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);
      }

      // send file changes to all other masters
      if (m_Routing.getNumOfMasters() > 1)
      {
         SectorMsg newmsg;
         newmsg.setData(0, (char*)&rt, 4);
         newmsg.setData(4, src.c_str(), src.length() + 1);
         int pos = 4 + src.length() + 1;
         if (rt < 0)
         {
            newmsg.setData(pos, uplevel.c_str(), uplevel.length() + 1);
            pos += uplevel.length() + 1;
            newmsg.setData(pos, newname.c_str(), newname.length() + 1);
         }
         else
            newmsg.setData(pos, dst.c_str(), dst.length() + 1);
         sync(newmsg.getData(), newmsg.m_iDataLength, 1104);
      }

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "move", (src + "->" + dst).c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 105: // delete dir/file
   {
      if (!m_Routing.match(msg->getData(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "delete", msg->getData(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      int rwx = SF_MODE::WRITE;
      string path = Metadata::revisePath(msg->getData());
      if ((path == "/") || !user->match(path, rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "delete", path.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      int n = m_pMetadata->lookup(path, attr);

      if (n < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "delete", path.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }
      else if (attr.m_bIsDir)
      {
         vector<string> fl;
         if (m_pMetadata->list(path, fl) > 0)
         {
            // directory not empty
            reject(ip, port, id, SectorError::E_NOEMPTY);
            logUserActivity(user, "delete", path.c_str(), SectorError::E_NOEMPTY, NULL, LogLevel::LEVEL_8);
            break;
         }
      }

      if (!attr.m_bIsDir)
      {
         for (set<Address, AddrComp>::iterator i = attr.m_sLocation.begin(); i != attr.m_sLocation.end(); ++ i)
         {
            int msgid = 0;
            m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);
         }
      }
      else
      {
         // For directories, the metadata doesn't record all of its locations.
         // So we send the cmd to all slaves.
         // TODO: this may cause problem when the number of slaves is big.
         m_SlaveManager.updateSlaveList(m_vSlaveList, m_llLastUpdateTime);
         for (vector<Address>::iterator i = m_vSlaveList.begin(); i != m_vSlaveList.end(); ++ i)
         {
            int msgid = 0;
            m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);
         }
      }

      m_pMetadata->remove(path.c_str(), true);

      // send file changes to all other masters
      sync(path.c_str(), path.length() + 1, 1105);

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "delete", path.c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 106: // make a copy of a file/dir
   {
      string src = Metadata::revisePath(msg->getData() + 4);
      string dst = Metadata::revisePath(msg->getData() + 4 + src.length() + 1 + 4);
      string uplevel = dst.substr(0, dst.find('/'));
      string sublevel = dst + src.substr(src.rfind('/'), src.length());

      if (!m_Routing.match(src.c_str(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "copy", src.c_str(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check source file/dir
      SNode as;
      if (m_pMetadata->lookup(src.c_str(), as) < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "copy", src.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check available disk space
      if (as.m_llSize > (int64_t)m_SlaveManager.getTotalDiskSpace())
      {
         reject(ip, port, id, SectorError::E_NODISK);
         logUserActivity(user, "copy", src.c_str(), SectorError::E_NODISK, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check source file read permission
      int rwx = SF_MODE::READ;
      if ((src == "/") || !user->match(src.c_str(), rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "copy", src.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      if (src == dst)
      {
         // sector_cp can be used to create replicas of a file/dir, if src == dst
         m_ReplicaLock.acquire();
         ReplicaJob job;
         job.m_strSource = src;
         job.m_strDest = dst;
         job.m_iPriority = COPY;
         m_ReplicaMgmt.insert(job);
         m_ReplicaCond.signal();
         m_ReplicaLock.release();

         m_GMP.sendto(ip, port, id, msg);
         break;
      }

      // check destination write permission
      rwx = SF_MODE::WRITE;
      if (!user->match(dst.c_str(), rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "copy", dst.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check if destination file already exists
      SNode tmp;
      int rt = m_pMetadata->lookup(dst.c_str(), tmp);
      if ((rt >= 0) && (!tmp.m_bIsDir))
      {
         reject(ip, port, id, SectorError::E_EXIST);
         logUserActivity(user, "copy", dst.c_str(), SectorError::E_EXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      if ((uplevel.length() > 0) && (m_pMetadata->lookup(uplevel.c_str(), tmp) < 0))
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "copy", dst.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      if (m_pMetadata->lookup(sublevel.c_str(), tmp) >= 0)
      {
         // destination file cannot exist, no overwite

         reject(ip, port, id, SectorError::E_EXIST);
         logUserActivity(user, "copy", dst.c_str(), SectorError::E_EXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      // respond client now becuase list_r() may take long time if the directory is big
      // copy() is asynchronous anyway
      m_GMP.sendto(ip, port, id, msg);

      // replace the directory prefix with dst
      string rep;
      if (rt < 0)
         rep = src;
      else
         rep = src.substr(0, src.rfind('/'));

      vector<string> filelist;
      m_pMetadata->list_r(src.c_str(), filelist);

      m_ReplicaLock.acquire();
      for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      {
         string target = *i;
         target.replace(0, rep.length(), dst);
         ReplicaJob job;
         job.m_strSource = *i;
         job.m_strDest = target;
         // cp has a higher priority than regular replication.
         job.m_iPriority = COPY;
         m_ReplicaMgmt.insert(job);
      }
      m_ReplicaCond.signal();
      m_ReplicaLock.release();

      logUserActivity(user, "copy", (src + "->" + dst).c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 107: // utime
   {
      if (!m_Routing.match(msg->getData(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "utime", msg->getData(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      int rwx = SF_MODE::WRITE;
      string path = Metadata::revisePath(msg->getData());
      if ((path == "/") || !user->match(path, rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "utime", path.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      if (m_pMetadata->lookup(path.c_str(), attr) < 0)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "utime", path.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      for (set<Address, AddrComp>::iterator i = attr.m_sLocation.begin(); i != attr.m_sLocation.end(); ++ i)
      {
         int msgid = 0;
         m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);
      }

      int64_t newts = *(int64_t*)(msg->getData() + strlen(msg->getData()) + 1);

      m_pMetadata->update(path, newts);

      // send file changes to all other masters
      if (m_Routing.getNumOfMasters() > 1)
      {
         SectorMsg newmsg;
         newmsg.setData(0, (char*)&newts, 8);
         newmsg.setData(8, path.c_str(), path.length() + 1);
         sync(newmsg.getData(), newmsg.m_iDataLength, 1107);
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize;
      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "utime", path.c_str(), 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 110: // open file
   {
      int32_t mode = *(int32_t*)(msg->getData());
      int32_t dataport = *(int32_t*)(msg->getData() + 4);
      int32_t name_len = *(int32_t*)(msg->getData() + 8);
      string path = Metadata::revisePath(msg->getData() + 12);
      int32_t opt_len = *(int32_t*)(msg->getData() + 12 + name_len);

      SF_OPT option;
      if (opt_len > 0)
         option.deserialize(msg->getData() + 12 + name_len + 4);
      if (option.m_llReservedSize < 0)
         option.m_llReservedSize = 0;
      if (option.m_strHintIP.c_str()[0] == '\0')
         option.m_strHintIP = ip;

      if (!m_Routing.match(path.c_str(), m_iRouterKey))
      {
         reject(ip, port, id, SectorError::E_ROUTING);
         logUserActivity(user, "open file", path.c_str(), SectorError::E_ROUTING, NULL, LogLevel::LEVEL_8);
         break;
      }

      // check user's permission on that file
      int rwx = mode;
      if (!user->match(path.c_str(), rwx))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "open file", path.c_str(), SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      int r = m_pMetadata->lookup(path.c_str(), attr);

      vector<SlaveNode> addr;

      if (r < 0)
      {
         // file does not exist
         if (!(mode & SF_MODE::WRITE))
         {
            reject(ip, port, id, SectorError::E_NOEXIST);
            logUserActivity(user, "open file", path.c_str(), SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
            break;
         }

         // otherwise, create a new file for write
         // choose a slave node for the new file
         set<Address, AddrComp> candidates;

         //if the current directory is nonsplit, the new file must be created on the same node
         for (int i = 0, n = path.length(); i < n; ++ i)
         {
            // if there is a ".nosplit" file in the path dir
            if (path.c_str()[i] == '/')
            {
               string updir = path.substr(0, i);
               if (m_pMetadata->lookup(updir + "/.nosplit", attr) >= 0)
               {
                  candidates = attr.m_sLocation;
                  break;
               }
            }
         }

         // create the new file in the metadata
         SNode sn;
         sn.m_strName = path;
         sn.m_bIsDir = false;
         sn.m_iReplicaNum = m_ReplicaConf.getReplicaNum(path, m_SysConfig.m_iReplicaNum);
         sn.m_iReplicaDist = m_ReplicaConf.getReplicaDist(path, m_SysConfig.m_iReplicaDist);
         m_ReplicaConf.getRestrictedLoc(path, sn.m_viRestrictedLoc);

         // client may choose to write to different number of replicas between 1 and max
         if (option.m_iReplicaNum > sn.m_iReplicaNum)
            option.m_iReplicaNum = sn.m_iReplicaNum;
         if (mode & SF_MODE::HiRELIABLE)
            option.m_iReplicaNum = sn.m_iReplicaNum;

         if (m_SlaveManager.chooseIONode(candidates, mode, addr, option, sn.m_iReplicaDist, &sn.m_viRestrictedLoc) <= 0)
         {
            reject(ip, port, id, SectorError::E_NODISK);
            logUserActivity(user, "open file", path.c_str(), SectorError::E_NODISK, NULL, LogLevel::LEVEL_8);
            break;
         }

         for (vector<SlaveNode>::iterator i = addr.begin(); i != addr.end(); ++ i)
         {
            Address a;
            a.m_strIP = i->m_strIP;
            a.m_iPort = i->m_iPort;
            sn.m_sLocation.insert(a);
         }

         m_pMetadata->create(sn);

         m_pMetadata->lock(path.c_str(), key, rwx);
      }
      else
      {
         if (attr.m_bIsDir)
         {
            // if this is a directory, cannot open it as a regular file
            reject(ip, port, id, SectorError::E_NOTFILE);
            logUserActivity(user, "open file", path.c_str(), SectorError::E_NOTFILE, NULL, LogLevel::LEVEL_8);
            break;
         }

         r = m_pMetadata->lock(path.c_str(), key, rwx);
         if (r < 0)
         {
            reject(ip, port, id, SectorError::E_BUSY);
            logUserActivity(user, "open file", path.c_str(), SectorError::E_BUSY, NULL, LogLevel::LEVEL_8);
            break;
         }

         m_SlaveManager.chooseIONode(attr.m_sLocation, mode, addr, option, attr.m_iReplicaDist, &attr.m_viRestrictedLoc);
      }

      int transid = m_TransManager.create(TransType::FILE, key, msg->getType(), path, mode);

      msg->setData(0, ip.c_str(), ip.length() + 1);
      msg->setData(64, (char*)&dataport, 4);
      msg->setData(136, (char*)&key, 4);
      msg->setData(140, (char*)&mode, 4);
      msg->setData(144, (char*)&transid, 4);
      msg->setData(148, (char*)user->m_pcKey, 16);
      msg->setData(164, (char*)user->m_pcIV, 8);
      msg->setData(172, path.c_str(), path.length() + 1);

      //TODO: optimize with multi_rpc
      for (vector<SlaveNode>::iterator i = addr.begin(); i != addr.end(); ++ i)
      {
         SectorMsg response;
         if (m_GMP.rpc(i->m_strIP.c_str(), i->m_iPort, msg, &response) >= 0)
         {
            if (response.getType() > 0)
            {
               m_TransManager.addSlave(transid, i->m_iNodeID);
               m_SlaveManager.incActTrans(i->m_iNodeID);
            }
            else
            {
               //TODO: roll back 
            }
         }
         else 
         {
            //TODO: remove this slave
         }
      }

      // send the connection information back to the client
      msg->setData(0, (char*)&transid, 4);
      msg->setData(4, (char*)&attr.m_llSize, 8);
      msg->setData(12, (char*)&attr.m_llTimeStamp, 8);

      // send all replica nodes address to the client
      int32_t addr_num = addr.size();
      msg->setData(20, (char*)&addr_num, 4);
      int offset = 24;
      for (vector<SlaveNode>::iterator i = addr.begin(); i != addr.end(); ++ i)
      {
         msg->setData(offset, i->m_strIP.c_str(), i->m_strIP.length() + 1);
         msg->setData(offset + 64, (char*)&i->m_iDataPort, 4);
         offset += 68;
      }
      msg->m_iDataLength = SectorMsg::m_iHdrSize + offset;

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "open", path.c_str(), 0, addr.rbegin()->m_strIP.c_str(), LogLevel::LEVEL_9);

      break;
   }

   case 112: // reopen a file, connect to a new slave
   {
      int32_t transid = *(int32_t*)msg->getData();
      int32_t dataport = *(int32_t*)(msg->getData() + 4);

      Transaction t;
      if ((m_TransManager.retrieve(transid, t) < 0) || (key != t.m_iUserKey))
      {
         reject(ip, port, id, SectorError::E_SECURITY);
         logUserActivity(user, "re-open", NULL, SectorError::E_SECURITY, NULL, LogLevel::LEVEL_8);
         break;
      }

      SNode attr;
      m_pMetadata->lookup(t.m_strFile.c_str(), attr);
      if (attr.m_sLocation.size() <= 1)
      {
         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "re-open", t.m_strFile.c_str(), SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
         break;
      }

      // choose from unused data locations only
      set<Address, AddrComp> candidates = attr.m_sLocation;
      for (set<int>::iterator i = t.m_siSlaveID.begin(); i != t.m_siSlaveID.end(); ++ i)
      {
         Address a;
         m_SlaveManager.getSlaveAddr(*i, a);
         candidates.erase(a);
      }

      // remove the last slave IDs, since they are not reponsive
      t.m_siSlaveID.clear();

      SF_OPT option;
      option.m_strHintIP = ip;

      vector<SlaveNode> addr;
      m_SlaveManager.chooseIONode(candidates, t.m_iMode, addr, option);
      if (addr.empty())
      {
         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "re-open", t.m_strFile.c_str(), SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
         break;
      }

      msg->setType(110);
      msg->setData(0, ip.c_str(), ip.length() + 1);
      msg->setData(64, (char*)&dataport, 4);
      msg->setData(136, (char*)&key, 4);
      msg->setData(140, (char*)&t.m_iMode, 4);
      msg->setData(144, (char*)&transid, 4);
      msg->setData(148, (char*)user->m_pcKey, 16);
      msg->setData(164, (char*)user->m_pcIV, 8);
      msg->setData(172, t.m_strFile.c_str(), t.m_strFile.length() + 1);

      SectorMsg response;
      if ((m_GMP.rpc(addr.begin()->m_strIP.c_str(), addr.begin()->m_iPort, msg, &response) < 0) || (response.getType() < 0))
      {
         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "re-open", t.m_strFile.c_str(), SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
      }

      m_TransManager.addSlave(transid, addr.begin()->m_iNodeID);
      m_SlaveManager.incActTrans(addr.begin()->m_iNodeID);

      // send the connection information back to the client
      msg->setType(112);
      msg->setData(0, addr.begin()->m_strIP.c_str(), addr.begin()->m_strIP.length() + 1);
      msg->setData(64, (char*)&(addr.begin()->m_iDataPort), 4);
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 68;

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "re-open", t.m_strFile.c_str(), SectorError::E_RESOURCE, addr.begin()->m_strIP.c_str(), LogLevel::LEVEL_9);

      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}

int Master::processDCCmd(const string& ip, const int port,  const User* user, const int32_t key, int id, SectorMsg* msg)
{
   // 200+ SPE

   switch (msg->getType())
   {
   case 201: // prepare SPE input information
   {
      if (!user->m_bExec)
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "request SPE", NULL, SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      vector<string> result;
      char* req = msg->getData();
      int32_t size = *(int32_t*)req;
      int offset = 0;
      bool notfound = false;
      while (size != -1)
      {
         int r = m_pMetadata->collectDataInfo(req + offset + 4, result);
         if (r < 0)
         {
            notfound = true;
            break;
         }

         offset += 4 + size;
         size = *(int32_t*)(req + offset);
      }

      if (notfound)
      {
         reject(ip, port, id, SectorError::E_NOEXIST);
         logUserActivity(user, "request SPE", NULL, SectorError::E_NOEXIST, NULL, LogLevel::LEVEL_8);
         break;
      }

      offset = 0;
      for (vector<string>::iterator i = result.begin(); i != result.end(); ++ i)
      {
         msg->setData(offset, i->c_str(), i->length() + 1);
         offset += i->length() + 1;
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize + offset;
      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "request SPE", NULL, 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 202: // locate SPEs
   {
      if (!user->m_bExec)
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "locate SPE", NULL, SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      //TODO: locate and pack SPE info in m_SlaveManager
      vector<SlaveNode> sl;
      Address client;
      client.m_strIP = ip;
      client.m_iPort = port;
      m_SlaveManager.chooseSPENodes(client, sl);

      int c = 0;
      for (vector<SlaveNode>::iterator i = sl.begin(); i != sl.end(); ++ i)
      {
         msg->setData(c * 72, i->m_strIP.c_str(), i->m_strIP.length() + 1);
         msg->setData(c * 72 + 64, (char*)&(i->m_iPort), 4);
         msg->setData(c * 72 + 68, (char*)&(i->m_iDataPort), 4);
         c ++;
      }

      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "locate SPE", NULL, 0, NULL, LogLevel::LEVEL_9);

      break;
   }

   case 203: // start spe
   {
      if (!user->m_bExec)
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "start SPE", NULL, SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      Address addr;
      addr.m_strIP = msg->getData();
      addr.m_iPort = *(int32_t*)(msg->getData() + 64);

      int transid = m_TransManager.create(TransType::SPHERE, key, msg->getType(), "", 0);
      int slaveid = m_SlaveManager.getSlaveID(addr);
      m_TransManager.addSlave(transid, slaveid);
      m_SlaveManager.incActTrans(slaveid);

      msg->setData(0, ip.c_str(), ip.length() + 1);
      msg->setData(64, (char*)&port, 4);
      msg->setData(68, (char*)&user->m_iDataPort, 4);
      msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);

      if ((m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg) < 0) || (msg->getType() < 0))
      {
         m_TransManager.updateSlave(transid, slaveid);
         m_SlaveManager.decActTrans(slaveid);

         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "start SPE", NULL, SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
         break;
      }

      msg->setData(0, (char*)&transid, 4);
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "start SPE", NULL, 0, addr.m_strIP.c_str(), LogLevel::LEVEL_9);

      break;
   }

   case 204: // start shuffler
   {
      string path = Metadata::revisePath(msg->getData() + 80);

      // check user sphere exec permission and output path write permission
      if (!user->m_bExec || !user->match(path.c_str(), SF_MODE::WRITE))
      {
         reject(ip, port, id, SectorError::E_PERMISSION);
         logUserActivity(user, "start Shuffler", NULL, SectorError::E_PERMISSION, NULL, LogLevel::LEVEL_8);
         break;
      }

      Address addr;
      addr.m_strIP = msg->getData();
      addr.m_iPort = *(int32_t*)(msg->getData() + 64);

      int transid = m_TransManager.create(TransType::SPHERE, key, msg->getType(), "", 0);
      m_TransManager.addSlave(transid, m_SlaveManager.getSlaveID(addr));
      m_SlaveManager.incActTrans(m_SlaveManager.getSlaveID(addr));

      msg->setData(0, ip.c_str(), ip.length() + 1);
      msg->setData(64, (char*)&port, 4);
      msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);
      msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&(user->m_iDataPort), 4);

      if ((m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg) < 0) || (msg->getType() < 0))
      {
         m_TransManager.updateSlave(transid, m_SlaveManager.getSlaveID(addr));
         m_SlaveManager.decActTrans(m_SlaveManager.getSlaveID(addr));

         reject(ip, port, id, SectorError::E_RESOURCE);
         logUserActivity(user, "start Shuffler", NULL, SectorError::E_RESOURCE, NULL, LogLevel::LEVEL_8);
         break;
      }

      msg->setData(4, (char*)&transid, 4);
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 8;
      m_GMP.sendto(ip, port, id, msg);

      logUserActivity(user, "start Shuffler", NULL, 0, addr.m_strIP.c_str(), LogLevel::LEVEL_9);

      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      logUserActivity(user, "unknown", NULL, SectorError::E_UNKNOWN, NULL, LogLevel::LEVEL_7);
      return -1;
   }

   return 0;
}

int Master::processDBCmd(const string& ip, const int port,  const User* /*user*/, const int32_t /*key*/, int id, SectorMsg* msg)
{
   // 300+ SpaceDB

   switch (msg->getType())
   {

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}

int Master::processMCmd(const string& ip, const int port,  const User* /*user*/, const int32_t /*key*/, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 1001: // new master
   {
      int32_t key = *(int32_t*)msg->getData();
      Address addr;
      addr.m_strIP = msg->getData() + 4;
      addr.m_iPort = *(int32_t*)(msg->getData() + 68);
      m_Routing.insert(key, addr);

      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 1005: // master probe
   {
      if (*(uint32_t*)msg->getData() != m_iRouterKey)
         msg->setType(-msg->getType());
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1007: // slave lost
   {
      int32_t sid = *(int32_t*)msg->getData();

      Address addr;
      if (m_SlaveManager.getSlaveAddr(sid, addr) >= 0)
         m_pMetadata->substract("/", addr);

      m_SlaveManager.remove(sid);

      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1009: // system shutdown
   {
      m_GMP.sendto(ip, port, id, msg);
      m_SectorLog.insert("System is shutdown.");
      m_Status = STOPPED;
      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}

#ifdef DEBUG
int Master::processDebugCmd(const string& ip, const int port,  const User* /*user*/, const int32_t /*key*/, int id, SectorMsg* msg)
{
   //99xx commands, for debug and testing purpose only

   switch (msg->getType())
   {
   case 9901:
   {
      int32_t type = *(int32_t*)msg->getData();
      Address addr;
      if (type == 0)
      {
         int32_t slave_id = *(int32_t*)(msg->getData() + 4);
         m_SlaveManager.getSlaveAddr(slave_id, addr);
      }
      else
      {
         addr.m_strIP = msg->getData() + 4;
         addr.m_iPort = *(int32_t*)(msg->getData() + 68);
      }

      int32_t msg_id = 0;
      m_GMP.sendto(addr.m_strIP, addr.m_iPort, msg_id, msg);

      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   case 9902:
   {
      int32_t type = *(int32_t*)msg->getData();
      Address addr;
      if (type == 0)
      {
         int32_t slave_id = *(int32_t*)(msg->getData() + 4);
         m_SlaveManager.getSlaveAddr(slave_id, addr);
      }
      else
      {
         addr.m_strIP = msg->getData() + 4;
         addr.m_iPort = *(int32_t*)(msg->getData() + 68);
      }

      int32_t msg_id = 0;
      m_GMP.sendto(addr.m_strIP, addr.m_iPort, msg_id, msg);

      m_GMP.sendto(ip, port, id, msg);

      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}
#endif

int Master::sync(const char* fileinfo, const int& size, const int& type)
{
   SectorMsg msg;
   msg.setKey(0);
   msg.setType(type);
   msg.setData(0, fileinfo, size);

   // send file changes to all other masters
   map<uint32_t, Address> al;
   m_Routing.getListOfMasters(al);
   for (map<uint32_t, Address>::iterator i = al.begin(); i != al.end(); ++ i)
   {
      if (i->first == m_iRouterKey)
         continue;

      m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg);
   }

   return 0;
}

int Master::processSyncCmd(const string& ip, const int port,  const User* /*user*/, const int32_t /*key*/, int id, SectorMsg* msg)
{
   switch (msg->getType())
   {
   case 1100: // file change
   {
      int change = *(int32_t*)msg->getData();
      Address addr;
      addr.m_strIP = msg->getData() + 4;
      addr.m_iPort = *(int32_t*)(msg->getData() + 68);

      int num = *(int32_t*)(msg->getData() + 72);
      int pos = 76;
      for (int i = 0; i < num; ++ i)
      {
         int size = *(int32_t*)(msg->getData() + pos);
         string fileinfo = msg->getData() + pos + 4;
         pos += size + 4;

         // restore file information
         SNode sn;
         sn.deserialize(fileinfo.c_str());
         sn.m_sLocation.insert(addr);

         if (change == FileChangeType::FILE_UPDATE_WRITE)
         {
            m_pMetadata->update(sn.m_strName, sn.m_llTimeStamp, sn.m_llSize);
         }
         else if (change == FileChangeType::FILE_UPDATE_NEW)
         {
             m_pMetadata->create(sn);
         }
         else if (change == FileChangeType::FILE_UPDATE_REPLICA)
         {
            m_pMetadata->addReplica(sn.m_strName, sn.m_llTimeStamp, sn.m_llSize, addr);
            m_ReplicaLock.acquire();
            m_sstrOnReplicate.erase(sn.m_strName);
            m_ReplicaLock.release();
         }
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1103: // mkdir
   {
      SNode sn;
      sn.m_strName = msg->getData();
      sn.m_bIsDir = true;
      m_pMetadata->create(sn);

      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1104: // mv
   {
      int rt = *(int32_t*)msg->getData();
      string src = msg->getData() + 4;

      if (rt < 0)
      {
         string uplevel = msg->getData() + 4 + src.length() + 1;
         string newname = msg->getData() + 4 + src.length() + 1 + uplevel.length() + 1;
         m_pMetadata->move(src.c_str(), uplevel.c_str(), newname.c_str());
      }
      else
      {
         string dst = msg->getData() + 4 + src.length() + 1;
         m_pMetadata->move(src.c_str(), dst.c_str());
      }

      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1105: // delete
   {
      m_pMetadata->remove(msg->getData(), true);

      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   case 1107: // utime
   {
      m_pMetadata->update(msg->getData() + 8, *(int64_t*)msg->getData());
      msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
      m_GMP.sendto(ip, port, id, msg);
      break;
   }

   default:
      reject(ip, port, id, SectorError::E_UNKNOWN);
      return -1;
   }

   return 0;
}

void Master::reject(const string& ip, const int port, int id, int32_t code)
{
   SectorMsg msg;
   msg.setType(-1);
   msg.setData(0, (char*)&code, 4);
   msg.m_iDataLength = SectorMsg::m_iHdrSize + 4;
   m_GMP.sendto(ip, port, id, &msg);
}

#ifndef WIN32
   void* Master::replica(void* s)
#else
   DWORD WINAPI Master::replica(void* s)
#endif
{
   Master* self = (Master*)s;

   // initially the master should wait for a while, because before all slaves join,
   // many files could be treated as undereplicated incorrectly.
   // we will wait for 10 minutes before the first check.
   sleep(10 * 60);

   int64_t last_replica_erase_time = CTimer::getTime();
   vector<string> under_replicated;
   vector<string> over_replicated;

   while (self->m_Status == RUNNING)
   {
      // wait for 600 seconds for the next check
      self->m_ReplicaLock.acquire();
      self->m_ReplicaCond.wait(self->m_ReplicaLock, 600*1000);
      self->m_ReplicaLock.release();

      // check replica, create or remove replicas if necessary
      if (self->m_ReplicaMgmt.getTotalNum() == 0)
      {
         // only the first master is responsible for replica checking
         if (self->m_Routing.getRouterID(self->m_iRouterKey) != 0)
            continue;

         // refresh special replication settings
         if (self->m_ReplicaConf.refresh(self->m_strSectorHome + "/conf/replica.conf"))
            self->m_pMetadata->refreshRepSetting("/", self->m_SysConfig.m_iReplicaNum, self->m_SysConfig.m_iReplicaDist, self->m_ReplicaConf.m_mReplicaNum, self->m_ReplicaConf.m_mReplicaDist, self->m_ReplicaConf.m_mRestrictedLoc);

         // TODO:: optimize this process: if there are too many files, this scan may kill the master
         under_replicated.clear();
         over_replicated.clear();
         self->m_pMetadata->checkReplica("/", under_replicated, over_replicated);

         if (!under_replicated.empty())
         {
            self->m_SectorLog << LogStart(LogLevel::LEVEL_1) << "Warning: found " << under_replicated.size() << " files that are under replicated." << LogEnd();

            self->m_ReplicaLock.acquire();
            for (vector<string>::iterator i = under_replicated.begin(); i != under_replicated.end(); ++ i)
            {
               ReplicaJob job;
               job.m_strSource = job.m_strDest = *i;
               job.m_iPriority = BACKGROUND;
               self->m_ReplicaMgmt.insert(job);
            }
            self->m_ReplicaLock.release();
         }

         // create replicas for files on slaves without enough disk space
         // so that some files can be removed from these nodes
         map<int64_t, Address> lowdisk;
         self->m_SlaveManager.checkStorageBalance(lowdisk);
         for (map<int64_t, Address>::iterator i = lowdisk.begin(); i != lowdisk.end(); ++ i)
         {
            self->m_SectorLog << LogStart(1) << "Warning: found slave " << i->second.m_strIP
                              << " with insufficient storage space." << LogEnd();

            vector<string> path;
            self->chooseDataToMove(path, i->second, i->first);
            self->m_ReplicaLock.acquire();
            for (vector<string>::iterator i = path.begin(); i != path.end(); ++ i)
            {
               ReplicaJob job;
               job.m_strSource = job.m_strDest = *i;
               job.m_bForceReplicate = true;
               self->m_ReplicaMgmt.insert(job);
            }
            self->m_ReplicaLock.release();
         }
      }

      // start any replication jobs in queue
      self->m_ReplicaLock.acquire();
      for (ReplicaMgmt::iterator i = self->m_ReplicaMgmt.begin(); i != self->m_ReplicaMgmt.end();)
      {
         // The number of concurrent replication in the system must be limited.	
         if (self->m_sstrOnReplicate.size() > self->m_SlaveManager.getNumberOfSlaves())
            break;

         if (self->createReplica(*i) >= 0)
         {
            ReplicaMgmt::iterator tmp = i;
            ++ i;
            self->m_ReplicaMgmt.erase(tmp);
         }
         else
         {
            ++ i;
         }
      }
      self->m_ReplicaLock.release();

      // over replication should be erased at a longer period, we use 1 hour 
      if (CTimer::getTime() - last_replica_erase_time < 3600*1000000LL)
         over_replicated.clear();
      else if (!over_replicated.empty())
         last_replica_erase_time = CTimer::getTime();

      // remove replicas from those over-replicated files
      // extra replicas can decrease write performance, and occupy disk spaces
      for (vector<string>::iterator i = over_replicated.begin(); i != over_replicated.end(); ++ i)
      {
         // choose one replica and remove it
         SNode attr;
         if (self->m_pMetadata->lookup(*i, attr) < 0)
            continue;

         // remove a directory, this must be a dir with .nosplit
         if ((attr.m_bIsDir) && (self->m_pMetadata->lookup(*i + "/.nosplit", attr) < 0))
            continue;

         Address addr;
         if (self->m_SlaveManager.chooseLessReplicaNode(attr.m_sLocation, addr) < 0)
            continue;

         self->removeReplica(*i, addr);
      }
   }

   return NULL;
}

int Master::createReplica(const ReplicaJob& job)
{
   SNode attr;
   if (m_pMetadata->lookup(job.m_strSource.c_str(), attr) < 0)
      return -1;

   SlaveNode sn;
   SNode sub_attr;
   if (attr.m_bIsDir && (job.m_strSource == job.m_strDest))
   {
      // Only nosplit dir can be replicated as a whole.
      if (m_pMetadata->lookup((job.m_strSource + "/.nosplit").c_str(), sub_attr) < 0)
         return -1;
   }

   if (job.m_strSource == job.m_strDest)
   {
      // do not create multiple replicas at the same time
      if (m_sstrOnReplicate.find(job.m_strSource) != m_sstrOnReplicate.end())
         return -1;

      if (!attr.m_bIsDir)
      {
         // do not over replicate
         if (attr.m_sLocation.size() >= (unsigned int)attr.m_iReplicaNum)
            return -1;
         if (m_SlaveManager.chooseReplicaNode(attr.m_sLocation, sn, attr.m_llSize, attr.m_iReplicaDist, &attr.m_viRestrictedLoc) < 0)
            return -1;
      }
      else
      {
         if (sub_attr.m_sLocation.size() >= (unsigned int)sub_attr.m_iReplicaNum)
            return -1;
         if (m_SlaveManager.chooseReplicaNode(sub_attr.m_sLocation, sn, attr.m_llSize, sub_attr.m_iReplicaDist, &sub_attr.m_viRestrictedLoc) < 0)
            return -1;
      }
   }
   else
   {
      set<Address, AddrComp> empty;
      int rd = m_ReplicaConf.getReplicaDist(job.m_strDest, m_SysConfig.m_iReplicaDist);
      vector<int> rl;
      m_ReplicaConf.getRestrictedLoc(job.m_strDest, rl);
      if (m_SlaveManager.chooseReplicaNode(empty, sn, attr.m_llSize, rd, &rl) < 0)
         return -1;
   }

   int transid = m_TransManager.create(TransType::REPLICA, 0, 111, job.m_strDest, 0);
   if (job.m_strSource == job.m_strDest)
      m_sstrOnReplicate.insert(job.m_strSource);

   int32_t dir = (attr.m_bIsDir) ? 1 : 0;

   SectorMsg msg;
   msg.setType(111);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&dir, 4);
   msg.setData(8, job.m_strSource.c_str(), job.m_strSource.length() + 1);
   msg.setData(8 + job.m_strSource.length() + 1, job.m_strDest.c_str(), job.m_strDest.length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
   {
      m_sstrOnReplicate.erase(job.m_strSource);
      return -1;
   }

   m_TransManager.addSlave(transid, sn.m_iNodeID);
   m_SlaveManager.incActTrans(sn.m_iNodeID);

   if (attr.m_bIsDir)
      return 0;

   // replicate index file to the same location
   string idx = job.m_strSource + ".idx";
   if (m_pMetadata->lookup(idx.c_str(), attr) < 0)
      return 0;

   transid = m_TransManager.create(TransType::REPLICA, 0, 111, job.m_strDest + ".idx", 0);
   if (job.m_strSource == job.m_strDest)
      m_sstrOnReplicate.insert(idx);

   msg.setType(111);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&dir, 4);
   msg.setData(8, idx.c_str(), idx.length() + 1);
   msg.setData(8 + idx.length() + 1, (job.m_strDest + ".idx").c_str(), (job.m_strDest + ".idx").length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
   {
      m_sstrOnReplicate.erase(idx);
      return 0;
   }

   m_TransManager.addSlave(transid, sn.m_iNodeID);
   m_SlaveManager.incActTrans(sn.m_iNodeID);

   return 0;
}

int Master::removeReplica(const std::string& filename, const Address& addr)
{
   SectorMsg msg;
   msg.setType(105);
   msg.setData(0, filename.c_str(), filename.length() + 1);

   int32_t id = 0;
   m_GMP.sendto(addr.m_strIP, addr.m_iPort, id, &msg);

   m_pMetadata->removeReplica(filename, addr);

   return 0;
}

int Master::serializeSysStat(char*& buf, int& size)
{
   char* cluster_info = NULL;
   int cluster_size = 0;
   m_SlaveManager.serializeClusterInfo(cluster_info, cluster_size);

   char* master_info = NULL;
   int master_size = 0;
   m_Routing.serializeMasterInfo(master_info, master_size);

   char* slave_info = NULL;
   int slave_size = 0;
   m_SlaveManager.serializeSlaveInfo(slave_info, slave_size);

   size = 40 + cluster_size + slave_size + master_size;
   buf = new char[size];

   *(int64_t*)buf = m_llStartTime;
   *(int64_t*)(buf + 8) = m_SlaveManager.getTotalDiskSpace();
   *(int64_t*)(buf + 16) = m_pMetadata->getTotalDataSize("/");
   *(int64_t*)(buf + 24) = m_pMetadata->getTotalFileNum("/");
   *(int64_t*)(buf + 32) = m_ReplicaMgmt.getTotalNum();

   char* p = buf + 40;
   memcpy(p, cluster_info, cluster_size);
   delete [] cluster_info;
   p += cluster_size;

   memcpy(p, master_info, master_size);
   delete [] master_info;
   p += master_size;

   memcpy(p, slave_info, slave_size);
   delete [] slave_info;

   return size;
}

int Master::removeSlave(const int& id, const Address& addr)
{
   // remove the data on that node
   m_pMetadata->substract("/", addr);

   //remove all associated transactions and release IO locks...
   vector<int> trans;
   m_TransManager.retrieve(id, trans);
   for (vector<int>::iterator i = trans.begin(); i != trans.end(); ++ i)
   {
      Transaction t;
      m_TransManager.retrieve(*i, t);

      int r = m_TransManager.updateSlave(*i, id);
      m_SlaveManager.decActTrans(id);

      // if this is the last slave released, unlock the file
      if ((t.m_iType == TransType::FILE) && (r == 0))
      {
         processWriteResults(t.m_strFile, t.m_mResults);
         m_pMetadata->unlock(t.m_strFile.c_str(), t.m_iUserKey, t.m_iMode);
      }
   }

   // send lost slave info to all existing masters
   map<uint32_t, Address> al;
   m_Routing.getListOfMasters(al);
   SectorMsg msg;
   msg.setKey(0);
   msg.setType(1007);
   msg.setData(0, (char*)&id, 4);

   for (map<uint32_t, Address>::iterator m = al.begin(); m != al.end(); ++ m)
   {
      if (m->first == m_iRouterKey)
         continue;

      m_GMP.rpc(m->second.m_strIP.c_str(), m->second.m_iPort, &msg, &msg);
   }

   return 0;
}

int Master::processWriteResults(const string& filename, map<int, string> results)
{
   // if no replica was changed, return now
   if (results.empty())
      return 0;

   int64_t timestamp = -1;
   int64_t size = -1;
   set<int> success;

   for (map<int, string>::iterator i = results.begin(); i != results.end(); ++ i)
   {
      SNode node;
      node.deserialize(i->second.c_str());

      // keep the latest copy
      if (node.m_llTimeStamp > timestamp)
      {
         timestamp = node.m_llTimeStamp;
         size = node.m_llSize;
      }
   }

   for (map<int, string>::iterator i = results.begin(); i != results.end(); ++ i)
   {
      SNode node;
      node.deserialize(i->second.c_str());

      if ((node.m_llTimeStamp == timestamp) && (node.m_llSize == size))
         success.insert(i->first);
   }

   //update file with new timestamp and size
   m_pMetadata->update(filename, timestamp, size);

   SNode attr;
   m_pMetadata->lookup(filename, attr);

   // all replicas are successfully updated
   if (attr.m_sLocation.size() == success.size())
      return 0;

   // remove those replicas with bad data
   for (set<Address, AddrComp>::iterator i = attr.m_sLocation.begin(); i != attr.m_sLocation.end(); ++ i)
   {
      if (success.find(m_SlaveManager.getSlaveID(*i)) == success.end())
         removeReplica(filename, *i);
   }

   return 0;
}

int Master::chooseDataToMove(vector<string>& path, const Address& addr, const int64_t& target_size)
{
   Metadata* branch;
   if (m_SysConfig.m_MetaType == MEMORY)
      branch = new Index;
   else
   {
      // not supported yet
      return 0;
   }

   // find all files on this particular slave
   m_pMetadata->getSlaveMeta(branch, addr);

   int64_t total_size = 0;
   queue<SNode> dataqueue;

   vector<string> datalist;
   branch->list("/", datalist);
   for (vector<string>::iterator i = datalist.begin(); i != datalist.end(); ++ i)
   {
      SNode sn;
      sn.deserialize(i->c_str());
      sn.m_strName = "/" + sn.m_strName;
      dataqueue.push(sn);
   }

   // add files to move until the total size reaches target_size
   while (!dataqueue.empty())
   {
      SNode node = dataqueue.front();
      dataqueue.pop();

      if (node.m_bIsDir)
      {
         branch->list(node.m_strName, datalist);
         for (vector<string>::iterator i = datalist.begin(); i != datalist.end(); ++ i)
         {
            SNode s;
            s.deserialize(i->c_str());
            s.m_strName = node.m_strName + "/" + s.m_strName;
            dataqueue.push(s);
         }
      }
      else
      {
         path.push_back(node.m_strName);
         total_size += node.m_llSize;
         if (total_size > target_size)
            break;
      }
   }

   delete branch;
   return 0;
}

void Master::startSlave(const std::string& addr, const std::string& base, const std::string& option, const std::string& log)
{
   //TODO: source .bash_profile on slave node to include more environments variables
   string slave_screen_log = "/dev/null";
   if (!log.empty())
      slave_screen_log = log;

   #ifndef WIN32
      string start_slave = base + "/slave/start_slave";
      string cmd = (string("ssh -o StrictHostKeychecking=no ") + addr + " \"" + start_slave + " " + base + " " + option + " &> " + slave_screen_log + "&\" &");
   #else
      string cmd = (string("ssh ") + addr + " \"" + "/bin/start_slave " + base + " " + option + " &> NULL &\"");
   #endif
   system(cmd.c_str());
}

void Master::logUserActivity(const User* user, const char* cmd, const char* file, const int res, const char* info, const int level)
{
   stringstream buf;
   buf << "user request => USER: " << user->m_strName << " UID: " << user->m_iKey << " IP: " << user->m_strIP << " CMD: " << cmd;
   if (NULL != file)
      buf << " PATH: " << file;
   buf << " RESULT: " << res;
   if (NULL != info)
      buf << " SLAVE: " << info;

   m_SectorLog.insert(buf.str().c_str(), level);
}

