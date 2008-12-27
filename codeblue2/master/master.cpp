/*****************************************************************************
Copyright � 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 12/22/2008
*****************************************************************************/

#include <common.h>
#include <ssltransport.h>
#include "master.h"
#include <dirent.h>
#include <constant.h>
#include <iostream>
#include <stack>

using namespace std;


int ActiveUser::deserialize(vector<string>& dirs, const string& buf)
{
   unsigned int s = 0;
   while (s < buf.length())
   {
      unsigned int t = buf.find(';', s);

      if (buf.c_str()[s] == '/')
         dirs.insert(dirs.end(), buf.substr(s, t - s));
      else
         dirs.insert(dirs.end(), "/" + buf.substr(s, t - s));
      s = t + 1;
   }

   return dirs.size();
}

bool ActiveUser::match(const string& path, int32_t rwx)
{
   if ((rwx & 1) != 0)
   {
      for (vector<string>::iterator i = m_vstrReadList.begin(); i != m_vstrReadList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 1;
            break;
         }
      }
   }

   if ((rwx & 2) != 0)
   {
      for (vector<string>::iterator i = m_vstrWriteList.begin(); i != m_vstrWriteList.end(); ++ i)
      {
         if ((path.length() >= i->length()) && (path.substr(0, i->length()) == *i) && ((path.length() == i->length()) || (path.c_str()[i->length()] == '/') || (*i == "/")))
         {
            rwx ^= 2;
            break;
         }
      }
   }

   return (rwx == 0);
}


Master::Master()
{
   pthread_mutex_init(&m_MetaLock, NULL);
   pthread_mutex_init(&m_ReplicaLock, NULL);
   pthread_cond_init(&m_ReplicaCond, NULL);
}

Master::~Master()
{
   m_SectorLog.close();
   pthread_mutex_destroy(&m_MetaLock);
   pthread_mutex_destroy(&m_ReplicaLock);
   pthread_cond_destroy(&m_ReplicaCond);
}

int Master::init()
{
   m_SectorLog.init("sector.log");

   // read configuration from master.conf
   if (m_SysConfig.init("master.conf") < 0)
   {
      m_SectorLog.insert("unable to read/parse configuration file.");
      return -1;
   }

   if (m_SlaveManager.init("topology.conf") < 0)
   {
      m_SectorLog.insert("Warning: no topology configuration found.");
   }

   // check local directories, create them is not exist
   m_strHomeDir = m_SysConfig.m_strHomeDir;
   DIR* test = opendir(m_strHomeDir.c_str());
   if (NULL == test)
   {
      if (errno != ENOENT)
         return -1;

      vector<string> dir;
      Index::parsePath(m_strHomeDir.c_str(), dir);

      string currpath = "/";
      for (vector<string>::iterator i = dir.begin(); i != dir.end(); ++ i)
      {
         currpath += *i;
         if ((-1 == ::mkdir(currpath.c_str(), S_IRWXU)) && (errno != EEXIST))
            return -1;
         currpath += "/";
      }
   }
   closedir(test);
   test = opendir((m_strHomeDir + ".metadata").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".metadata").c_str(), S_IRWXU) < 0))
      {
         cerr << "unable to create home directory.\n";
         return -1;
      }
   }
   closedir(test);

   test = opendir((m_strHomeDir + ".sphere").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".sphere").c_str(), S_IRWXU) < 0))
      {
         cerr << "unable to create home directory.\n";
         return -1;
      }
   }
   closedir(test);


   // load slave list and addresses
   loadSlaveAddr("slaves.list");

   // add "slave" as a special user
   m_mActiveUser.clear();
   ActiveUser au;
   au.m_strName = "slave";
   au.m_iKey = 0;
   au.m_vstrReadList.insert(au.m_vstrReadList.begin(), "/");
   //au.m_vstrWriteList.insert(au.m_vstrWriteList.begin(), "/");
   m_mActiveUser[au.m_iKey] = au;

   // running...
   m_Status = RUNNING;

   Transport::initialize();

   // start GMP
   if (m_GMP.init(m_SysConfig.m_iServerPort) < 0)
   {
      cerr << "cannot initialize GMP.\n";
      return -1;
   }

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

   m_SysStat.m_llStartTime = time(NULL);
   m_SectorLog.insert("Sector started.");

   return 1;
}

int Master::run()
{
   while (m_Status == RUNNING)
   {
      sleep(60);

      // check each users, remove inactive ones
      vector<int> tbru;

      for (map<int, ActiveUser>::iterator i = m_mActiveUser.begin(); i != m_mActiveUser.end(); ++ i)
      {
         if (0 == i->first)
            continue;

         if (CTimer::getTime() - i->second.m_llLastRefreshTime > 30 * 60 * 1000000LL)
         {
            bool active = false;
            // check if there is any active transtions requested by the user
            for (map<int, Transaction>::iterator t = m_TransManager.m_mTransList.begin(); t != m_TransManager.m_mTransList.end(); ++ t)
            {
               if (t->second.m_iUserKey == i->second.m_iKey)
               {
                  active = true;
                  break;
               }
            }

            if (!active)
               tbru.insert(tbru.end(), i->first);
         }
      }

      // remove from active user list
      for (vector<int>::iterator i = tbru.begin(); i != tbru.end(); ++ i)
      {
         char* text = new char[64 + m_mActiveUser[*i].m_strName.length()];
         sprintf(text, "User %s timeout. Kicked out.", m_mActiveUser[*i].m_strName.c_str());
         m_SectorLog.insert(text);
         delete [] text;

         m_mActiveUser.erase(*i);
      }

      // check each slave node
      // if probe fails, remove the metadata about the data on the node, and create new replicas
      vector<int> tbrs;
      vector<SlaveAddr> tbsaddr;

      for (map<int, SlaveNode>::iterator i = m_SlaveManager.m_mSlaveList.begin(); i != m_SlaveManager.m_mSlaveList.end(); ++ i)
      {
         SectorMsg msg;
         msg.setType(1);

         if (m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg) > 0)
         {
            i->second.m_llLastUpdateTime = CTimer::getTime();
            i->second.m_llTimeStamp = *(int64_t*)(msg.getData() + 0);
            i->second.m_llAvailDiskSpace = *(int64_t*)(msg.getData() + 8);
            i->second.m_llTotalFileSize = *(int64_t*)(msg.getData() + 16);
            i->second.m_llCurrMemUsed = *(int64_t*)(msg.getData() + 24);
            i->second.m_llCurrCPUUsed = *(int64_t*)(msg.getData() + 32);
            i->second.m_llTotalInputData = *(int64_t*)(msg.getData() + 40);
            i->second.m_llTotalOutputData = *(int64_t*)(msg.getData() + 48);

            char* p = msg.getData() + 56;
            int n = *(int32_t*)p;
            p += 4;
            for (int j = 0; j < n; ++ j)
            {
               i->second.m_mSysIndInput[p] = *(int64_t*)(p + 16);
               p += 24;
            }
            n = *(int32_t*)p;
            p += 4;
            for (int j = 0; j < n; ++ j)
            {
               i->second.m_mSysIndOutput[p] = *(int64_t*)(p + 16);
               p += 24;
            }
            n = *(int32_t*)p;
            p += 4;
            for (int j = 0; j < n; ++ j)
            {
               i->second.m_mCliIndInput[p] = *(int64_t*)(p + 16);
               p += 24;
            }
            n = *(int32_t*)p;
            p += 4;
            for (int j = 0; j < n; ++ j)
            {
               i->second.m_mCliIndOutput[p] = *(int64_t*)(p + 16);
               p += 24;
            }

            i->second.m_iRetryNum = 0;

            if (i->second.m_llAvailDiskSpace < 10000000000LL)
            {
               if (i->second.m_iStatus == 1)
               {
                  char text[64];
                  sprintf(text, "Slave %s has less than 10GB available disk space left.", i->second.m_strIP.c_str());
                  m_SectorLog.insert(text);

                  i->second.m_iStatus = 2;
               }
            }
            else
            {
               if (i->second.m_iStatus == 2)
                  i->second.m_iStatus = 1;
            }
         }
         else if (++ i->second.m_iRetryNum > 10)
         {
            m_SectorLog.insert(("Detect slave drop " + i->second.m_strIP + ".").c_str());

            // to be removed
            tbrs.insert(tbrs.end(), i->first);

            // remove the data in that 
            Address addr;
            addr.m_strIP = i->second.m_strIP;
            addr.m_iPort = i->second.m_iPort;
            pthread_mutex_lock(&m_MetaLock);
            m_Metadata.substract(m_Metadata.m_mDirectory, addr);
            pthread_mutex_unlock(&m_MetaLock);

            // to be restarted
            map<string, SlaveAddr>::iterator sa = m_mSlaveAddrRec.find(i->second.m_strIP);
            if (sa != m_mSlaveAddrRec.end())
               tbsaddr.insert(tbsaddr.end(), sa->second);
         }
      }

      // remove from slave list
      for (vector<int>::iterator i = tbrs.begin(); i != tbrs.end(); ++ i)
         m_SlaveManager.remove(*i);

      // update cluster statistics
      m_SlaveManager.updateClusterStat(m_SlaveManager.m_Cluster);

      // restart dead slaves
      if (tbsaddr.size() > 0)
      {
         for (vector<SlaveAddr>::iterator i = tbsaddr.begin(); i != tbsaddr.end(); ++ i)
         {
            m_SectorLog.insert(("Restart slave " + i->m_strAddr + " " + i->m_strBase).c_str());

            // kill and restart the slave
            system((string("ssh ") + i->m_strAddr + " killall -9 start_slave").c_str());
            system((string("ssh ") + i->m_strAddr + " \"" + i->m_strBase + "/start_slave " + i->m_strBase + " &> /dev/null &\"").c_str());
         }

         // do not check replicas at this time because files on the restarted slave have not been counted yet
         continue;
      }

      // check replica, create or remove replicas if necessary
      pthread_mutex_lock(&m_ReplicaLock);
      if (m_vstrToBeReplicated.empty())
      {
         pthread_mutex_lock(&m_MetaLock);
         checkReplica(m_Metadata.m_mDirectory, "/", m_vstrToBeReplicated);
         pthread_mutex_unlock(&m_MetaLock);
      }
      if (!m_vstrToBeReplicated.empty())
         pthread_cond_signal(&m_ReplicaCond);
      pthread_mutex_unlock(&m_ReplicaLock);
   }

   return 1;
}

int Master::stop()
{
   m_Status = STOPPED;

   return 1;
}

void* Master::service(void* s)
{
   Master* self = (Master*)s;

   SSLTransport::init();
   SSLTransport serv;
   serv.initServerCTX("master_node.cert", "master_node.key");
   serv.open(NULL, self->m_SysConfig.m_iServerPort);
   serv.listen();

   while (self->m_Status == RUNNING)
   {
      char ip[64];
      int port;
      SSLTransport* s = serv.accept(ip, port);

      Param* p = new Param;
      p->ip = ip;
      p->port = port;
      p->self = self;
      p->ssl = s;

      pthread_t t;
      pthread_create(&t, NULL, serviceEx, p);
      pthread_detach(t);
   }

   SSLTransport::destroy();

   return NULL;
}

void* Master::serviceEx(void* p)
{
   Master* self = ((Param*)p)->self;
   SSLTransport* s = ((Param*)p)->ssl;
   string ip = ((Param*)p)->ip;
   //int port = ((Param*)p)->port;

   SSLTransport secconn;
   secconn.initClientCTX("security_node.cert");
   secconn.open(NULL, 0);
   int r = secconn.connect(self->m_SysConfig.m_strSecServIP.c_str(), self->m_SysConfig.m_iSecServPort);

   int32_t cmd;
   s->recv((char*)&cmd, 4);

   if (r < 0)
   {
      cmd = -cmd;
      s->send((char*)&cmd, 4);
      goto EXIT;
   }

   switch (cmd)
   {
      case 1: // slave node join
      {
         secconn.send((char*)&cmd, 4);
         secconn.send(ip.c_str(), 64);
         int32_t res = -1;
         secconn.recv((char*)&res, 4);

         s->send((char*)&res, 4);

         if (res == 1)
         {
            SlaveNode sn;
            sn.m_strIP = ip;
            s->recv((char*)&sn.m_iPort, 4);
            sn.m_llLastUpdateTime = CTimer::getTime();
            sn.m_iRetryNum = 0;

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = sn.m_iPort;

            int size = 0;
            s->recv((char*)&size, 4);

            char* buf = new char[size];
            s->recv(buf, size);

            ofstream meta((self->m_strHomeDir + ".metadata/" + ip).c_str());
            meta.write(buf, size);
            delete [] buf;
            meta.close();
            map<string, SNode> branch;
            ifstream ifs((self->m_strHomeDir + ".metadata/" + ip).c_str());
            Index::deserialize(ifs, branch, addr);
            ifs.close();
            pthread_mutex_lock(&self->m_MetaLock);
            Index::merge(self->m_Metadata.m_mDirectory, branch);
            pthread_mutex_unlock(&self->m_MetaLock);

            sn.m_llTotalFileSize = Index::getTotalDataSize(branch);
            s->recv((char*)&(sn.m_llAvailDiskSpace), 8);
            sn.m_llCurrMemUsed = 0;
            sn.m_llCurrCPUUsed = 0;
            sn.m_llTotalInputData = 0;
            sn.m_llTotalOutputData = 0;

            self->m_SlaveManager.insert(sn);
            self->m_SlaveManager.updateClusterStat(self->m_SlaveManager.m_Cluster);

            char text[64];
            sprintf(text, "Slave node %s joined.", ip.c_str());
            self->m_SectorLog.insert(text);
         }
         else
         {
            char text[64];
            sprintf(text, "Slave node %s join rejected.", ip.c_str());
            self->m_SectorLog.insert(text);
         }

         break;
      }

      case 2: // user login
      {
         char user[64];
         s->recv(user, 64);
         char password[128];
         s->recv(password, 128);

         secconn.send((char*)&cmd, 4);
         secconn.send(user, 64);
         secconn.send(password, 128);
         char clientIP[64];
         strcpy(clientIP, ip.c_str());
         secconn.send(clientIP, 64);

         int32_t key;
         secconn.recv((char*)&key, 4);

         if (key > 0)
         {
            ActiveUser au;
            au.m_strName = user;
            au.m_strIP = ip;
            s->recv((char*)&au.m_iPort, 4);
            au.m_iKey = key;
            au.m_llLastRefreshTime = CTimer::getTime();

            int32_t size = 0;
            char* buf = NULL;

            secconn.recv((char*)&size, 4);
            if (size > 0)
            {
               buf = new char[size];
               secconn.recv(buf, size);
               au.deserialize(au.m_vstrReadList, buf);
               delete [] buf;
            }

            secconn.recv((char*)&size, 4);
            if (size > 0)
            {
               buf = new char[size];
               secconn.recv(buf, size);
               au.deserialize(au.m_vstrWriteList, buf);
               delete [] buf;
            }

            int32_t exec;
            secconn.recv((char*)&exec, 4);
            au.m_bExec = exec;

            self->m_mActiveUser[au.m_iKey] = au;

            char text[128];
            sprintf(text, "User %s login from %s", user, ip.c_str());
            self->m_SectorLog.insert(text);
         }
         else
         {
            char text[128];
            sprintf(text, "User %s login rejected from %s", user, ip.c_str());
            self->m_SectorLog.insert(text);
         }

         s->send((char*)&key, 4);
      }

      default:
         break;
   }

EXIT:
   secconn.close();
   s->close();

   return NULL;
}

void* Master::process(void* s)
{
   Master* self = (Master*)s;

   char ip[64];
   int port;
   int32_t id;
   SectorMsg* msg = new SectorMsg;
   msg->resize(65536);

   while (self->m_Status == RUNNING)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      int32_t key = msg->getKey();
      map<int, ActiveUser>::iterator i = self->m_mActiveUser.find(key);
      if (i == self->m_mActiveUser.end())
      {
         self->reject(ip, port, id, SectorError::E_SECURITY);
         continue;
      }

      ActiveUser* user = &(i->second);

      if (key > 0)
      {
         if ((user->m_strIP != ip) || (user->m_iPort != port))
         {
            self->reject(ip, port, id, SectorError::E_SECURITY);
            continue;
         }

         user->m_llLastRefreshTime = CTimer::getTime();
      }
      else if (key == 0)
      {
         Address addr;
         addr.m_strIP = ip;
         addr.m_iPort = port;
         if (self->m_SlaveManager.m_mAddrList.end() == self->m_SlaveManager.m_mAddrList.find(addr))
         {
            self->reject(ip, port, id, SectorError::E_SECURITY);
            continue;
         }
      }
      else
      {
         self->reject(ip, port, id, SectorError::E_SECURITY);
         continue;
      }

      switch (msg->getType())
      {
         // internal system commands

         case 1: // slave reports transaction status
         {
            int transid = *(int32_t*)msg->getData();

            Transaction t;
            int find = self->m_TransManager.retrieve(transid, t);
            if ((find >= 0) && (t.m_iType == 1))
            {
               msg->m_iDataLength = SectorMsg::m_iHdrSize;
               self->m_GMP.sendto(ip, port, id, msg);
               self->m_TransManager.update(transid);
               break;
            }

            int change = *(int32_t*)(msg->getData() + 4);
            string path = msg->getData() + 8;

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = port;

            set<Address, AddrComp> tbr;
            pthread_mutex_lock(&self->m_MetaLock);
            self->m_Metadata.update(path.c_str(), addr, change, tbr);
            pthread_mutex_unlock(&self->m_MetaLock);

            for (set<Address, AddrComp>::iterator i = tbr.begin(); i != tbr.end(); ++ i)
            {
               SectorMsg dmsg;
               dmsg.setData(0, path.c_str(), path.length() + 1);
               int msgid = 0;
               self->m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, &dmsg);
            }

            if (change == 3)
            {
               SNode attr;
               attr.deserialize(path.c_str());
               self->m_sstrOnReplicate.erase(attr.m_strName);
            }

            //TODO: update used disk space of the slave node

            if (find >= 0)
            {
               pthread_mutex_lock(&self->m_MetaLock);
               self->m_Metadata.unlock(path.c_str(), t.m_iMode);
               pthread_mutex_unlock(&self->m_MetaLock);

               self->m_TransManager.update(transid);
            }

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);

            pthread_mutex_lock(&self->m_ReplicaLock);
            if (!self->m_vstrToBeReplicated.empty())
               pthread_cond_signal(&self->m_ReplicaCond);
            pthread_mutex_unlock(&self->m_ReplicaLock);

            break;
         }

         case 2: // client logout
         {
            char text[128];
            sprintf(text, "User %s logout from %s.", user->m_strName.c_str(), ip);
            self->m_SectorLog.insert(text);

            self->m_mActiveUser.erase(key);
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 3: // sysinfo
         {
            self->m_SysStat.m_llAvailDiskSpace = self->m_SlaveManager.getTotalDiskSpace();
            self->m_SysStat.m_llTotalSlaves = self->m_SlaveManager.getTotalSlaves();
            self->m_SysStat.m_llTotalFileSize = Index::getTotalDataSize(self->m_Metadata.m_mDirectory);
            self->m_SysStat.m_llTotalFileNum = Index::getTotalFileNum(self->m_Metadata.m_mDirectory);

            int size = SysStat::g_iSize + 8 + self->m_SlaveManager.m_Cluster.m_mSubCluster.size() * 48 + self->m_SlaveManager.m_mSlaveList.size() * 72;
            char* buf = new char[size];
            self->m_SysStat.serialize(buf, size, self->m_SlaveManager.m_mSlaveList, self->m_SlaveManager.m_Cluster);

            msg->setData(0, buf, size);
            delete [] buf;
            self->m_GMP.sendto(ip, port, id, msg);

            if (user->m_strName == "root")
            {
               //TODO: send current users, current transactions
            }

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "sysinfo", "", "SUCCESS", "");

            break;
         }

         // 100+ storage system

         case 101: // ls
         {
            int rwx = 1;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "ls", msg->getData(), "REJECT", "");
               break;
            }

            vector<string> filelist;
            pthread_mutex_lock(&self->m_MetaLock);
            self->m_Metadata.list(msg->getData(), filelist);
            pthread_mutex_unlock(&self->m_MetaLock);

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

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "ls", msg->getData(), "SUCCESS", "");

            break;
         }

         case 102: // stat
         {
            int rwx = 1;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }

            SNode attr;
            pthread_mutex_lock(&self->m_MetaLock);
            int r = self->m_Metadata.lookup(msg->getData(), attr);
            pthread_mutex_unlock(&self->m_MetaLock);
            if (r < 0)
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "stat", msg->getData(), "REJECT", "");
            }
            else
            {
               char buf[128];
               attr.serialize(buf);
               msg->setData(0, buf, strlen(buf) + 1);

               int c = 0;
               for (set<Address, AddrComp>::iterator i = attr.m_sLocation.begin(); i != attr.m_sLocation.end(); ++ i)
               {
                  msg->setData(128 + c * 68, i->m_strIP.c_str(), i->m_strIP.length() + 1);
                  msg->setData(128 + c * 68 + 64, (char*)&(i->m_iPort), 4);
                  ++ c;
               }

               self->m_GMP.sendto(ip, port, id, msg);

               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "stat", msg->getData(), "SUCCESS", "");
            }

            break;
         }

         case 103: // mkdir
         {
            int rwx = 2;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", msg->getData(), "REJECT", "");
               break;
            }

            SNode attr;
            if (self->m_Metadata.lookup(msg->getData(), attr) >= 0)
            {
               // directory already exist
               self->reject(ip, port, id, SectorError::E_EXIST);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", msg->getData(), "REJECT", "");
               break;
            }

            Address client;
            client.m_strIP = ip;
            client.m_iPort = port;
            set<int> empty;
            SlaveNode sn;
            if (self->m_SlaveManager.chooseIONode(empty, client, 2, sn) < 0)
            {
               self->reject(ip, port, id, SectorError::E_RESOURCE);
               break;
            }

            int msgid = 0;
            self->m_GMP.sendto(sn.m_strIP.c_str(), sn.m_iPort, msgid, msg);

            pthread_mutex_lock(&self->m_MetaLock);
            self->m_Metadata.create(msg->getData(), true);
            pthread_mutex_unlock(&self->m_MetaLock);

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", msg->getData(), "REJECT", sn.m_strIP.c_str());

            break;
         }

         case 104: // move a dir/file
         {
            int rwx = 2;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);

               break;
            }

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 105: // delete dir/file
         {
            int rwx = 2;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "delete", msg->getData(), "REJECT", "");
               break;
            }

            set<Address, AddrComp> addr;
            string filename = msg->getData();
            pthread_mutex_lock(&self->m_MetaLock);
            self->m_Metadata.lookup(filename.c_str(), addr);
            pthread_mutex_unlock(&self->m_MetaLock);

            for (set<Address, AddrComp>::iterator i = addr.begin(); i != addr.end(); ++ i)
            {
               int msgid = 0;
               self->m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);

               //TODO: update used disk space of the slave node
            }

            pthread_mutex_lock(&self->m_MetaLock);
            self->m_Metadata.remove(filename.c_str(), true);
            pthread_mutex_unlock(&self->m_MetaLock);

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", filename.c_str(), "SUCCESS", "");

            break;
         }

         case 110: // open file
         {
            int32_t dataport = *(int32_t*)(msg->getData());
            int32_t mode = *(int32_t*)(msg->getData() + 4);
            string path = msg->getData() + 8;

            // check user's permission on that file
            int rwx = mode;
            if (!user->match(path.c_str(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", msg->getData(), "REJECT", "");
               break;
            }

            SNode attr;
            pthread_mutex_lock(&self->m_MetaLock);
            int r = self->m_Metadata.lookup(path.c_str(), attr);
            pthread_mutex_unlock(&self->m_MetaLock);

            Address addr;
            SlaveNode sn;

            if (r < 0)
            {
               // file does not exist
               if (mode == 1)
               {
                  self->reject(ip, port, id, SectorError::E_NOEXIST);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", msg->getData(), "REJECT", "");
                  break;
               }

               // otherwise, create a new file for write
               // choose a slave node for the new file
               Address client;
               client.m_strIP = ip;
               client.m_iPort = port;
               set<int> empty;
               if (self->m_SlaveManager.chooseIONode(empty, client, mode, sn) < 0)
               {
                  self->reject(ip, port, id, SectorError::E_RESOURCE);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", msg->getData(), "REJECT", "");
                  break;
               }

               addr.m_strIP = sn.m_strIP;
               addr.m_iPort = sn.m_iPort;
            }
            else
            {
               // choose a slave node with the requested file
               Address client;
               client.m_strIP = ip;
               client.m_iPort = port;
               self->m_SlaveManager.chooseIONode(attr.m_sLocation, client, mode, sn);

               addr.m_strIP = sn.m_strIP;
               addr.m_iPort = sn.m_iPort;

               pthread_mutex_lock(&self->m_MetaLock);
               r = self->m_Metadata.lock(path.c_str(), rwx);
               pthread_mutex_unlock(&self->m_MetaLock);
               if (r < 0)
               {
                  self->reject(ip, port, id, SectorError::E_BUSY);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", msg->getData(), "REJECT", "");
                  break;
               }
            }

            int transid = self->m_TransManager.insert(sn.m_iNodeID, 0, key, msg->getType(), path, mode);

            // send infomation back to the client

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&(dataport), 4);
            msg->setData(68, (char*)&key, 4);
            msg->setData(72, (char*)&(mode), 4);
            msg->setData(76, (char*)&(transid), 4);
            msg->setData(80, path.c_str(), path.length() + 1);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);
            dataport = *(int32_t*)(msg->getData());

            msg->setData(0, addr.m_strIP.c_str(), addr.m_strIP.length() + 1);
            msg->setData(64, (char*)&addr.m_iPort, 4);
            msg->setData(68, (char*)&dataport, 4);

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", path.c_str(), "SUCCESS", addr.m_strIP.c_str());

            break;
         }

         // 200+ SPE

         case 201: // prepare SPE input information
         {
            if (!user->m_bExec)
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request SPE", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            vector<string> result;
            char* req = msg->getData();
            int32_t size = *(int32_t*)req;
            int offset = 0;
            bool notfound = false;
            while (size != -1)
            {
               if (self->m_Metadata.collectDataInfo(req + offset + 4, result) < 0)
               {
                  notfound = true;
                  break;
               }

               offset += 4 + size;
               size = *(int32_t*)(req + offset);
            }

            if (notfound)
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request SPE", "", "REJECTED: FILE NO EXIST", "");
               break;
            }

            offset = 0;
            for (vector<string>::iterator i = result.begin(); i != result.end(); ++ i)
            {
               msg->setData(offset, i->c_str(), i->length() + 1);
               offset += i->length() + 1;
            }

            msg->m_iDataLength = SectorMsg::m_iHdrSize + offset;
            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request SPE", "", "SUCCESS", "");

            break;
         }

         case 202: // locate SPEs
         {
            if (!user->m_bExec)
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "locate SPE", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            int c = 0;
            for (map<int, SlaveNode>::iterator i = self->m_SlaveManager.m_mSlaveList.begin(); i != self->m_SlaveManager.m_mSlaveList.end(); ++ i)
            {
               msg->setData(c * 68, i->second.m_strIP.c_str(), i->second.m_strIP.length() + 1);
               msg->setData(c * 68 + 64, (char*)&(i->second.m_iPort), 4);
               c ++;
            }

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "locate SPE", "", "SUCCESS", "");

            break;
         }

         case 203: // start spe
         {
            if (!user->m_bExec)
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request SPE", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            int transid = self->m_TransManager.insert(self->m_SlaveManager.m_mAddrList[addr], 1, key, msg->getType(), "", 0);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);
            msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request SPE", "", "SUCCESS", addr.m_strIP.c_str());

            break;
         }

         case 204: // start shuffler
         {
            if (!user->m_bExec)
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request Shuffler", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            int transid = self->m_TransManager.insert(self->m_SlaveManager.m_mAddrList[addr], 1, key, msg->getType(), "", 0);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);
            msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "request Shuffler", "", "SUCCESS", addr.m_strIP.c_str());

            break;
         }

         default:
         {
            self->reject(ip, port, id, SectorError::E_UNKNOWN);
            break;
         }
      }
   }

   return NULL;
}

void Master::reject(char* ip, int port, int id, int32_t code)
{
   SectorMsg msg;
   msg.setType(-1);
   msg.setData(0, (char*)&code, 4);
   msg.m_iDataLength = SectorMsg::m_iHdrSize + 4;
   m_GMP.sendto(ip, port, id, &msg);
}

void* Master::processEx(void* p)
{
   return NULL;
}


void* Master::replica(void* s)
{
   Master* self = (Master*)s;

   while (self->m_Status == RUNNING)
   {
      pthread_mutex_lock(&self->m_ReplicaLock);

      vector<string>::iterator r = self->m_vstrToBeReplicated.begin();

      for (; r != self->m_vstrToBeReplicated.end(); ++ r)
      {
         if (self->m_TransManager.getTotalTrans() + self->m_sstrOnReplicate.size() >= self->m_SlaveManager.getTotalSlaves())
            break;

         // avoid replicate a file that is currently being replicated
         if (self->m_sstrOnReplicate.find(*r) == self->m_sstrOnReplicate.end())
            self->createReplica(*r);
      }

      // remove those already been replicated
      self->m_vstrToBeReplicated.erase(self->m_vstrToBeReplicated.begin(), r);

      pthread_cond_wait(&self->m_ReplicaCond, &self->m_ReplicaLock);

      pthread_mutex_unlock(&self->m_ReplicaLock);
   }

   return NULL;
}

void Master::checkReplica(map<string, SNode>& currdir, const string& currpath, vector<string>& replica)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         if (int(i->second.m_sLocation.size()) < m_SysConfig.m_iReplicaNum)
            replica.insert(replica.end(), currpath + "/" + i->second.m_strName);
      }
      else
      {
          string path = currpath + "/" + i->second.m_strName;
          checkReplica(i->second.m_mDirectory, path, replica);
      }
   }
}

int Master::createReplica(const string& path)
{
   SNode attr;
   pthread_mutex_lock(&m_MetaLock);
   int r = m_Metadata.lookup(path.c_str(), attr);
   pthread_mutex_unlock(&m_MetaLock);

   if (r < 0)
      return r;

   SlaveNode sn;
   if (m_SlaveManager.chooseReplicaNode(attr.m_sLocation, sn, attr.m_llSize) < 0)
      return -1;

   SectorMsg msg;
   msg.setType(111);
   msg.setData(0, (char*)&attr.m_llTimeStamp, 8);
   msg.setData(8, path.c_str(), path.length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
      return -1;

   m_sstrOnReplicate.insert(path);

   // replicate index file to the same location
   string idx = path + ".idx";
   pthread_mutex_lock(&m_MetaLock);
   r = m_Metadata.lookup(idx.c_str(), attr);
   pthread_mutex_unlock(&m_MetaLock);

   if (r < 0)
      return 0;

   msg.setType(111);
   msg.setData(0, (char*)&attr.m_llTimeStamp, 8);
   msg.setData(8, idx.c_str(), idx.length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
      return 0;

   m_sstrOnReplicate.insert(idx);

   return 0;
}

int Master::removeReplica(const string& path)
{
   return 0;
}

void Master::loadSlaveAddr(string file)
{   
   ifstream ifs(file.c_str());

   if (ifs.bad() || ifs.fail())
      return;

   while (!ifs.eof())
   {
      char line[256];
      line[0] = '\0';
      ifs.getline(line, 256);
      if (strlen(line) == 0)
         continue;

      SlaveAddr sa;
      sa.m_strAddr = line;
      sa.m_strAddr = sa.m_strAddr.substr(0, sa.m_strAddr.find(' '));
      sa.m_strBase = line;
      sa.m_strBase = sa.m_strBase.substr(sa.m_strBase.find(' ') + 1, sa.m_strBase.length());
      string ip = sa.m_strAddr.substr(sa.m_strAddr.find('@') + 1, sa.m_strAddr.length());

      m_mSlaveAddrRec[ip] = sa;
   }
}
