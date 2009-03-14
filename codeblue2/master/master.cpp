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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/10/2009
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
   // check read flag bit 1 and write flag bit 2
   rwx &= 3;

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


Master::Master():
m_pcTopoData(NULL),
m_iTopoDataSize(0)
{
   pthread_mutex_init(&m_ReplicaLock, NULL);
   pthread_cond_init(&m_ReplicaCond, NULL);
}

Master::~Master()
{
   m_SectorLog.close();
   delete [] m_pcTopoData;
   pthread_mutex_destroy(&m_ReplicaLock);
   pthread_cond_destroy(&m_ReplicaCond);
}

int Master::init()
{
   m_SectorLog.init("sector.log");

   // read configuration from master.conf
   if (m_SysConfig.init("master.conf") < 0)
   {
      cerr << "unable to read/parse configuration file.\n";
      m_SectorLog.insert("unable to read/parse configuration file.");
      return -1;
   }

   if (m_SlaveManager.init("topology.conf") < 0)
   {
      cerr << "Warning: no topology configuration found.\n";
      m_SectorLog.insert("Warning: no topology configuration found.");
   }
   else
   {
      m_iTopoDataSize = m_SlaveManager.m_Topology.getTopoDataSize();
      m_pcTopoData = new char[m_iTopoDataSize];
      m_SlaveManager.m_Topology.serialize(m_pcTopoData, m_iTopoDataSize);
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
         m_SectorLog.insert("unable to create home directory.");
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
      m_SectorLog.insert("cannot initialize GMP.");
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

   cout << "Sector master is successfully running now. check sector.log for more details.\n";
   cout << "There is no further screen output from this program.\n";

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
            i->second.deserialize(msg.getData(), msg.m_iDataLength);

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
            m_SectorLog.insert(("Slave lost " + i->second.m_strIP + ".").c_str());

            // to be removed
            tbrs.insert(tbrs.end(), i->first);

            // remove the data in that 
            Address addr;
            addr.m_strIP = i->second.m_strIP;
            addr.m_iPort = i->second.m_iPort;
            CGuard::enterCS(m_Metadata.m_MetaLock);
            m_Metadata.substract(m_Metadata.m_mDirectory, addr);
            CGuard::leaveCS(m_Metadata.m_MetaLock);

            //remove all associated transactions and release IO locks...
            vector<int> trans;
            m_TransManager.retrieve(i->first, trans);
            for (vector<int>::iterator t = trans.begin(); t != trans.end(); ++ t)
            {
               Transaction tt;
               m_TransManager.retrieve(*t, tt);
               m_Metadata.unlock(tt.m_strFile.c_str(), tt.m_iMode);
               m_TransManager.updateSlave(*t, i->first);
            }

            // to be restarted
            map<string, SlaveAddr>::iterator sa = m_mSlaveAddrRec.find(i->second.m_strIP);
            if (sa != m_mSlaveAddrRec.end())
               tbsaddr.insert(tbsaddr.end(), sa->second);
         }

         if (i->second.m_sBadVote.size() * 2 > m_SlaveManager.m_mSlaveList.size())
         {
            vector<int> trans;
            m_TransManager.retrieve(i->first, trans);
            if (trans.size() > 0)
               continue;

            m_SectorLog.insert(("Bad slave detected " + i->second.m_strIP + ".").c_str());

            // remove the data on that slave
            Address addr;
            addr.m_strIP = i->second.m_strIP;
            addr.m_iPort = i->second.m_iPort;
            CGuard::enterCS(m_Metadata.m_MetaLock);
            m_Metadata.substract(m_Metadata.m_mDirectory, addr);
            CGuard::leaveCS(m_Metadata.m_MetaLock);

            // to be removed
            tbrs.insert(tbrs.end(), i->first);
         }
         else if (i->second.m_llLastVoteTime - CTimer::getTime() > 24LL * 60 * 3600 * 1000000)
         {
            i->second.m_sBadVote.clear();
            i->second.m_llLastVoteTime = CTimer::getTime();
         }
      }

      // remove from slave list
      for (vector<int>::iterator i = tbrs.begin(); i != tbrs.end(); ++ i)
         m_SlaveManager.remove(*i);

      // update cluster statistics
      m_SlaveManager.updateClusterStat();

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
         CGuard::enterCS(m_Metadata.m_MetaLock);
         checkReplica(m_Metadata.m_mDirectory, "/", m_vstrToBeReplicated);
         CGuard::leaveCS(m_Metadata.m_MetaLock);
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
      cmd = SectorError::E_NOSECSERV;
      s->send((char*)&cmd, 4);
      goto EXIT;
   }

   switch (cmd)
   {
      case 1: // slave node join
      {
         secconn.send((char*)&cmd, 4);
         char slaveIP[64];
         strcpy(slaveIP, ip.c_str());
         secconn.send(slaveIP, 64);
         int32_t res = -1;
         secconn.recv((char*)&res, 4);

         s->send((char*)&res, 4);

         if (res == 1)
         {
            SlaveNode sn;
            sn.m_strIP = ip;
            s->recv((char*)&sn.m_iPort, 4);
            s->recv((char*)&sn.m_iDataPort, 4);
            sn.m_llLastUpdateTime = CTimer::getTime();
            sn.m_iRetryNum = 0;
            sn.m_llLastVoteTime = CTimer::getTime();

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = sn.m_iPort;

            int32_t size = 0;
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

            ofstream left((self->m_strHomeDir + ".metadata/" + ip + ".left").c_str());
            CGuard::enterCS(self->m_Metadata.m_MetaLock);
            Index::merge(self->m_Metadata.m_mDirectory, branch, "/", left);
            CGuard::leaveCS(self->m_Metadata.m_MetaLock);
            left.close();

            ifs.open((self->m_strHomeDir + ".metadata/" + ip + ".left").c_str());
            ifs.seekg(0, ios::end);
            size = ifs.tellg();
            s->send((char*)&size, 4);
            if (size > 0)
            {
               buf = new char[size];
               ifs.seekg(0);
               ifs.read(buf, size);
               s->send(buf, size);
               delete [] buf;
            }
            ifs.close();

            sn.m_llTotalFileSize = Index::getTotalDataSize(branch);
            s->recv((char*)&(sn.m_llAvailDiskSpace), 8);
            sn.m_llCurrMemUsed = 0;
            sn.m_llCurrCPUUsed = 0;
            sn.m_llTotalInputData = 0;
            sn.m_llTotalOutputData = 0;

            // the slave manager will assign a unique ID to the new node
            self->m_SlaveManager.insert(sn);
            self->m_SlaveManager.updateClusterStat();

            s->send((char*)&sn.m_iNodeID, 4);

            char text[64];
            sprintf(text, "Slave node %s:%d joined.", ip.c_str(), sn.m_iPort);
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

         int32_t key = 0;
         secconn.recv((char*)&key, 4);

         s->send((char*)&key, 4);

         if (key > 0)
         {
            ActiveUser au;
            au.m_strName = user;
            au.m_strIP = ip;
            au.m_iKey = key;
            au.m_llLastRefreshTime = CTimer::getTime();

            s->recv((char*)&au.m_iPort, 4);
            s->recv((char*)&au.m_iDataPort, 4);
            s->recv((char*)au.m_pcKey, 16);
            s->recv((char*)au.m_pcIV, 8);

            s->send((char*)&self->m_iTopoDataSize, 4);
            if (self->m_iTopoDataSize > 0)
               s->send(self->m_pcTopoData, self->m_iTopoDataSize);

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
            int slaveid = *(int32_t*)(msg->getData() + 4);

            Transaction t;
            if (self->m_TransManager.retrieve(transid, t) < 0)
            {
               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }
            
            int change = *(int32_t*)(msg->getData() + 8);
            string path = msg->getData() + 12;

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = port;

            set<Address, AddrComp> tbr;
            int r = self->m_Metadata.update(path.c_str(), addr, change);

            if (change == 3)
            {
               SNode attr;
               attr.deserialize(path.c_str());
               self->m_sstrOnReplicate.erase(attr.m_strName);
            }

            // unlock the file, if this is a file operation
            // update transaction status, if this is a file operation; if it is sphere, a final sphere report will be sent, see #4.
            if (t.m_iType == 0)
            {
               self->m_Metadata.unlock(t.m_strFile.c_str(), t.m_iMode);
               self->m_TransManager.updateSlave(transid, slaveid);
            }

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            if (r < 0)
               msg->setType(-msg->getType());
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
            CGuard::enterCS(self->m_Metadata.m_MetaLock);
            self->m_SysStat.m_llTotalFileSize = Index::getTotalDataSize(self->m_Metadata.m_mDirectory);
            self->m_SysStat.m_llTotalFileNum = Index::getTotalFileNum(self->m_Metadata.m_mDirectory);
            CGuard::leaveCS(self->m_Metadata.m_MetaLock);

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

         case 4: // sphere status & performance report
         {
            int transid = *(int32_t*)msg->getData();
            int slaveid = *(int32_t*)(msg->getData() + 4);

            Transaction t;
            if ((self->m_TransManager.retrieve(transid, t) < 0) || (t.m_iType != 1))
            {
               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            // the slave votes slow slaves
            int num = *(int*)(msg->getData() + 8);
            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = port;
            int voter = self->m_SlaveManager.m_mAddrList[addr];
            vector<Address> bad;
            for (int i = 0; i < num; ++ i)
            {
               addr.m_strIP = msg->getData() + 12 + i * 68;
               addr.m_iPort = *(int*)(msg->getData() + 12 + i * 68 + 64);

               int slave = self->m_SlaveManager.m_mAddrList[addr];
               self->m_SlaveManager.m_mSlaveList[slave].m_sBadVote.insert(voter);
            }

            self->m_TransManager.updateSlave(transid, slaveid);

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         // 100+ storage system

         case 101: // ls
         {
            int rwx = SF_MODE::READ;
            string dir = msg->getData();
            if (!user->match(dir, rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "ls", dir.c_str(), "REJECT", "");
               break;
            }

            vector<string> filelist;
            self->m_Metadata.list(dir.c_str(), filelist);

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

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "ls", dir.c_str(), "SUCCESS", "");

            break;
         }

         case 102: // stat
         {
            int rwx = SF_MODE::READ;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }

            SNode attr;
            int r = self->m_Metadata.lookup(msg->getData(), attr);
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
            int rwx = SF_MODE::WRITE;
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
            vector<SlaveNode> addr;
            if (self->m_SlaveManager.chooseIONode(empty, client, SF_MODE::WRITE, addr, 1) <= 0)
            {
               self->reject(ip, port, id, SectorError::E_RESOURCE);
               break;
            }

            int msgid = 0;
            self->m_GMP.sendto(addr.begin()->m_strIP.c_str(), addr.begin()->m_iPort, msgid, msg);

            self->m_Metadata.create(msg->getData(), true);

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", msg->getData(), "REJECT", addr.begin()->m_strIP.c_str());

            break;
         }

         case 104: // move a dir/file
         {
            string src = msg->getData() + 4;
            string dst = msg->getData() + 4 + src.length() + 1 + 4;
            string uplevel = dst.substr(0, dst.find('/'));
            string sublevel = dst + src.substr(src.rfind('/'), src.length());

            SNode tmp;
            if ((uplevel.length() > 0) && (self->m_Metadata.lookup(uplevel.c_str(), tmp) < 0))
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               break;
            }
            if (self->m_Metadata.lookup(sublevel.c_str(), tmp) >= 0)
            {
               self->reject(ip, port, id, SectorError::E_EXIST);
               break;
            }

            int rwx = SF_MODE::READ;
            if (!user->match(src.c_str(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }
            rwx = SF_MODE::WRITE;
            if (!user->match(dst.c_str(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }

            SNode as, at;
            int rs = self->m_Metadata.lookup(src.c_str(), as);
            int rt = self->m_Metadata.lookup(dst.c_str(), at);
            set<Address, AddrComp> addrlist;
            self->m_Metadata.lookup(src.c_str(), addrlist);

            if (rs < 0)
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               break;
            }
            if ((rt >= 0) && (!at.m_bIsDir))
            {
               self->reject(ip, port, id, SectorError::E_EXIST);
               break;
            }

            string newname = dst.substr(dst.rfind('/'), dst.length());
            if (rt < 0)
               self->m_Metadata.move(src.c_str(), uplevel.c_str(), newname.c_str());
            else
               self->m_Metadata.move(src.c_str(), dst.c_str());

            msg->setData(0, src.c_str(), src.length() + 1);
            msg->setData(src.length() + 1, uplevel.c_str(), uplevel.length() + 1);
            msg->setData(src.length() + 1 + uplevel.length() + 1, newname.c_str(), newname.length() + 1);
            for (set<Address, AddrComp>::iterator i = addrlist.begin(); i != addrlist.end(); ++ i)
            {
               int msgid = 0;
               self->m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);
            }

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 105: // delete dir/file
         {
            int rwx = SF_MODE::WRITE;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "delete", msg->getData(), "REJECT", "");
               break;
            }

            set<Address, AddrComp> addr;
            string filename = msg->getData();
            self->m_Metadata.lookup(filename.c_str(), addr);

            for (set<Address, AddrComp>::iterator i = addr.begin(); i != addr.end(); ++ i)
            {
               int msgid = 0;
               self->m_GMP.sendto(i->m_strIP.c_str(), i->m_iPort, msgid, msg);

               //TODO: update used disk space of the slave node
            }

            self->m_Metadata.remove(filename.c_str(), true);

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "mkdir", filename.c_str(), "SUCCESS", "");

            break;
         }

         case 106: // make a copy of a file/dir
         {
            string src = msg->getData() + 4;
            string dst = msg->getData() + 4 + src.length() + 1 + 4;
            string uplevel = dst.substr(0, dst.find('/'));
            string sublevel = dst + src.substr(src.rfind('/'), src.length());

            SNode tmp;
            if ((uplevel.length() > 0) && (self->m_Metadata.lookup(uplevel.c_str(), tmp) < 0))
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               break;
            }
            if (self->m_Metadata.lookup(sublevel.c_str(), tmp) >= 0)
            {
               self->reject(ip, port, id, SectorError::E_EXIST);
               break;
            }

            int rwx = SF_MODE::READ;
            if (!user->match(src.c_str(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }
            rwx = SF_MODE::WRITE;
            if (!user->match(dst.c_str(), rwx))
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               break;
            }

            SNode as, at;
            int rs = self->m_Metadata.lookup(src.c_str(), as);
            int rt = self->m_Metadata.lookup(dst.c_str(), at);
            vector<string> filelist;
            self->m_Metadata.list_r(src.c_str(), filelist);

            if (rs < 0)
            {
               self->reject(ip, port, id, SectorError::E_NOEXIST);
               break;
            }
            if ((rt >= 0) && (!at.m_bIsDir))
            {
               self->reject(ip, port, id, SectorError::E_EXIST);
               break;
            }

            // replace the directory prefix with dst
            string rep;
            if (rt < 0)
               rep = src;
            else
               rep = src.substr(0, src.rfind('/'));

            pthread_mutex_lock(&self->m_ReplicaLock);
            for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
            {
               string target = *i;
               target.replace(0, rep.length(), dst);
               self->m_vstrToBeReplicated.insert(self->m_vstrToBeReplicated.begin(), src + "\t" + target);
            }
            if (!self->m_vstrToBeReplicated.empty())
               pthread_cond_signal(&self->m_ReplicaCond);
            pthread_mutex_unlock(&self->m_ReplicaLock);

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 110: // open file
         {
            int32_t mode = *(int32_t*)(msg->getData());
            int32_t dataport = *(int32_t*)(msg->getData() + 4);
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
            int r = self->m_Metadata.lookup(path.c_str(), attr);

            Address client;
            client.m_strIP = ip;
            client.m_iPort = port;
            vector<SlaveNode> addr;

            if (r < 0)
            {
               // file does not exist
               if (!(mode & SF_MODE::WRITE))
               {
                  self->reject(ip, port, id, SectorError::E_NOEXIST);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", path.c_str(), "REJECT", "");
                  break;
               }

               // otherwise, create a new file for write
               // choose a slave node for the new file
               set<int> empty;
               if (self->m_SlaveManager.chooseIONode(empty, client, mode, addr, self->m_SysConfig.m_iReplicaNum) <= 0)
               {
                  self->reject(ip, port, id, SectorError::E_RESOURCE);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", msg->getData(), "REJECT", "");
                  break;
               }
            }
            else
            {
               self->m_SlaveManager.chooseIONode(attr.m_sLocation, client, mode, addr, self->m_SysConfig.m_iReplicaNum);

               r = self->m_Metadata.lock(path.c_str(), rwx);
               if (r < 0)
               {
                  self->reject(ip, port, id, SectorError::E_BUSY);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", path.c_str(), "REJECT", "");
                  break;
               }
            }

            int transid = self->m_TransManager.create(0, key, msg->getType(), path, mode);

            //set up all slave nodes, chain of write
            string srcip;
            int srcport;
            string dstip = "";
            int dstport = 0;

            msg->setData(136, (char*)&key, 4);
            msg->setData(140, (char*)&mode, 4);
            msg->setData(144, (char*)&transid, 4);
            msg->setData(148, (char*)user->m_pcKey, 16);
            msg->setData(164, (char*)user->m_pcIV, 8);
            msg->setData(172, path.c_str(), path.length() + 1);

            for (vector<SlaveNode>::iterator i = addr.begin(); i != addr.end();)
            {
               vector<SlaveNode>::iterator curraddr = i ++;

               if (i == addr.end())
               {
                  srcip = ip;
                  srcport = dataport;
               }
               else
               {
                  srcip = i->m_strIP;
                  srcport = i->m_iDataPort;
                  // do not use secure transfer between slave nodes
                  int m = mode & (0xFFFFFFFF - SF_MODE::SECURE);
                  msg->setData(140, (char*)&m, 4);
               }

               msg->setData(0, srcip.c_str(), srcip.length() + 1);
               msg->setData(64, (char*)&(srcport), 4);
               msg->setData(68, dstip.c_str(), dstip.length() + 1);
               msg->setData(132, (char*)&(dstport), 4);
               SectorMsg response;
               if ((self->m_GMP.rpc(curraddr->m_strIP.c_str(), curraddr->m_iPort, msg, &response) < 0) || (response.getType() < 0))
               {
                  self->reject(ip, port, id, SectorError::E_RESOURCE);
                  self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", path.c_str(), "FAIL", "");

                  //TODO: FIX THIS, ROLLBACK TRANS
               }

               dstip = curraddr->m_strIP;
               dstport = curraddr->m_iDataPort;

               self->m_TransManager.addSlave(transid, curraddr->m_iNodeID);
            }

            // send the connection information back to the client
            msg->setData(0, dstip.c_str(), dstip.length() + 1);
            msg->setData(64, (char*)&dstport, 4);
            msg->setData(68, (char*)&transid, 4);
            msg->setData(72, (char*)&attr.m_llSize, 8);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 80;

            self->m_GMP.sendto(ip, port, id, msg);

            if (key != 0)
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "open", path.c_str(), "SUCCESS", addr.rbegin()->m_strIP.c_str());

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
               CGuard::enterCS(self->m_Metadata.m_MetaLock);
               int r = self->m_Metadata.collectDataInfo(req + offset + 4, result);
               CGuard::leaveCS(self->m_Metadata.m_MetaLock);

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
               msg->setData(c * 72, i->second.m_strIP.c_str(), i->second.m_strIP.length() + 1);
               msg->setData(c * 72 + 64, (char*)&(i->second.m_iPort), 4);
               msg->setData(c * 72 + 68, (char*)&(i->second.m_iDataPort), 4);
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
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start SPE", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            int transid = self->m_TransManager.create(1, key, msg->getType(), "", 0);
            int slaveid = self->m_SlaveManager.m_mAddrList[addr];
            self->m_TransManager.addSlave(transid, slaveid);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);
            msg->setData(68, (char*)&user->m_iDataPort, 4);
            msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);

            if ((self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg) < 0) || (msg->getType() < 0))
            {
               self->reject(ip, port, id, SectorError::E_RESOURCE);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start SPE", "", "SALVE FAILURE", "");
               break;
            }

            msg->setData(0, (char*)&transid, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start SPE", "", "SUCCESS", addr.m_strIP.c_str());

            break;
         }

         case 204: // start shuffler
         {
            if (!user->m_bExec)
            {
               self->reject(ip, port, id, SectorError::E_PERMISSION);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start Shuffler", "", "REJECTED DUE TO PERMISSION", "");
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            int transid = self->m_TransManager.create(1, key, msg->getType(), "", 0);
            self->m_TransManager.addSlave(transid, self->m_SlaveManager.m_mAddrList[addr]);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);
            msg->setData(msg->m_iDataLength - SectorMsg::m_iHdrSize, (char*)&transid, 4);

            if ((self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg) < 0) || (msg->getType() < 0))
            {
               self->reject(ip, port, id, SectorError::E_RESOURCE);
               self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start Shuffler", "", "SLAVE FAILURE", "");
               break;
            }

            self->m_GMP.sendto(ip, port, id, msg);

            self->m_SectorLog.logUserActivity(user->m_strName.c_str(), ip, "start Shuffler", "", "SUCCESS", addr.m_strIP.c_str());

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
         {
            int pos = r->find('\t');
            self->createReplica(r->substr(0, pos), r->substr(pos + 1, r->length()));
         }
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
         {
            string file = currpath + "/" + i->second.m_strName;
            replica.insert(replica.end(), file + "\t" + file);
         }
      }
      else
      {
         string path = currpath + "/" + i->second.m_strName;
         checkReplica(i->second.m_mDirectory, path, replica);
      }
   }
}

int Master::createReplica(const string& src, const string& dst)
{
   SNode attr;
   int r = m_Metadata.lookup(src.c_str(), attr);
   if (r < 0)
      return r;

   SlaveNode sn;
   if (src == dst)
   {
      if (m_SlaveManager.chooseReplicaNode(attr.m_sLocation, sn, attr.m_llSize) < 0)
         return -1;
   }
   else
   {
      set<Address, AddrComp> empty;
      if (m_SlaveManager.chooseReplicaNode(empty, sn, attr.m_llSize) < 0)
         return -1;
   }

   int transid = m_TransManager.create(0, 0, 111, dst, 0);

   SectorMsg msg;
   msg.setType(111);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&attr.m_llTimeStamp, 8);
   msg.setData(12, src.c_str(), src.length() + 1);
   msg.setData(12 + src.length() + 1, dst.c_str(), dst.length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
      return -1;

   m_TransManager.addSlave(transid, sn.m_iNodeID);

   if (src == dst)
      m_sstrOnReplicate.insert(src);

   // replicate index file to the same location
   string idx = src + ".idx";
   r = m_Metadata.lookup(idx.c_str(), attr);

   if (r < 0)
      return 0;

   transid = m_TransManager.create(0, 0, 111, dst + ".idx", 0);

   msg.setType(111);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&attr.m_llTimeStamp, 8);
   msg.setData(12, idx.c_str(), idx.length() + 1);
   msg.setData(12 + idx.length() + 1, (dst + ".idx").c_str(), (dst + ".idx").length() + 1);

   if ((m_GMP.rpc(sn.m_strIP.c_str(), sn.m_iPort, &msg, &msg) < 0) || (msg.getData() < 0))
      return 0;

   m_TransManager.addSlave(transid, sn.m_iNodeID);

   if (src == dst)
      m_sstrOnReplicate.insert(idx);

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
