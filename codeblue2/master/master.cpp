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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/18/2008
*****************************************************************************/

#include <common.h>
#include <ssltransport.h>
#include "master.h"
#include <dirent.h>
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

   if ((rwx & 4) != 0)
   {
      if (!m_vstrExecList.empty())
         rwx ^= 4;
   }

   return (rwx == 0);
}


Master::Master()
{
}

Master::~Master()
{
}

int Master::init()
{
   // read configuration from master.conf
   m_SysConfig.init("master.conf");

   // check local directory
   m_strHomeDir = m_SysConfig.m_strHomeDir;
   DIR* test = opendir((m_strHomeDir + ".metadata").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".metadata").c_str(), S_IRWXU) < 0))
         return -4;
   }
   closedir(test);

   test = opendir((m_strHomeDir + ".sphere").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".sphere").c_str(), S_IRWXU) < 0))
         return -4;
   }
   closedir(test);

   test = opendir((m_strHomeDir + ".tmp").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".tmp").c_str(), S_IRWXU) < 0))
         return -4;
   }
   closedir(test);


   m_mActiveUser.clear();
   ActiveUser au;
   au.m_strName = "slave";
   au.m_iKey = 0;
   au.m_vstrReadList.insert(au.m_vstrReadList.begin(), "/");
   m_mActiveUser[au.m_iKey] = au;


   m_Status = RUNNING;

   // start service thread
   pthread_t svcserver;
   pthread_create(&svcserver, NULL, service, this);
   pthread_detach(svcserver);

   // start GMP
   if (m_GMP.init(m_SysConfig.m_iServerPort) < 0)
      return -5;

   // start management/process thread
   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   return 1;
}

int Master::run()
{
   while (m_Status == RUNNING)
   {
      // check each slave node
      // if probe fails, remove the metadata about the data on the node, and create new replicas

      for (map<int, SlaveNode>::iterator i = m_SlaveList.m_mSlaveList.begin(); i != m_SlaveList.m_mSlaveList.end(); ++ i)
      {
         SectorMsg msg;
         msg.setType(1);

         if (m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, &msg, &msg) < 0)
         {
            //remote start
            //system("ssh xxx")
         }
         else
         {
            i->second.m_llLastUpdateTime = CTimer::getTime();
         }

         if (CTimer::getTime() - i->second.m_llLastUpdateTime > 600)
         {
            Address addr;
            addr.m_strIP = i->second.m_strIP;
            addr.m_iPort = i->second.m_iPort;
            m_Metadata.substract(m_Metadata.m_mDirectory, addr);
         }
      }

      // check replica, create or remove replicas if necessary
      checkReplica(m_Metadata.m_mDirectory, "/");

      sleep(60);
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
   secconn.initClientCTX("security_server.cert");
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
            sn.m_llMaxDiskSpace = -1;
            sn.m_llUsedDiskSpace = 0;
            sn.m_llLastUpdateTime = CTimer::getTime();
            sn.m_iCurrWorkLoad = 0;
            sn.m_strExecDir = "";

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = sn.m_iPort;

            int size = 0;
            s->recv((char*)&size, 4);

            char* buf = new char[size];
            s->recv(buf, size);

            ofstream meta((self->m_strHomeDir + ".metadata/" + ip).c_str());
            meta.write(buf, size);
            meta.close();
            map<string, SNode> branch;
            ifstream ifs((self->m_strHomeDir + ".metadata/" + ip).c_str());
            Index::deserialize(ifs, branch, addr);
            ifs.close();
            Index::merge(self->m_Metadata.m_mDirectory, branch);

            sn.m_llUsedDiskSpace = Index::getTotalDataSize(branch);
            s->recv((char*)&(sn.m_llMaxDiskSpace), 8);
            s->recv((char*)&(sn.m_iClusterID), 4);

            self->m_SlaveList.insert(sn);
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

            secconn.recv((char*)&size, 4);
            if (size > 0)
            {
               buf = new char[size];
               secconn.recv(buf, size);
               au.deserialize(au.m_vstrExecList, buf);
               delete [] buf;
            }

            self->m_mActiveUser[au.m_iKey] = au;
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
         self->reject(ip, port, id, -1);
         continue;
      }

      ActiveUser* user = &(i->second);

      if (key > 0)
      {
         if ((user->m_strIP != ip) || (user->m_iPort != port))
         {
            self->reject(ip, port, id, -1);
            continue;
         }
      }
      else if (key == 0)
      {
         Address addr;
         addr.m_strIP = ip;
         addr.m_iPort = port;
         if (self->m_SlaveList.m_mAddrList.end() == self->m_SlaveList.m_mAddrList.find(addr))
         {
            self->reject(ip, port, id, -1);
            continue;
         }
      }
      else
      {
         self->reject(ip, port, id, -1);
         continue;
      }

      switch (msg->getType())
      {
         // internal system commands

         case 1: // slave reports transaction status
         {
            //int transid = *(int32_t*)msg->getData();

            cout << "get report " << msg->getData() + 4 << endl;

            Address addr;
            addr.m_strIP = ip;
            addr.m_iPort = port;
            self->m_Metadata.update(msg->getData() + 4, addr);
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 2: // client logout
         {
            self->m_mActiveUser.erase(key);
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         // 100+ storage system

         case 101: // ls
         {
            int rwx = 1;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            vector<string> filelist;
            self->m_Metadata.list(msg->getData(), filelist);

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

            break;
         }

         case 102: // stat
         {
            int rwx = 1;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            SNode attr;
            if (self->m_Metadata.lookup(msg->getData(), attr) < 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = SectorMsg::m_iHdrSize;
               self->m_GMP.sendto(ip, port, id, msg);
            }
            else
            {
               char buf[128];
               attr.serialize(buf);
               msg->setData(0, buf, strlen(buf) + 1);
               msg->m_iDataLength = SectorMsg::m_iHdrSize + strlen(buf) + 1;
               self->m_GMP.sendto(ip, port, id, msg);
            }

            break;
         }

         case 103: // mkdir
         {
            int rwx = 2;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            self->m_Metadata.create(msg->getData(), true);
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 104: // move a dir/file
         {
            int rwx = 2;
            if (!user->match(msg->getData(), rwx))
            {
               self->reject(ip, port, id, -1);
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
               self->reject(ip, port, id, -1);
               break;
            }

            set<Address, AddrComp> addr;
            self->m_Metadata.lookup(msg->getData(), addr);

            for (set<Address, AddrComp>::iterator i = addr.begin(); i != addr.end(); ++ i)
            {
               self->m_GMP.rpc(i->m_strIP.c_str(), i->m_iPort, msg, msg);
            }

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 110: // open file
         {
            int32_t dataport = *(int32_t*)(msg->getData());
            int32_t mode = *(int32_t*)(msg->getData() + 4);
            char path[128];
            strcpy(path, msg->getData() + 8);

            cout << "open file " << ip << " " << port << " " << dataport << " " << mode << " " << path << " " << key << endl;

            // check user's permission on that file

            int rwx = mode;
            if (!user->match(path, rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            SNode attr;
            int r = self->m_Metadata.lookup(path, attr);

            Address addr;

            if (r < 0)
            {
               // file does not exist
               if (mode == 1)
               {
                  self->reject(ip, port, id, -1);
                  break;
               }

               // otherwise, create a new file for write
               self->m_Metadata.create(path);

               // choose a slave node for the new file
               SlaveNode sn;
               Address client;
               client.m_strIP = ip;
               client.m_iPort = port;
               set<int> empty;
               self->m_SlaveList.chooseIONode(empty, client, rwx, sn);

               addr.m_strIP = sn.m_strIP;
               addr.m_iPort = sn.m_iPort;
            }
            else
            {
               // choose a slave node with the requested file
               SlaveNode sn;
               Address client;
               client.m_strIP = ip;
               client.m_iPort = port;
               self->m_SlaveList.chooseIONode(attr.m_sLocation, client, rwx, sn);

               addr.m_strIP = sn.m_strIP;
               addr.m_iPort = sn.m_iPort;
            }

            //if (self->m_Metadata.lock(path, rwx) < 0)
            //{
            //   self->reject(ip, port, id, -2);
            //   break;
            //}

            // send infomation back to the client

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&(dataport), 4);
            msg->setData(68, (char*)&(mode), 4);
            msg->setData(72, path, strlen(path) + 1);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);
            dataport = *(int32_t*)(msg->getData());

            msg->setData(0, addr.m_strIP.c_str(), addr.m_strIP.length() + 1);
            msg->setData(64, (char*)&addr.m_iPort, 4);
            msg->setData(68, (char*)&dataport, 4);

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         // 200+ SPE

         case 201: // upload spe
         {
            int rwx = 4;
            if (!user->match("", rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            string lib = msg->getData();
            int libsize = msg->m_iDataLength - SectorMsg::m_iHdrSize - 64;

            char path[128];
            sprintf(path, "%s/.sphere/%d", self->m_strHomeDir.c_str(), key);

            ::mkdir(path, S_IRWXU);

            ofstream ofs((string(path) + "/" + lib).c_str());
            ofs.write(msg->getData() + 64, libsize);
            ofs.close();

            for (map<int, SlaveNode>::iterator i = self->m_SlaveList.m_mSlaveList.begin(); i != self->m_SlaveList.m_mSlaveList.end(); ++ i)
            {
               msg->m_iDataLength = SectorMsg::m_iHdrSize + libsize + 64;
               self->m_GMP.rpc(i->second.m_strIP.c_str(), i->second.m_iPort, msg, msg);
            }

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 202: // locate SPEs
         {
            int rwx = 4;
            if (!user->match("", rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            int c = 0;
            for (map<int, SlaveNode>::iterator i = self->m_SlaveList.m_mSlaveList.begin(); i != self->m_SlaveList.m_mSlaveList.end(); ++ i)
            {
               msg->setData(c * 68, i->second.m_strIP.c_str(), i->second.m_strIP.length() + 1);
               msg->setData(c * 68 + 64, (char*)&(i->second.m_iPort), 4);
               c ++;
            }

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 203: // start spe
         {
            int rwx = 4;
            if (!user->match("", rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 204: // start shuffler
         {
            int rwx = 4;
            if (!user->match("", rwx))
            {
               self->reject(ip, port, id, -1);
               break;
            }

            Address addr;
            addr.m_strIP = msg->getData();
            addr.m_iPort = *(int32_t*)(msg->getData() + 64);

            msg->setData(0, ip, strlen(ip) + 1);
            msg->setData(64, (char*)&port, 4);

            self->m_GMP.rpc(addr.m_strIP.c_str(), addr.m_iPort, msg, msg);

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         default:
         {
            self->reject(ip, port, id, -1);
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

void Master::checkReplica(map<string, SNode>& currdir, const string& currpath)
{
   for (map<string, SNode>::iterator i = currdir.begin(); i != currdir.end(); ++ i)
   {
      if (!i->second.m_bIsDir)
      {
         if (int(i->second.m_sLocation.size()) < m_SysConfig.m_iReplicaNum)
         {
            // choose a node for replica
            // using a random node, should fix this later
            SlaveNode sn;
            if (m_SlaveList.chooseReplicaNode(i->second.m_sLocation, sn) > 0)
               createReplica(sn.m_strIP.c_str(), sn.m_iPort, currpath + "/" + i->second.m_strName);
         }
      }
      else
      {
          string path = currpath + "/" + i->second.m_strName;
          checkReplica(i->second.m_mDirectory, path);
      }
   }
}

int Master::createReplica(const char* ip, int port, const string& path)
{
   SectorMsg msg;
   msg.setType(111);
   msg.setData(0, path.c_str(), path.length() + 1);
   msg.m_iDataLength = SectorMsg::m_iHdrSize + path.length() + 1;
   m_GMP.rpc(ip, port, &msg, &msg);

   return 0;
}

int Master::removeReplica(const char* ip, int port, const string& path)
{

   return 0;
}
