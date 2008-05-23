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
   Yunhong Gu [gu@lac.uic.edu], last updated 05/23/2008
*****************************************************************************/


#include "slave.h"
#include <ssltransport.h>
#include <iostream>
#include <dirent.h>
#include <netdb.h>

using namespace std;

Slave::Slave()
{
}

Slave::~Slave()
{
}

int Slave::init()
{
   m_SysConfig.init("slave.conf");

   m_strMasterHost = m_SysConfig.m_strMasterHost;
   struct hostent* masterip = gethostbyname(m_strMasterHost.c_str());
   if (NULL == masterip)
      return -1;
   char buf[64];
   m_strMasterIP = inet_ntop(AF_INET, masterip->h_addr_list[0], buf, 64);
   m_iMasterPort = m_SysConfig.m_iMasterPort;

   m_GMP.init(0);
   m_iLocalPort = m_GMP.getPort();

   // initialize local directory
   m_strHomeDir = m_SysConfig.m_strHomeDir;

   // check local directory
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

   cout << "scaning " << m_strHomeDir << endl;
   m_LocalFile.scan(m_strHomeDir.c_str(), m_LocalFile.m_mDirectory);

   ofstream ofs((m_strHomeDir + ".metadata/metadata.txt").c_str());
   m_LocalFile.serialize(ofs, m_LocalFile.m_mDirectory, 1);
   ofs.close();

   // start the slave...
   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   return 1;
}

int Slave::run()
{
   // join the server
   SSLTransport::init();

   SSLTransport secconn;
   secconn.initClientCTX("master_node.cert");
   secconn.open(NULL, 0);
   int r = secconn.connect(m_strMasterHost.c_str(), m_iMasterPort);

   if (r < 0)
      return -1;

   cout << "SEC CONN SET UP " << r << endl;

   int cmd = 1;
   secconn.send((char*)&cmd, 4);
   int res = -1;
   secconn.recv((char*)&res, 4);

   cout << "RECV RES " << res << endl;

   if (res > 0)
      secconn.send((char*)&m_iLocalPort, 4);

   struct stat s;
   stat((m_strHomeDir + ".metadata/metadata.txt").c_str(), &s);
   int32_t size = s.st_size;

   cout << "meta size " << size << endl;

   ifstream meta((m_strHomeDir + ".metadata/metadata.txt").c_str());
   char* buf = new char[size];
   meta.read(buf, size);
   meta.close();

   secconn.send((char*)&size, 4);
   secconn.send(buf, size);

   // send total available disk space
   secconn.send((char*)&(m_SysConfig.m_llMaxDataSize), 8);

   // send cluster ID
   secconn.send((char*)&(m_SysConfig.m_iClusterID), 4);

   secconn.close();
   SSLTransport::destroy();

   if (res < 0)
      return res;

   while (true)
   {
      // send keep alive to server

      sleep(10);
   }

   return 1;
}

void* Slave::process(void* s)
{
   Slave* self = (Slave*)s;

   char ip[64];
   int port;
   int32_t id;
   SectorMsg* msg = new SectorMsg;
   msg->resize(65536);

   cout << "slave process " << self->m_iLocalPort << endl;

   while (true)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      cout << "recv cmd " << ip << " " << port << " " << self->m_strMasterIP << " " << self->m_iMasterPort << endl;

      // a slave only accepts commands from the master
      if ((self->m_strMasterIP != ip) || (self->m_iMasterPort != port))
          continue;

      cout << "type " << (msg->getType()) << endl;

      switch (msg->getType())
      {
         case 1: // probe
         {
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 104: // move dir/file
         {
            char* oldpath = msg->getData();
            char* newpath = msg->getData() + 64;
            self->m_LocalFile.move(oldpath, newpath);
            ::rename((self->m_strHomeDir + oldpath).c_str(), (self->m_strHomeDir + newpath).c_str());
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 105: // remove dir/file
         {
            char* path = msg->getData();
            self->m_LocalFile.remove(path, true);
            ::remove((self->m_strHomeDir + path).c_str());
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 110: // open file
         {
            cout << "===> start file server " << ip << " " << port << endl;

            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport);

            Param2* p = new Param2;
            p->serv_instance = self;
            p->datachn = datachn;
            p->client_ip = msg->getData();
            p->client_data_port = *(int*)(msg->getData() + 64);
            p->mode = *(int*)(msg->getData() + 68);
            p->filename = msg->getData() + 72;

            pthread_t file_handler;
            pthread_create(&file_handler, NULL, fileHandler, p);
            pthread_detach(file_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 111: // create a replica to local
         {
            Param2* p = new Param2;
            p->serv_instance = self;
            p->filename = msg->getData();

            pthread_t replica_handler;
            pthread_create(&replica_handler, NULL, copy, p);
            pthread_detach(replica_handler);

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 201: // prepare SPE operator
         {
            string lib = msg->getData();
            int libsize = msg->m_iDataLength - SectorMsg::m_iHdrSize - 64;

            char path[128];
            sprintf(path, "%s/.sphere/%d", self->m_strHomeDir.c_str(), msg->getKey());

            ::mkdir(path, S_IRWXU);

            ofstream ofs((string(path) + "/" + lib).c_str());
            ofs.write(msg->getData() + 64, libsize);
            ofs.close();

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 203: // processing engine
         {
            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport);

            Param4* p = new Param4;
            p->serv_instance = self;
            p->datachn = datachn;
            p->client_ip = msg->getData();
            p->client_ctrl_port = *(int32_t*)(msg->getData() + 64);
            p->speid = *(int32_t*)(msg->getData() + 68);
            p->client_data_port = *(int32_t*)(msg->getData() + 72);
            p->key = *(int32_t*)(msg->getData() + 76);
            p->function = msg->getData() + 80;
            p->rows = *(int32_t*)(msg->getData() + 144);
            p->psize = *(int32_t*)(msg->getData() + 148);
            if (p->psize > 0)
            {
               p->param = new char[p->psize];
               memcpy(p->param, msg->getData() + 152, p->psize);
            }
            else
              p->param = NULL;

            cout << "starting SPE ... " << p->speid << " " << p->client_data_port << " " << p->function << " " << dataport << endl;

            pthread_t spe_handler;
            pthread_create(&spe_handler, NULL, SPEHandler, p);
            pthread_detach(spe_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 204: // accept SPE buckets
         {
            CGMP* gmp = new CGMP;
            gmp->init();

            Param5* p = new Param5;
            p->serv_instance = self;
            p->client_ip = ip;
            p->client_ctrl_port = port;
            p->dsnum = *(int32_t*)(msg->getData() + 68);
            p->path = msg->getData() + 72;
            p->filename = msg->getData() + 136;
            p->gmp = gmp;

            pthread_t spe_shuffler;
            pthread_create(&spe_shuffler, NULL, SPEShuffler, p);
            pthread_detach(spe_shuffler);

            *(int32_t*)msg->getData() = gmp->getPort();
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }
      }
   }

   delete msg;
   return NULL;
}

void Slave::report(const int32_t& transid, const string& filename)
{
   SectorMsg msg;
   msg.setType(1);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);

   SNode sn;
   sn.m_strName = filename;
   struct stat s;
   stat((m_strHomeDir + filename).c_str(), &s);
   sn.m_llTimeStamp = s.st_mtime;
   ifstream ifs((m_strHomeDir + filename).c_str());
   ifs.seekg(0, ios::end);
   sn.m_llSize = ifs.tellg();

   char buf[1024];
   sn.serialize(buf);

   msg.setData(4, buf, strlen(buf) + 1);

   cout << "report " << m_strMasterIP << " " << m_iMasterPort << " " << buf << endl;

   m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg);
}
