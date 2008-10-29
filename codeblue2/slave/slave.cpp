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
   Yunhong Gu [gu@lac.uic.edu], last updated 10/28/2008
*****************************************************************************/


#include "slave.h"
#include <ssltransport.h>
#include <iostream>
#include <dirent.h>
#include <netdb.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <sys/times.h>
#include "../udt/common.h"

using namespace std;

Slave::Slave()
{
   m_strBase = "./";
}

Slave::~Slave()
{
}

int Slave::init(const char* base)
{
   if (NULL != base)
      m_strBase = base;

   string conf = m_strBase + "/slave.conf";
   if (m_SysConfig.init(conf) < 0)
   {
      cerr << "unable to initialize from configuration file; quit.\n";
      return -1;
   }

   // obtain master IP address
   m_strMasterHost = m_SysConfig.m_strMasterHost;
   struct hostent* masterip = gethostbyname(m_strMasterHost.c_str());
   if (NULL == masterip)
   {
      cerr << "invalid master address " << m_strMasterHost << endl;
      return -1;
   }
   char buf[64];
   m_strMasterIP = inet_ntop(AF_INET, masterip->h_addr_list[0], buf, 64);
   m_iMasterPort = m_SysConfig.m_iMasterPort;

   // init GMP
   m_GMP.init(0);
   m_iLocalPort = m_GMP.getPort();

   // initialize local directory
   m_strHomeDir = m_SysConfig.m_strHomeDir;

   // check local directory
   if (createSysDir() < 0)
   {
      cerr << "unable to create system directory " << m_strHomeDir << endl;
      return -1;
   }

   cout << "scaning " << m_strHomeDir << endl;
   m_LocalFile.scan(m_strHomeDir.c_str(), m_LocalFile.m_mDirectory);

   ofstream ofs((m_strHomeDir + ".metadata/metadata.txt").c_str());
   m_LocalFile.serialize(ofs, m_LocalFile.m_mDirectory, 1);
   ofs.close();

   return 1;
}

int Slave::connect()
{
   // join the server
   SSLTransport::init();

   string cert = m_strBase + "/master_node.cert";

   SSLTransport secconn;
   secconn.initClientCTX(cert.c_str());
   secconn.open(NULL, 0);
   int r = secconn.connect(m_strMasterHost.c_str(), m_iMasterPort);

   if (r < 0)
   {
      cerr << "unable to set up secure channel to the master.\n";
      return -1;
   }

   secconn.getLocalIP(m_strLocalHost);

   int cmd = 1;
   secconn.send((char*)&cmd, 4);
   int res = -1;
   secconn.recv((char*)&res, 4);

   if (res < 0)
   {
      cerr << "security check failed.\n";
      return -1;
   }

   secconn.send((char*)&m_iLocalPort, 4);

   struct stat s;
   stat((m_strHomeDir + ".metadata/metadata.txt").c_str(), &s);
   int32_t size = s.st_size;

   ifstream meta((m_strHomeDir + ".metadata/metadata.txt").c_str());
   char* buf = new char[size];
   meta.read(buf, size);
   meta.close();

   secconn.send((char*)&size, 4);
   secconn.send(buf, size);

   // calculate total available disk size
   struct statfs64 slavefs;
   statfs64(m_SysConfig.m_strHomeDir.c_str(), &slavefs);
   int64_t availdisk = slavefs.f_bfree * slavefs.f_bsize;
   secconn.send((char*)&(availdisk), 8);

   secconn.close();
   SSLTransport::destroy();

   // initialize slave statistics
   m_SlaveStat.init();

   cout << "Slave initialized. Running.\n";

   return 1;
}

void Slave::run()
{
   char ip[64];
   int port;
   int32_t id;
   SectorMsg* msg = new SectorMsg;
   msg->resize(65536);

   cout << "slave process " << m_iLocalPort << endl;

   while (true)
   {
      m_GMP.recvfrom(ip, port, id, msg);

      cout << "recv cmd " << ip << " " << port << " type " << msg->getType() << endl;

      // a slave only accepts commands from the master
      if ((m_strMasterIP != ip) || (m_iMasterPort != port))
          continue;

      switch (msg->getType())
      {
         case 1: // probe
         {
            // calculate total available disk size
            struct statfs64 slavefs;
            statfs64(m_SysConfig.m_strHomeDir.c_str(), &slavefs);
            int64_t availdisk = slavefs.f_bfree * slavefs.f_bsize;

            m_SlaveStat.refresh();

            msg->setData(0, (char*)&availdisk, 8);
            msg->setData(8, (char*)&(m_SlaveStat.m_llCurrMemUsed), 8);
            msg->setData(16, (char*)&(m_SlaveStat.m_llCurrCPUUsed), 8);
            m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 3: // stop
         {
            // stop the slave node
            break;
         }

         case 103: // mkdir
         {
            createDir(msg->getData());

            break;
         }

         case 104: // move dir/file
         {
            char* oldpath = msg->getData();
            char* newpath = msg->getData() + 64;
            m_LocalFile.move(oldpath, newpath);
            ::rename((m_strHomeDir + oldpath).c_str(), (m_strHomeDir + newpath).c_str());

            break;
         }

         case 105: // remove dir/file
         {
            cout << "REMOVE  " << m_strHomeDir + msg->getData() << endl;
            char* path = msg->getData();
            m_LocalFile.remove(path, true);
            string sysrm = string("rm -rf ") + reviseSysCmdPath(m_strHomeDir) + reviseSysCmdPath(path);
            system(sysrm.c_str());

            break;
         }

         case 110: // open file
         {
            cout << "===> start file server " << ip << " " << port << endl;

            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport, true, false);

            Param2* p = new Param2;
            p->serv_instance = this;
            p->datachn = datachn;
            p->client_ip = msg->getData();
            p->client_data_port = *(int*)(msg->getData() + 64);
            p->mode = *(int*)(msg->getData() + 68);
            p->transid = *(int*)(msg->getData() + 72);
            p->filename = msg->getData() + 76;

            pthread_t file_handler;
            pthread_create(&file_handler, NULL, fileHandler, p);
            pthread_detach(file_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;

            m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 111: // create a replica to local
         {
            Param3* p = new Param3;
            p->serv_instance = this;
            p->timestamp = *(int64_t*)msg->getData();
            p->filename = msg->getData() + 8;

            pthread_t replica_handler;
            pthread_create(&replica_handler, NULL, copy, p);
            pthread_detach(replica_handler);

            m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 203: // processing engine
         {
            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport, true, false);

            Param4* p = new Param4;
            p->serv_instance = this;
            p->datachn = datachn;
            p->client_ip = msg->getData();
            p->client_ctrl_port = *(int32_t*)(msg->getData() + 64);
            p->speid = *(int32_t*)(msg->getData() + 68);
            p->client_data_port = *(int32_t*)(msg->getData() + 72);
            p->key = *(int32_t*)(msg->getData() + 76);
            p->function = msg->getData() + 80;
            int offset = 80 + p->function.length() + 1;
            p->rows = *(int32_t*)(msg->getData() + offset);
            p->psize = *(int32_t*)(msg->getData() + offset + 4);
            if (p->psize > 0)
            {
               p->param = new char[p->psize];
               memcpy(p->param, msg->getData() + offset + 8, p->psize);
            }
            else
               p->param = NULL;
            p->type = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 8);
            p->transid = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 4);

            cout << "starting SPE ... " << p->speid << " " << p->client_data_port << " " << p->function << " " << dataport << " " << p->transid << endl;

            pthread_t spe_handler;
            pthread_create(&spe_handler, NULL, SPEHandler, p);
            pthread_detach(spe_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;

            m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 204: // accept SPE buckets
         {
            CGMP* gmp = new CGMP;
            gmp->init();

            Param5* p = new Param5;
            p->serv_instance = this;
            p->client_ip = msg->getData();
            p->client_ctrl_port = *(int32_t*)(msg->getData() + 64);
            p->bucketnum = *(int32_t*)(msg->getData() + 68);
            p->bucketid = *(int32_t*)(msg->getData() + 72);
            p->path = msg->getData() + 80;
            int offset = 80 + p->path.length() + 1 + 4;

            p->filename = msg->getData() + offset;
            p->gmp = gmp;

            offset += p->filename.length() + 1;

            p->key = *(int32_t*)(msg->getData() + offset);
            p->type = *(int32_t*)(msg->getData() + offset + 4);
            if (p->type == 1)
            {
               p->function = msg->getData() + offset + 4 + 4 + 4;
            }

            p->transid = *(int32_t*)(msg->getData() + msg->m_iDataLength - SectorMsg::m_iHdrSize - 4);

            pthread_t spe_shuffler;
            pthread_create(&spe_shuffler, NULL, SPEShuffler, p);
            pthread_detach(spe_shuffler);

            *(int32_t*)msg->getData() = gmp->getPort();
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
            m_GMP.sendto(ip, port, id, msg);

            break;
         }
      }
   }

   delete msg;
}

int Slave::report(const int32_t& transid, const string& filename, const int& change)
{
   struct stat s;
   if (-1 == stat((m_strHomeDir + filename).c_str(), &s))
      return -1;

   SNode sn;
   sn.m_strName = filename;
   sn.m_bIsDir = 0;
   sn.m_llTimeStamp = s.st_mtime;
   ifstream ifs((m_strHomeDir + filename).c_str());
   ifs.seekg(0, ios::end);
   sn.m_llSize = ifs.tellg();

   char buf[1024];
   sn.serialize(buf);

   //update local
   Address addr;
   addr.m_strIP = "127.0.0.1";
   addr.m_iPort = 0;
   set<Address, AddrComp> nothing;
   m_LocalFile.update(buf, addr, change, nothing);

   SectorMsg msg;
   msg.setType(1);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&change, 4);
   msg.setData(8, buf, strlen(buf) + 1);

   cout << "report " << m_strMasterIP << " " << m_iMasterPort << " " << buf << endl;

   if (m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)msg.getData();

   return 1;
}

int Slave::reportSphere(const int& transid)
{
   SectorMsg msg;
   msg.setType(1);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);

   cout << "reportSphere " << m_strMasterIP << " " << m_iMasterPort << " " << transid << endl;

   if (m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)msg.getData();

   return 1;
}

int Slave::createDir(const string& path)
{
   vector<string> dir;
   Index::parsePath(path.c_str(), dir);

   string currpath = m_strHomeDir;
   for (vector<string>::iterator i = dir.begin(); i != dir.end(); ++ i)
   {
      currpath += *i;
      if ((-1 == ::mkdir(currpath.c_str(), S_IRWXU)) && (errno != EEXIST))
         return -1;
      currpath += "/";
   }

   return 1;
}

int Slave::createSysDir()
{
   // check local directory
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
         return -1;
   }
   closedir(test);

   test = opendir((m_strHomeDir + ".sphere").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".sphere").c_str(), S_IRWXU) < 0))
         return -1;
   }
   closedir(test);
   system(("rm -rf " + reviseSysCmdPath(m_strHomeDir) + ".sphere/*").c_str());

   test = opendir((m_strHomeDir + ".tmp").c_str());
   if (NULL == test)
   {
      if ((errno != ENOENT) || (mkdir((m_strHomeDir + ".tmp").c_str(), S_IRWXU) < 0))
         return -1;
   }
   closedir(test);
   system(("rm -rf " + reviseSysCmdPath(m_strHomeDir) + ".tmp/*").c_str());

   return 0;
}

string Slave::reviseSysCmdPath(const string& path)
{
   string rpath;
   for (const char *p = path.c_str(), *q = path.c_str() + path.length(); p != q; ++ p)
   {
      if ((*p == 32) || (*p == 34) || (*p == 38) || (*p == 39))
         rpath.append(1, char(92));
      rpath.append(1, *p);
   }

   return rpath;
}

void SlaveStat::init()
{
   m_llTimeStamp = CTimer::getTime();
   m_llCurrMemUsed = 0;
   m_llLastCPUTime = 0;
   m_llCurrCPUUsed = 0;
   m_llTotalInputData = 0;
   m_llTotalOutputData = 0;
   m_mIndInput.clear();
   m_mIndOutput.clear();
}

void SlaveStat::refresh()
{
   // LINUX only

   m_llTimeStamp = CTimer::getTime();

   int pid = getpid();

   char memfile[64];
   char cpufile[64];
   sprintf(memfile, "/proc/%d/statm", pid);
   sprintf(cpufile, "/proc/%d/stat", pid);

   ifstream ifs;
   ifs.open(memfile);
   ifs >> m_llCurrMemUsed;
   ifs.close();

   /*
   ifs.open(cpufile);
   string tmp;
   for (int i = 0; i < 14; ++ i)
      ifs >> tmp;
   int stime = atoi(tmp.c_str());
   ifs >> tmp;
   int utime = atoi(tmp.c_str());
   m_llCPUUsed = (stime + utime) / 
   ifs.close();
   */

   clock_t hz = sysconf(_SC_CLK_TCK);
   tms cputime;
   times(&cputime);
   m_llLastCPUTime += m_llCurrCPUUsed;
   m_llCurrCPUUsed = (cputime.tms_utime + cputime.tms_stime) / hz - m_llLastCPUTime;
}
