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
   Yunhong Gu [gu@lac.uic.edu], last updated 01/15/2009
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

   Transport::initialize();

   // init GMP
   m_GMP.init(0);
   m_iLocalPort = m_GMP.getPort();

   // initialize local directory
   m_strHomeDir = m_SysConfig.m_strHomeDir;

   // initialize slave log
   m_SectorLog.init((m_strHomeDir + ".sector.log").c_str());

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

   m_SectorLog.insert("Sector slave started.");

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
   delete [] buf;

   // move out-of-date files to the ".attic" directory
   secconn.recv((char*)&size, 4);
   if (size > 0)
   {
      buf = new char[size];
      secconn.recv(buf, size);
      ofstream left((m_strHomeDir + ".metadata/metadata.left.txt").c_str());
      left.write(buf, size);
      left.close();
      ifstream ifs((m_strHomeDir + ".metadata/metadata.left.txt").c_str());
      while (!ifs.eof())
      {
         char tmp[65536];
         ifs.getline(tmp, 65536);
         if (strlen(tmp) > 0)
            move(tmp, ".attic/");
      }
      ifs.close();

      m_SectorLog.insert("WARNING: certain files have been moved to ./attic due to conflicts.");
   }

   // calculate total available disk size
   struct statfs64 slavefs;
   statfs64(m_SysConfig.m_strHomeDir.c_str(), &slavefs);
   int64_t availdisk = slavefs.f_bfree * slavefs.f_bsize;
   secconn.send((char*)&(availdisk), 8);

   secconn.recv((char*)&m_iSlaveID, 4);

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
      if (m_GMP.recvfrom(ip, port, id, msg) < 0)
         break;

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
            m_SlaveStat.m_llAvailSize = slavefs.f_bfree * slavefs.f_bsize;
            m_SlaveStat.m_llDataSize = Index::getTotalDataSize(m_LocalFile.m_mDirectory);

            m_SlaveStat.refresh();

            msg->setData(0, (char*)&(m_SlaveStat.m_llTimeStamp), 8);
            msg->setData(8, (char*)&m_SlaveStat.m_llAvailSize, 8);
            msg->setData(16, (char*)&m_SlaveStat.m_llDataSize, 8);
            msg->setData(24, (char*)&(m_SlaveStat.m_llCurrMemUsed), 8);
            msg->setData(32, (char*)&(m_SlaveStat.m_llCurrCPUUsed), 8);
            msg->setData(40, (char*)&(m_SlaveStat.m_llTotalInputData), 8);
            msg->setData(48, (char*)&(m_SlaveStat.m_llTotalOutputData), 8);
            int size = (m_SlaveStat.m_mSysIndInput.size() + m_SlaveStat.m_mSysIndOutput.size() + m_SlaveStat.m_mCliIndInput.size() + m_SlaveStat.m_mCliIndOutput.size()) * 24 + 16;
            char* buf = new char[size];
            m_SlaveStat.serializeIOStat(buf, size);
            msg->setData(56, buf, size);
            delete [] buf;
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
            char* tmp = new char[64 + strlen(msg->getData())];
            sprintf(tmp, "created new directory %s.", msg->getData());
            m_SectorLog.insert(tmp);
            delete [] tmp;
            break;
         }

         case 104: // move dir/file
         {
            string src = msg->getData();
            string dstdir = msg->getData() + src.length() + 1;
            string newname = msg->getData() + src.length() + 1 + dstdir.length() + 1;

            m_LocalFile.move(src.c_str(), dstdir.c_str(), newname.c_str());
            move(src, dstdir + newname);

            break;
         }

         case 105: // remove dir/file
         {
            char* path = msg->getData();
            m_LocalFile.remove(path, true);
            string sysrm = string("rm -rf ") + reviseSysCmdPath(m_strHomeDir) + reviseSysCmdPath(path);
            system(sysrm.c_str());

            char* tmp = new char[64 + strlen(path)];
            sprintf(tmp, "removed directory %s.", path);
            m_SectorLog.insert(tmp);
            delete [] tmp;

            break;
         }

         case 110: // open file
         {
            cout << "===> start file server " << ip << " " << port << endl;

            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport, true, true);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = SectorMsg::m_iHdrSize + 4;
            m_GMP.sendto(ip, port, id, msg);

            // receive address information of the neighbor nodes
            m_GMP.recvfrom(ip, port, id, msg);
            if (msg->getType() != 112)
            {
               msg->setType(-112);
               msg->m_iDataLength = SectorMsg::m_iHdrSize;
               m_GMP.sendto(ip, port, id, msg);
               break;
            }

            Param2* p = new Param2;
            p->serv_instance = this;
            p->datachn = datachn;
            p->dataport = dataport;
            p->src_ip = msg->getData();
            p->src_port = *(int*)(msg->getData() + 64);
            p->dst_ip = msg->getData() + 68;
            p->dst_port = *(int*)(msg->getData() + 132);
            p->key = *(int*)(msg->getData() + 136);
            p->mode = *(int*)(msg->getData() + 140);
            p->transid = *(int*)(msg->getData() + 144);
            memcpy(p->crypto_key, msg->getData() + 148, 16);
            memcpy(p->crypto_iv, msg->getData() + 164, 8);
            p->filename = msg->getData() + 172;

            char* tmp = new char[64 + p->filename.length()];
            sprintf(tmp, "opened file %s from %s:%d.", p->filename.c_str(), p->src_ip.c_str(), p->src_port);
            m_SectorLog.insert(tmp);
            delete [] tmp;

            pthread_t file_handler;
            pthread_create(&file_handler, NULL, fileHandler, p);
            pthread_detach(file_handler);

            msg->m_iDataLength = SectorMsg::m_iHdrSize;
            m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 111: // create a replica to local
         {
            Param3* p = new Param3;
            p->serv_instance = this;
            p->timestamp = *(int64_t*)msg->getData();
            p->src = msg->getData() + 8;
            p->dst = msg->getData() + 8 + p->src.length() + 1;

            char* tmp = new char[64 + p->src.length() + p->dst.length()];
            sprintf(tmp, "created replica %s %s.", p->src.c_str(), p->dst.c_str());
            m_SectorLog.insert(tmp);
            delete [] tmp;

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

            if (datachn->open(dataport, true, false) < 0)
            {
               msg->setType(-1);
               m_GMP.sendto(ip, port, id, msg);
               break;
            }

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
            char* tmp = new char[64 + p->function.length()];
            sprintf(tmp, "starting SPE ... %d %d %s %d %d.", p->speid, p->client_data_port, p->function.c_str(), dataport, p->transid);
            m_SectorLog.insert(tmp);
            delete [] tmp;

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

            char* tmp = new char[64 + p->filename.length()];
            sprintf(tmp, "starting SPE Bucket... %s %d %d %d.", p->filename.c_str(), p->key, p->type, p->transid);
            m_SectorLog.insert(tmp);
            delete [] tmp;

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
   m_LocalFile.update(buf, addr, change);

   SectorMsg msg;
   msg.setType(1);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&m_iSlaveID, 4);
   msg.setData(8, (char*)&change, 4);
   msg.setData(12, buf, strlen(buf) + 1);

   cout << "report " << m_strMasterIP << " " << m_iMasterPort << " " << buf << endl;

   if (m_GMP.rpc(m_strMasterIP.c_str(), m_iMasterPort, &msg, &msg) < 0)
      return -1;

   if (msg.getType() < 0)
      return *(int32_t*)msg.getData();

   return 1;
}

int Slave::reportSphere(const int& transid, const vector<Address>* bad)
{
   SectorMsg msg;
   msg.setType(4);
   msg.setKey(0);
   msg.setData(0, (char*)&transid, 4);
   msg.setData(4, (char*)&m_iSlaveID, 4);

   int num = (NULL == bad) ? 0 : bad->size();
   msg.setData(8, (char*)&num, 4);
   for (int i = 0; i < num; ++ i)
   {
      msg.setData(12 + 68 * i, (*bad)[i].m_strIP.c_str(), (*bad)[i].m_strIP.length() + 1);
      msg.setData(12 + 68 * i + 64, (char*)&((*bad)[i].m_iPort), 4);
   }

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

int Slave::move(const string& src, const string& dst)
{
   string tmp = dst + src.substr(src.rfind('/'), src.length());
   createDir(tmp.substr(0, tmp.rfind('/')));
   system((string("mv ") + reviseSysCmdPath(m_strHomeDir + src) + " " + reviseSysCmdPath(m_strHomeDir + tmp)).c_str());
   return 1;
}


void SlaveStat::init()
{
   m_llStartTime = m_llTimeStamp = CTimer::getTime();
   m_llCurrMemUsed = 0;
   m_llCurrCPUUsed = 0;
   m_llTotalInputData = 0;
   m_llTotalOutputData = 0;
   m_mSysIndInput.clear();
   m_mSysIndOutput.clear();
   m_mCliIndInput.clear();
   m_mCliIndOutput.clear();

   pthread_mutex_init(&m_StatLock, 0);
}

void SlaveStat::refresh()
{
   // THIS CODE IS FOR LINUX ONLY. NOT PORTABLE

   m_llTimeStamp = CTimer::getTime();

   int pid = getpid();

   char memfile[64];
   sprintf(memfile, "/proc/%d/status", pid);

   ifstream ifs;
   ifs.open(memfile);
   char buf[1024];
   for (int i = 0; i < 12; ++ i)
      ifs.getline(buf, 1024);
   string tmp;
   ifs >> tmp;
   ifs >> m_llCurrMemUsed;
   m_llCurrMemUsed *= 1024;
   ifs.close();

   clock_t hz = sysconf(_SC_CLK_TCK);
   tms cputime;
   times(&cputime);
   m_llCurrCPUUsed = (cputime.tms_utime + cputime.tms_stime) * 1000000LL / hz;
}

void SlaveStat::updateIO(const string& ip, const int64_t& size, const int& type)
{
   pthread_mutex_lock(&m_StatLock);

   map<string, int64_t>::iterator a;

   if (type == 0)
   {
      map<string, int64_t>::iterator a = m_mSysIndInput.find(ip);
      if (a == m_mSysIndInput.end())
      {
         m_mSysIndInput[ip] = 0;
         a = m_mSysIndInput.find(ip);
      }

      a->second += size;
      m_llTotalInputData += size;
   }
   else if (type == 1)
   {
      map<string, int64_t>::iterator a = m_mSysIndOutput.find(ip);
      if (a == m_mSysIndOutput.end())
      {
         m_mSysIndOutput[ip] = 0;
         a = m_mSysIndOutput.find(ip);
      }

      a->second += size;
      m_llTotalOutputData += size;
   }
   else if (type == 2)
   {
      map<string, int64_t>::iterator a = m_mCliIndInput.find(ip);
      if (a == m_mCliIndInput.end())
      {
         m_mCliIndInput[ip] = 0;
         a = m_mCliIndInput.find(ip);
      }

      a->second += size;
      m_llTotalInputData += size;
   }
   else if (type == 3)
   {
      map<string, int64_t>::iterator a = m_mCliIndOutput.find(ip);
      if (a == m_mCliIndOutput.end())
      {
         m_mCliIndOutput[ip] = 0;
         a = m_mCliIndOutput.find(ip);
      }

      a->second += size;
      m_llTotalOutputData += size;
   }

   pthread_mutex_unlock(&m_StatLock);
}

int SlaveStat::serializeIOStat(char* buf, unsigned int size)
{
   if (size < (m_mSysIndInput.size() + m_mSysIndOutput.size() + m_mCliIndInput.size() + m_mCliIndOutput.size()) * 24 + 16)
      return -1;

   pthread_mutex_lock(&m_StatLock);

   char* p = buf;
   *(int32_t*)p = m_mSysIndInput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mSysIndInput.begin(); i != m_mSysIndInput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mSysIndOutput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mSysIndOutput.begin(); i != m_mSysIndOutput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mCliIndInput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mCliIndInput.begin(); i != m_mCliIndInput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   *(int32_t*)p = m_mCliIndOutput.size();
   p += 4;
   for (map<string, int64_t>::iterator i = m_mCliIndOutput.begin(); i != m_mCliIndOutput.end(); ++ i)
   {
      strcpy(p, i->first.c_str());
      *(int64_t*)(p + 16) = i->second;
      p += 24;
   }

   pthread_mutex_unlock(&m_StatLock);

   return (m_mSysIndInput.size() + m_mSysIndOutput.size() + m_mCliIndInput.size() + m_mCliIndOutput.size()) * 24 + 16;
}

void Slave::logError(int type, const string& ip, const int& port, const string& name)
{
   char* tmp = new char[64 + name.length()];

   switch (type)
   {
   case 1:
      sprintf(tmp, "failed to connect to file client %s:%d %s.", ip.c_str(), port, name.c_str());
      break;

   case 2:
      sprintf(tmp, "failed to connect spe client %s:%d %s.", ip.c_str(), port, name.c_str());
      break;

   case 3:
      sprintf(tmp, "failed to load spe library %s:%d %s.", ip.c_str(), port, name.c_str());
      break;

   default:
      sprintf(tmp, "unknown error.");
      break;
   }

   m_SectorLog.insert(tmp);
   delete [] tmp;
}
