/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 09/24/2007
*****************************************************************************/


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <sstream>
#include <fsclient.h>
#include <common.h>
#include <chord.h>
#include <dhash.h>
#include <center.h>
#include <server.h>

using namespace std;
using namespace cb;

Server::Server(const string& ip)
{
   m_strLocalHost = ip;
   m_pRouter = NULL;
}

Server::~Server()
{
   m_GMP.close();
   Client::close();

   delete m_pRouter;
}

int Server::init(char* ip, int port)
{
   cout << "SECTOR server built 09222007.\n";

   m_SysConfig.init("sector.conf");

   m_iLocalPort = m_SysConfig.m_iSECTORPort;
   m_GMP.init(m_iLocalPort);

   m_pRouter = new Chord;

   m_pRouter->setAppPort(m_iLocalPort);

   int res;
   if (NULL == ip)
   {
      res = m_pRouter->start(m_strLocalHost.c_str(), m_SysConfig.m_iRouterPort);
   }
   else
   {
      CCBMsg msg;
      msg.setType(8); // look up port for the router
      msg.m_iDataLength = 4;

      if ((m_GMP.rpc(ip, port, &msg, &msg) < 0) || (msg.getType() < 0))
         return -1;

      res = m_pRouter->join(m_strLocalHost.c_str(), ip, m_SysConfig.m_iRouterPort, *(int*)msg.getData());
   }
   if (res < 0)
      return -1;


   m_strHomeDir = m_SysConfig.m_strDataDir;
   //cout << "Home Dir " << m_strHomeDir << endl;
   m_SectorFS.init(m_strHomeDir);
   m_HomeDirMTime = -1;

   if (scanLocalFile() < 0)
      return -1;

   gettimeofday(&m_ReplicaCheckTime, 0);

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   Client::init(m_strLocalHost.c_str(), m_iLocalPort);

   return 1;
}

int Server::run()
{
   while (true)
   {
      updateOutLink();
      sleep(10);

      updateInLink();
      sleep(10);

      // check out link more often since it is more important
      updateOutLink();
      sleep(10);
   }

   return 1;
}

void* Server::process(void* s)
{
   Server* self = (Server*)s;

   char ip[64];
   int port;
   int32_t id;
   CCBMsg* msg = new CCBMsg;
   msg->resize(65536);

   while (true)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      //cout << "recv CB " << msg->getType() << " " << ip << " " << port << " " << msg->m_iDataLength << endl;

      switch (msg->getType())
      {
         case 1: // locate file
         {
            string filename = msg->getData();
            set<Node, NodeComp> nl;

            if (self->m_RemoteFile.lookup(filename, NULL, &nl) < 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
            }
            else
            {
               // feedback all copies of the requested file
               int num = 0;
               for (set<Node, NodeComp>::iterator i = nl.begin(); i != nl.end(); ++ i)
               {
                  msg->setData(num * 68, i->m_pcIP, strlen(i->m_pcIP) + 1);
                  msg->setData(num * 68 + 64, (char*)(&i->m_iAppPort), 4);
                  ++ num;

                  // only a limited number of nodes to be sent back
                  if (num * 68 > 65536 - 68 - 4)
                     break;
               }
               msg->m_iDataLength = 4 + num * (64 + 4);
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 2: // open file
         {
            char* filename = msg->getData();
            string dir;

            if (self->m_LocalFile.lookup(filename, dir) < 0)
               self->scanLocalFile();
            if (self->m_LocalFile.lookup(filename, dir) < 0)
            {
               // no file exist
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            cout << "OPEN FILE " << dir << " " << filename << endl;

            int mode = *(int*)(msg->getData() + 64);

            if (mode > 1)
            {
               char cert[1024];
               if (msg->m_iDataLength > 4 + 64 + 4 + 4)
                  strcpy(cert, msg->getData() + 72);
               else
               {
                  // no certificate, reject write request
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;

                  self->m_GMP.sendto(ip, port, id, msg);
                  break;
               }

               // check ownership and previlege;
               ifstream ifs;
               ifs.open((self->m_strHomeDir + "/.cert/" + filename + ".cert").c_str());
               char ecert[1024];
               ecert[0] = '\0';
               ifs.getline(ecert, 1024);

               if (0 == strlen(ecert))
               {
                  // read only file
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;

                  self->m_GMP.sendto(ip, port, id, msg);
                  break;
               }

               unsigned char sha[SHA_DIGEST_LENGTH + 1];
               SHA1((const unsigned char*)cert, strlen(cert), sha);
               sha[SHA_DIGEST_LENGTH] = '\0';
               stringstream shastr(stringstream::in | stringstream::out);
               for (int i = 0; i < SHA_DIGEST_LENGTH; i += 4)
                  shastr << *(int32_t*)(sha + i);

               if (shastr.str() != string(ecert))
               {
                  // certificate do not match!
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;

                  self->m_GMP.sendto(ip, port, id, msg);
                  break;
               }
            }

            self->m_AccessLog.insert(ip, port, msg->getData());

            cout << "===> start file server " << ip << " " << port << endl;

            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport);

            Param2* p = new Param2;
            p->serv_instance = self;
            p->filename = msg->getData();
            p->datachn = datachn;
            p->client_ip = ip;
            p->client_data_port = *(int*)(msg->getData() + 68);
            p->mode = mode;

            pthread_t file_handler;
            pthread_create(&file_handler, NULL, fileHandler, p);
            pthread_detach(file_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = 4 + 4;

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 3: // add a new file
         {
            CFileAttr attr;
            attr.deserialize(msg->getData(), msg->m_iDataLength - 4);
            Node n;
            strcpy(n.m_pcIP, ip);
            n.m_iAppPort = port;

            if (self->m_RemoteFile.insert(attr, n) < 0)
               msg->setType(-msg->getType());

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 4: // lookup a file server
         {
            string filename = msg->getData();
            int fid = DHash::hash(filename.c_str(), m_iKeySpace);
            int r = self->m_pRouter->lookup(fid, (Node*)msg->getData());

            if (-1 == r)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
            }
            else
            {
               msg->m_iDataLength = 4 + sizeof(Node);
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 5: // create a local file
         {
            string filename = msg->getData();
            string dir;

            if ((self->m_LocalFile.lookup(filename, dir) > 0) || (!self->m_SysConfig.m_IPSec.checkIP(ip)))
            {
               // file already exist, or not from an allowed IP
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            CFileAttr attr;
            strcpy(attr.m_pcName, filename.c_str());
            attr.m_llTimeStamp = CTimer::getTime();
            attr.m_uiID = DHash::hash(attr.m_pcName, m_iKeySpace);

            dir = ".sector-fs/";
            self->m_SectorFS.create(attr.m_pcName, attr.m_uiID, dir);
            self->m_LocalFile.insert(attr, dir);

            // generate certificate for the file owner
            char cert[1024];
            timeval t;
            gettimeofday(&t, 0);
            srand(t.tv_sec);
            sprintf(cert, "%s %d %s %d%d%d%d%d", ip, port, filename.c_str(), rand(), rand(), rand(), rand(), rand());

            unsigned char sha[SHA_DIGEST_LENGTH + 1];
            SHA1((const unsigned char*)cert, strlen(cert), sha);
            sha[SHA_DIGEST_LENGTH] = '\0';

            DIR* test = opendir((self->m_strHomeDir + ".cert").c_str());
            if (NULL == test)
            {
               if ((errno != ENOENT) || (mkdir((self->m_strHomeDir + ".cert").c_str(), S_IRWXU) < 0))
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;

                  self->m_GMP.sendto(ip, port, id, msg);
                  break;
               }
            }
            closedir(test);

            ofstream cf((self->m_strHomeDir + ".cert/" + filename + ".cert").c_str());
            for (int i = 0; i < SHA_DIGEST_LENGTH; i += 4)
               cf << *(int32_t*)(sha + i);
            cf.close();

            msg->setData(0, cert, strlen(cert) + 1);
            msg->m_iDataLength = 4 + strlen(cert) + 1;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 6: // probe the existence of a file
         {
            char* fl = new char [msg->m_iDataLength - 4];
            memcpy(fl, msg->getData(), msg->m_iDataLength - 4);

            int c = 0;
            string dir;
            for (int i = 0; i < (msg->m_iDataLength - 4) / 64; ++ i)
            {
               if (self->m_LocalFile.lookup(fl + i * 64, dir) < 0)
               {
                  msg->setData(c * 64, fl + i * 64, strlen(fl + i * 64) + 1);
                  ++ c;
               }
            }
            delete [] fl;

            msg->m_iDataLength = 4 + c * 64;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 7: // check query from RemoteFileIndex
         {
            char* fl = new char [msg->m_iDataLength - 4];
            memcpy(fl, msg->getData(), msg->m_iDataLength - 4);

            int c = 0;
            for (int i = 0; i < (msg->m_iDataLength - 4) / 64; ++ i)
            {
               if (!self->m_pRouter->has(DHash::hash(fl + i * 64, m_iKeySpace)))
               {
                  self->m_RemoteFile.remove(fl + i * 64);
                  msg->setData(c * 64, fl + i * 64, strlen(fl + i * 64) + 1);
                  ++ c;
               }
               else
               {
                  Node n;
                  strcpy(n.m_pcIP, ip);
                  n.m_iAppPort = port;
                  if (!self->m_RemoteFile.check(fl + i * 64, n))
                  {
                     msg->setData(c * 64, fl + i * 64, strlen(fl + i * 64) + 1);
                     ++ c;
                  }
               }
            }
            delete [] fl;

            msg->m_iDataLength = 4 + c * 64;
            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 8: // server join request router port number
         {
            *(int*)(msg->getData()) = self->m_SysConfig.m_iRouterPort;
            msg->m_iDataLength = 4 + 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 9: // server information
         {
            NodeInfo* ni = (NodeInfo*)msg->getData();
            ni->m_iStatus = 1;
            ni->m_iSPEMem = self->m_SysConfig.m_iMaxSPEMem;

            msg->m_iDataLength = 4 + sizeof(NodeInfo);

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 11: // create a replica
         {
            string filename = msg->getData() + 8;
            string dir;

            //check if file already exists!
            if (self->m_LocalFile.lookup(filename, dir) > 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
               break;
            }

            // check if there is enough local disk space
            if (*(int*)msg->getData() > self->m_SysConfig.m_llMaxDataSize - KnowledgeBase::getTotalDataSize(self->m_SysConfig.m_strDataDir))
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
               break;
            }

            Param1* p = new Param1;
            p->serv_instance = self;
            p->msg = new CCBMsg(*msg);

            pthread_t rep;
            pthread_create(&rep, NULL, createReplica, p);
            pthread_detach(rep);

            msg->m_iDataLength = 4;

            break;
         }

         case 300: // processing engine
         {
            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport);

            Param4* p = new Param4;
            p->serv_instance = self;
            p->datachn = datachn;
            p->client_ip = ip;
            p->client_ctrl_port = port;
            p->speid = *(int32_t*)msg->getData();
            p->client_data_port = *(int32_t*)(msg->getData() + 4);
            p->function = msg->getData() + 8;
            p->rows = *(int32_t*)(msg->getData() + 72);
            p->psize = msg->m_iDataLength - 76;
            if (p->psize > 0)
            {
               p->param = new char[p->psize];
               memcpy(p->param, msg->getData() + 76, p->psize);
            }

            cout << "starting SPE ... " << p->speid << " " << p->client_data_port << " " << p->function << endl;

            pthread_t spe_handler;
            pthread_create(&spe_handler, NULL, SPEHandler, p);
            pthread_detach(spe_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = 4 + 4;

            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         case 301: // accept SPE buckets
         {
            CGMP* gmp = new CGMP;
            gmp->init();

            Param5* p = new Param5;
            p->serv_instance = self;
            p->client_ip = ip;
            p->client_ctrl_port = port;
            p->dsnum = *(int32_t*)msg->getData();
            p->filename = msg->getData() + 4;
            p->gmp = gmp;

            pthread_t spe_shuffler;
            pthread_create(&spe_shuffler, NULL, SPEShuffler, p);
            pthread_detach(spe_shuffler);

            *(int32_t*)msg->getData() = gmp->getPort();
            msg->m_iDataLength = 4 + 4;
            self->m_GMP.sendto(ip, port, id, msg);

            break;
         }

         default:
         {
            Param1* p = new Param1;
            p->serv_instance = self;
            p->client_ip = ip;
            p->msg_id = id;
            p->client_ctrl_port = port;
            p->msg = new CCBMsg(*msg);

            pthread_t ex_thread;
            pthread_create(&ex_thread, NULL, processEx, p);
            pthread_detach(ex_thread);

            break;
         }
      }
   }

   delete msg;
   return NULL;
}

void* Server::processEx(void* p)
{
   Server* self = ((Param1*)p)->serv_instance;
   string ip = ((Param1*)p)->client_ip;
   int port = ((Param1*)p)->client_ctrl_port;
   int32_t id = ((Param1*)p)->msg_id;
   CCBMsg* msg = ((Param1*)p)->msg;
   delete (Param1*)p;

   //cout << "recv request " << msg->getType() << endl;

   switch (msg->getType())
   {
      case 101: // stat
      {
         string filename = msg->getData();
         int fid = DHash::hash(filename.c_str(), m_iKeySpace);
         Node n;

         int r = self->m_pRouter->lookup(fid, &n);

         if (-1 == r)
         {
            msg->setType(-msg->getType());
            msg->m_iDataLength = 4;
         }
         else
         {
            if ((self->m_strLocalHost == n.m_pcIP) && (self->m_iLocalPort == n.m_iAppPort))
            {
               CFileAttr attr;
               if (self->m_RemoteFile.lookup(filename, &attr) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
               else
               {
                  attr.serialize(msg->getData(), msg->m_iDataLength);
                  msg->m_iDataLength += 4;
               }

               cout << "syn " << filename << " " << msg->getType() << " " << msg->m_iDataLength - 4 << " " << attr.m_llSize << endl;
            }
            else
            {
               if (self->m_GMP.rpc(n.m_pcIP, n.m_iAppPort, msg, msg) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
            }
         }

         break;
      }

      default:
         msg->setType(-msg->getType());
         msg->m_iDataLength = 4;
         self->m_GMP.sendto(ip.c_str(), port, id, msg);

         break;
   }

    self->m_GMP.sendto(ip.c_str(), port, id, msg);

   //cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

   delete msg;

   return NULL;
}

void* Server::createReplica(void* p)
{
   Server* self = ((Param1*)p)->serv_instance;
   CCBMsg* msg = ((Param1*)p)->msg;
   delete (Param1*)p;

   string filename = msg->getData() + 8;
   delete msg;

   CFileAttr attr;
   strcpy(attr.m_pcName, filename.c_str());
   attr.m_llTimeStamp = CTimer::getTime();
   attr.m_uiID = DHash::hash(attr.m_pcName, m_iKeySpace);

   string dir = ".sector-fs/";
   self->m_SectorFS.create(filename, attr.m_uiID, dir);
   self->m_LocalFile.insert(attr, dir);

   File* f = Client::createFileHandle();
   if (f->open(filename) > 0)
      f->download((self->m_strHomeDir + dir + filename).c_str());
   f->close();
   Client::releaseFileHandle(f);

   return NULL;
}

void Server::updateOutLink()
{
   scanLocalFile();

   map<Node, set<string>, NodeComp> li;
   m_LocalFile.getLocIndex(li);

   CCBMsg msg;
   msg.resize(65536);

   for (map<Node, set<string>, NodeComp>::iterator i = li.begin(); i != li.end(); ++ i)
   {
      timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = 10000000;
      nanosleep(&ts, NULL);

      // ask remote if it is the right node to hold the metadata for these files
      msg.setType(7);

      int c = 0;
      for (set<string>::iterator f = i->second.begin(); f != i->second.end(); ++ f)
      {
         msg.setData(c * 64, f->c_str(), f->length() + 1);
         ++ c;
      }
      msg.m_iDataLength = 4 + c * 64;

      if (m_GMP.rpc(i->first.m_pcIP, i->first.m_iAppPort, &msg, &msg) >= 0)
         c = (msg.m_iDataLength - 4) / 64;

      for (int m = 0; m < c; ++ m)
      {
         char* filename = msg.getData() + m * 64;
         Node loc;
         int fid = DHash::hash(filename, m_iKeySpace);
         if (-1 == m_pRouter->lookup(fid, &loc))
            continue;

         m_LocalFile.updateNameServer(filename, loc);

         // send metadata to a new node
         CCBMsg msg3;
         msg3.setType(3);
         CFileAttr attr;
         string dir;
         m_LocalFile.lookup(filename, dir, &attr);
         attr.serialize(msg3.getData(), msg3.m_iDataLength);
         msg3.m_iDataLength += 4;
         m_GMP.rpc(loc.m_pcIP, loc.m_iAppPort, &msg3, &msg3);
      }
   }
}

void Server::updateInLink()
{
   map<Node, set<string>, NodeComp> li;
   m_RemoteFile.getLocIndex(li);

   CCBMsg msg;
   msg.resize(65536);

   for (map<Node, set<string>, NodeComp>::iterator i = li.begin(); i != li.end(); ++ i)
   {
      timespec ts;
      ts.tv_sec = 0;
      ts.tv_nsec = 10000000;
      nanosleep(&ts, NULL);

      // check if the original file still exists
      int c = 0;
      for (set<string>::iterator f = i->second.begin(); f != i->second.end(); ++ f)
      {
         msg.setData(c * 64, f->c_str(), f->length() + 1);
         ++ c;
      }

      msg.setType(6);
      msg.m_iDataLength = 4 + c * 64;
      int r = m_GMP.rpc(i->first.m_pcIP, i->first.m_iAppPort, &msg, &msg);

      if (r < 0)
         m_RemoteFile.remove(i->first);
      else
      {
         for (c = 0; c < (msg.m_iDataLength - 4) / 64; ++ c)
            m_RemoteFile.removeCopy(msg.getData() + c * 64, i->first);
      }
   }

   /*
   timeval currtime;
   gettimeofday(&currtime, 0);
   if (currtime.tv_sec - m_ReplicaCheckTime.tv_sec > 24 * 3600)
   {
      // less than 2 copies in the system, create a new one

      map<string, int> ri;
      m_RemoteFile.getReplicaInfo(ri, 2);

      for (map<string, int>::const_iterator i = ri.begin(); i != ri.end(); ++ i)
      {
         int seed = 1 + (int)(10.0 * rand() / (RAND_MAX + 1.0));
         Node n;
         if (m_pRouter->lookup(seed, &n) < 0)
            continue;

         CFileAttr attr;
         m_RemoteFile.lookup(i->first, &attr);

         msg.setType(11);
         msg.setData(0, (char*)&(attr.m_llSize), 8);
         msg.setData(8, attr.m_pcName, strlen(attr.m_pcName) + 1);
         msg.m_iDataLength = 4 + 8 + strlen(attr.m_pcName) + 1;

         m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg);
      }
   }
   */
}

int Server::scanLocalFile()
{
   struct stat s;
   stat(m_strHomeDir.c_str(), &s);
   if (m_HomeDirMTime != s.st_mtime)
      m_HomeDirMTime = s.st_mtime;
   else
      return 0;

   vector<string> filelist;
   vector<string> dirs;
   m_SectorFS.scan(filelist, dirs, "");

   // check deleted files
   set<string> lfindex;
   m_LocalFile.getFileList(lfindex);
   set<string> sortlist;
   for (vector<string>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
      sortlist.insert(*i);
   for (set<string>::iterator i = lfindex.begin(); i != lfindex.end(); ++ i)
   {
      if (sortlist.find(*i) == sortlist.end())
         m_LocalFile.remove(*i);
   }

   // check new files on disk
   for (unsigned int i = 0; i < filelist.size(); ++ i)
   {
      string tmp;
      if (m_LocalFile.lookup(filelist[i], tmp) < 0)
      {
         ifstream ifs((m_strHomeDir + dirs[i] + filelist[i]).c_str());
         ifs.seekg(0, ios::end);
         int64_t size = ifs.tellg();
         ifs.close();

         CFileAttr attr;
         strcpy(attr.m_pcName, filelist[i].c_str());
         // original file is read only
         attr.m_iAttr = 1;
         attr.m_llSize = size;

         struct stat s;
         stat((m_strHomeDir + dirs[i] + filelist[i]).c_str(), &s);
         attr.m_llTimeStamp = (int64_t)s.st_mtime * 1000000;

         m_LocalFile.insert(attr, dirs[i]);

         cout << "add local file... " << dirs[i] << " " << filelist[i] << " " << size << endl;
      }
   }

   return 1;
}
