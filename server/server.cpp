/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 51
Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 06/25/2007
*****************************************************************************/


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <sys/sendfile.h>
#include <server.h>
#include <assert.h>
#include <sstream>
#include <signal.h>
#include <util.h>
#include <data.h>
#include <sql.h>
#include <table.h>
#include <fsclient.h>

using namespace std;
using namespace cb;

Server::Server(const string& ip)
{
   m_strLocalHost = ip;
}

Server::~Server()
{
   m_GMP.close();
   Client::close();
}

int Server::init(char* ip, int port)
{
   m_SysConfig.init("sector.conf");

   m_iLocalPort = m_SysConfig.m_iSECTORPort;
   m_GMP.init(m_iLocalPort);
   m_Router.setAppPort(m_iLocalPort);

   int res;
   if (NULL == ip)
   {
      res = m_Router.start(m_strLocalHost.c_str(), m_SysConfig.m_iRouterPort);
   }
   else
   {
      CCBMsg msg;
      msg.setType(8); // look up port for the router
      msg.m_iDataLength = 4;

      if ((m_GMP.rpc(ip, port, &msg, &msg) < 0) || (msg.getType() < 0))
         return -1;

      res = m_Router.join(m_strLocalHost.c_str(), ip, m_SysConfig.m_iRouterPort, *(int*)msg.getData());
   }
   if (res < 0)
      return -1;

   if (scanLocalFile() < 0)
      return -1;

   struct stat s;
   stat(m_strHomeDir.c_str(), &s);
   m_HomeDirMTime = s.st_mtime;

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, process, this);
   pthread_detach(msgserver);

   // ignore TCP broken pipe
   signal(SIGPIPE, SIG_IGN);

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

            if (self->m_LocalFile.lookup(filename) < 0)
            {
               // no file exist
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

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
            int r = self->m_Router.lookup(fid, (Node*)msg->getData());

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

            if ((self->m_LocalFile.lookup(filename) > 0) || (!self->m_SysConfig.m_IPSec.checkIP(ip)))
            {
               // file already exist, or not from an allowed IP
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            string file = self->m_strHomeDir + filename;

            ofstream fs;
            fs.open(file.c_str());
            fs.close();

            CFileAttr attr;
            strcpy(attr.m_pcName, filename.c_str());
            attr.m_llTimeStamp = Time::getTime();

            self->m_LocalFile.insert(attr);

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
            for (int i ; i < (msg->m_iDataLength - 4) / 64; ++ i)
            {
               if (self->m_LocalFile.lookup(fl + i * 64) < 0)
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
               if (!self->m_Router.has(DHash::hash(fl + i * 64, m_iKeySpace)))
               {
                  self->m_RemoteFile.remove(fl + i * 64);
                  msg->setData(c * 64, fl + i * 64, strlen(fl + i * 64) + 1);
                  ++ c;
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

         case 200: // open a SQL connection
         {
            char* filename = msg->getData() + 4;

            if (self->m_LocalFile.lookup(filename) < 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            self->m_AccessLog.insert(ip, port, msg->getData());

            cout << "===> start SQL server " << endl;

            Transport* datachn = new Transport;
            int dataport = 0;
            datachn->open(dataport);

            Param3* p = new Param3;
            p->serv_instance = self;
            p->filename = msg->getData() + 4;
            p->query = msg->getData() + 68;
            p->datachn = datachn;
            p->client_ip = ip;
            p->client_data_port = *(int*)(msg->getData());

            pthread_t sql_handler;
            pthread_create(&sql_handler, NULL, SQLHandler, p);
            pthread_detach(sql_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = 4 + 4;

            self->m_GMP.sendto(ip, port, id, msg);

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
            p->buckets = *(int32_t*)(msg->getData() + 76);
            p->param = NULL;
            if (p->buckets > 0)
            {
               p->locations = new char[p->buckets * 72];
               memcpy(p->locations, msg->getData() + 80, p->buckets * 72);
               p->psize = msg->m_iDataLength - 80 - p->buckets * 72;
               if (p->psize > 0)
               {
                  p->param = new char[p->psize];
                  memcpy(p->param, msg->getData() + 80 + p->buckets * 72, p->psize);
               }
            }
            else
            {
               p->locations = NULL;
               p->psize = msg->m_iDataLength - 80;
               if (p->psize > 0)
               {
                  p->param = new char[p->psize];
                  memcpy(p->param, msg->getData() + 80, p->psize);
               }
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

   //cout << "recv request " << msg->getType() << endl;

   switch (msg->getType())
   {
      case 11:
      {
         string filename = msg->getData() + 8;

         //check if file already exists!
         if (self->m_LocalFile.lookup(filename) > 0)
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

         File* f = Client::createFileHandle();
         if (f->open(filename) < 0)
            msg->setType(-msg->getType());
         else if (f->download((self->m_strHomeDir + filename).c_str()) < 0)
            msg->setType(-msg->getType());
         f->close();
         Client::releaseFileHandle(f);

         msg->m_iDataLength = 4;

         break;
      }

      case 101: // stat
      {
         string filename = msg->getData();
         int fid = DHash::hash(filename.c_str(), m_iKeySpace);
         Node n;

         int r = self->m_Router.lookup(fid, &n);

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

      case 201: //semantics
      {
         string filename = msg->getData();
         int fid = DHash::hash(filename.c_str(), m_iKeySpace);
         Node n;

         if (self->m_LocalFile.lookup(filename) >= 0)
         {
            vector<DataAttr> attr;
            string tmp;
            Semantics::loadSemantics(self->m_strHomeDir + filename + ".sem", attr);
            Semantics::serialize(tmp, attr);
            memcpy(msg->getData(), tmp.c_str(), tmp.length());
            msg->m_iDataLength = 4 + tmp.length();

            break;
         }

         int r = self->m_Router.lookup(fid, &n);

         if (-1 == r)
         {
            msg->setType(-msg->getType());
            msg->m_iDataLength = 4;
         }
         else
         {
            if (self->m_strLocalHost == n.m_pcIP)
            {
               set<Node, NodeComp> nl;
               if (self->m_RemoteFile.lookup(filename, NULL, &nl) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
               else
               {
                  if (self->m_GMP.rpc(nl.begin()->m_pcIP, nl.begin()->m_iAppPort, msg, msg) < 0)
                  {
                     msg->setType(-msg->getType());
                     msg->m_iDataLength = 4;
                  }
               }
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
         break;
   }

   self->m_GMP.sendto(ip.c_str(), port, id, msg);

   //cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

   delete msg;
   delete (Param1*)p;

   return NULL;
}

void Server::updateOutLink()
{
   struct stat s;
   stat(m_strHomeDir.c_str(), &s);
   if (m_HomeDirMTime != s.st_mtime)
   {
      scanLocalFile();
      m_HomeDirMTime = s.st_mtime;
   }

   map<Node, set<string>, NodeComp> li;
   m_LocalFile.getLocIndex(li);

   CCBMsg msg;
   msg.resize(65536);

   for (map<Node, set<string>, NodeComp>::iterator i = li.begin(); i != li.end(); ++ i)
   {
      usleep(1000);

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
         if (-1 == m_Router.lookup(fid, &loc))
            continue;

         m_LocalFile.updateNameServer(filename, loc);

         // send metadata to a new node
         CCBMsg msg3;
         msg3.setType(3);
         CFileAttr attr;
         m_LocalFile.lookup(filename, &attr);
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

   //Node loc;

   for (map<Node, set<string>, NodeComp>::iterator i = li.begin(); i != li.end(); ++ i)
   {
      usleep(1000);

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
      {
         cout << "RPC FAIL " << i->first.m_pcIP << endl;
         m_RemoteFile.remove(i->first);
      }
      else
      {
         for (c = 0; c < (msg.m_iDataLength - 4) / 64; ++ c)
            m_RemoteFile.removeCopy(msg.getData() + c * 64, i->first);
      }

      // less than 2 copies in the system, create a new one
      // TODO: start a timeout before making a copy
   }
}

int Server::scanLocalFile()
{
   m_strHomeDir = m_SysConfig.m_strDataDir;
   //cout << "Home Dir " << m_strHomeDir << endl;

   CFileAttr attr;

   // initialize all files in the home directory, excluding "." and ".."
   dirent **namelist;
   int n = scandir(m_strHomeDir.c_str(), &namelist, 0, alphasort);

   set<string> localfiles;
   localfiles.clear();

   if (n < 0)
      perror("scandir");
   else 
   {
      for (int i = 0; i < n; ++ i) 
      {
         // skip ".", "..", and other reserved directory starting by '.'
         // skip directory

         struct stat s;
         stat((m_strHomeDir + namelist[i]->d_name).c_str(), &s);

         if ((namelist[i]->d_name[0] != '.') && (!S_ISDIR(s.st_mode)))
            localfiles.insert(localfiles.end(), namelist[i]->d_name);

         free(namelist[i]);
      }
      free(namelist);
   }

   // check deleted files
   set<string> fl;
   m_LocalFile.getFileList(fl);
   for (set<string>::iterator i = fl.begin(); i != fl.end(); ++ i)
   {
      if (localfiles.find(*i) == localfiles.end())
      {
         m_LocalFile.remove(*i);
         //cout << "remove local file " << *i << endl;
      }
   }

   // check new files on disk
   for (set<string>::iterator i = localfiles.begin(); i != localfiles.end(); ++ i)
   {
      if (m_LocalFile.lookup(*i) < 0)
      {
         ifstream ifs((m_strHomeDir + *i).c_str());
         ifs.seekg(0, ios::end);
         int64_t size = ifs.tellg();
         ifs.close();

         strcpy(attr.m_pcName, i->c_str());
         // original file is read only
         attr.m_iAttr = 1;
         attr.m_llSize = size;

         struct stat s;
         stat((m_strHomeDir + *i).c_str(), &s);
         attr.m_llTimeStamp = (int64_t)s.st_mtime * 1000000;

         m_LocalFile.insert(attr);

         //cout << "add local file... " << *i << " " << size << endl;
      }
   }

   return 1;
}
