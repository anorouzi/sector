/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 03/24/2007
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

   if (initLocalFile() < 0)
      return -1;

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

   timeval t;
   gettimeofday(&t, 0);
   srand(t.tv_sec);

   while (true)
   {
      self->m_GMP.recvfrom(ip, port, id, msg);

      //cout << "recv CB " << msg->getType() << " " << ip << " " << port << " " << msg->m_iDataLength << endl;

      switch (msg->getType())
      {
         case 1: // locate file
         {
            string filename = msg->getData();

            set<CFileAttr, CAttrComp> filelist;

            if (self->m_RemoteFile.lookup(filename, &filelist) < 0)
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
            }
            else
            {
               // feedback all copies of the requested file
               int num = 0;
               for (set<CFileAttr, CAttrComp>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
               {
                  msg->setData(num * 68, i->m_pcHost, strlen(i->m_pcHost) + 1);
                  msg->setData(num * 68 + 64, (char*)(&i->m_iPort), 4);
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

            if (self->m_LocalFile.lookup(filename, NULL) < 0)
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
            //cout << "feedback port " << dataport <<endl;

            self->m_GMP.sendto(ip, port, id, msg);

            //cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

            break;
         }

         case 3: // add a new file
         {
            CFileAttr attr;
            attr.deserialize(msg->getData(), msg->m_iDataLength - 4);

            if (self->m_RemoteFile.insert(attr) < 0)
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
            strcpy(attr.m_pcHost, self->m_strLocalHost.c_str());
            attr.m_iPort = self->m_iLocalPort;
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
            string filename = msg->getData();

            if (self->m_LocalFile.lookup(filename) <= 0)
               msg->setType(-msg->getType());

            msg->m_iDataLength = 4;

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

         case 7: // check query from RemoteFileIndex
         {
            if (!self->m_Router.has(DHash::hash(msg->getData(), m_iKeySpace)))
            {
               self->m_RemoteFile.remove(msg->getData());
               msg->setType(- msg->getType());
            }

            msg->m_iDataLength = 4;
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

            set<CFileAttr, CAttrComp> filelist;
            if (self->m_LocalFile.lookup(filename, &filelist) < 0)
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
            //cout << "feedback port " << dataport <<endl;

            self->m_GMP.sendto(ip, port, id, msg);

            cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

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
            p->function = msg->getData() + 8;
            p->psize = msg->m_iDataLength - 72;
            p->param = new char[p->psize];
            memcpy(p->param, msg->getData() + 72, p->psize);
            p->client_data_port = *(int32_t*)(msg->getData() + 4);

            cout << "starting SPE ... " << p->speid << " " << p->client_data_port << " " << p->function << endl;

            pthread_t spe_handler;
            pthread_create(&spe_handler, NULL, SPEHandler, p);
            pthread_detach(spe_handler);

            msg->setData(0, (char*)&dataport, 4);
            msg->m_iDataLength = 4 + 4;
            cout << "feedback port " << dataport <<endl;

            self->m_GMP.sendto(ip, port, id, msg);

            cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

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

      //cout << "respond CB " << msg->getType() << endl;
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
         if (self->m_LocalFile.lookup(filename, NULL) > 0)
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
               set<CFileAttr, CAttrComp> sa;
               if (self->m_RemoteFile.lookup(filename, &sa) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
               else
               {
                  sa.begin()->serialize(msg->getData(), msg->m_iDataLength);
                  msg->m_iDataLength += 4;
               }

               cout << "syn " << filename << " " << msg->getType() << " " << msg->m_iDataLength - 4 << " " << sa.begin()->m_llSize << endl;
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
         set<CFileAttr, CAttrComp> sa;

         if (self->m_LocalFile.lookup(filename, &sa) >= 0)
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
               set<CFileAttr, CAttrComp> sa;
               if (self->m_RemoteFile.lookup(filename, &sa) < 0)
               {
                  msg->setType(-msg->getType());
                  msg->m_iDataLength = 4;
               }
               else
               {
                  if (self->m_GMP.rpc(sa.begin()->m_pcHost, sa.begin()->m_iPort, msg, msg) < 0)
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
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_LocalFile.getFileList(filelist);

   CCBMsg msg;
   //msg.resize(65536);

   Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(100);

      // ask remote if it is the right node to hold the metadata for this file
      msg.setType(7);
      strcpy(msg.getData(), i->first.c_str());
      msg.m_iDataLength = 4 + strlen(i->first.c_str()) + 1;

//cout << "checking "<< i->first << " " << i->second.begin()->m_pcNameHost << " " << i->second.begin()->m_iNamePort << endl;

      if ((m_GMP.rpc(i->second.begin()->m_pcNameHost, i->second.begin()->m_iNamePort, &msg, &msg) >= 0) && (msg.getType() > 0))
         continue;

//cout << "wrong node??? " << i->first << " " << i->second.begin()->m_pcNameHost << " " << i->second.begin()->m_iNamePort << endl;

      int fid = DHash::hash(i->first.c_str(), m_iKeySpace);
      if (-1 == m_Router.lookup(fid, &loc))
         continue;

//cout << "new loc " << loc.m_pcIP << " " << loc.m_iAppPort << endl;

      m_LocalFile.updateNameServer(i->first, loc);

      // send metadata to a new node
      msg.setType(3);
      i->second.begin()->serialize(msg.getData(), msg.m_iDataLength);
      msg.m_iDataLength += 4;
      m_GMP.rpc(loc.m_pcIP, loc.m_iAppPort, &msg, &msg);
   }
}

void Server::updateInLink()
{
   map<string, set<CFileAttr, CAttrComp> > filelist;
   m_RemoteFile.getFileList(filelist);

   CCBMsg msg;
   msg.resize(65536);

   //Node loc;

   for (map<string, set<CFileAttr, CAttrComp> >::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      usleep(100);

      // check if the original file still exists
      for (set<CFileAttr, CAttrComp>::iterator j = i->second.begin(); j != i->second.end(); ++ j)
      {
         msg.setType(6);
         msg.setData(0, j->m_pcName, strlen(j->m_pcName) + 1);
         msg.m_iDataLength = 4 + strlen(j->m_pcName) + 1;

         int r = m_GMP.rpc(j->m_pcHost, j->m_iPort, &msg, &msg);

         if ((r <= 0) || (msg.getType() < 0))
            m_RemoteFile.removeCopy(*j);
      }

      if (i->second.size() == 0)
         m_RemoteFile.remove(i->first);
      else if ((i->second.size() < 2) && (i->second.begin()->m_iType != 3))
      {
         // less than 2 copies in the system, create a new one
         // TODO: start a timeout before making a copy
         /*
         int seed = 1 + (int)(10.0 * rand() / (RAND_MAX + 1.0));
         Node n;
         if (m_Router.lookup(seed, &n) < 0)
            continue;

         msg.setType(11);
         msg.setData(0, (char*)&(i->second.begin()->m_llSize), 8);
         msg.setData(8, i->second.begin()->m_pcName, strlen(i->second.begin()->m_pcName) + 1);
         msg.m_iDataLength = 4 + 8 + strlen(i->second.begin()->m_pcName) + 1;

         m_GMP.rpc(n.m_pcIP, n.m_iAppPort, &msg, &msg);
         */
      }
   }
}

int Server::initLocalFile()
{
   m_strHomeDir = m_SysConfig.m_strDataDir;

   cout << "Home Dir " << m_strHomeDir << endl;

   CFileAttr attr;

   // initialize all files in the home directory, excluding "." and ".."
   dirent **namelist;
   int n = scandir(m_strHomeDir.c_str(), &namelist, 0, alphasort);

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
         {
            ifstream ifs((m_strHomeDir + namelist[i]->d_name).c_str());
            ifs.seekg(0, ios::end);
            int64_t size = ifs.tellg();
            ifs.close();

            strcpy(attr.m_pcName, namelist[i]->d_name);
            strcpy(attr.m_pcHost, m_strLocalHost.c_str());
            attr.m_iPort = m_iLocalPort;
            strcpy(attr.m_pcNameHost, "");
            attr.m_iNamePort = 0;
            // original file is read only
            attr.m_iAttr = 1;
            attr.m_llSize = size;
            attr.m_llTimeStamp = (int64_t)s.st_mtime * 1000000;

            m_LocalFile.insert(attr);

            cout << "init local file... " << namelist[i]->d_name << " " << size << endl;
         }

         free(namelist[i]);
      }
      free(namelist);
   }

   return 1;
}
