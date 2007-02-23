/*****************************************************************************
Copyright � 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/sendfile.h>
#include <server.h>
#include <assert.h>
#include <sstream>
#include <signal.h>
#include <util.h>
#include <data.h>
#include <sql.h>
#include <table.h>

using namespace std;
using namespace CodeBlue;

Server::Server(const string& ip)
{
   m_strLocalHost = ip;
}

Server::~Server()
{
   m_GMP.close();
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
      res = m_Router.start(m_strLocalHost.c_str(), m_iLocalPort);
   }
   else
   {
      CCBMsg msg;
      msg.setType(11); // look up port for the router
      msg.m_iDataLength = 4;

      if ((m_GMP.rpc(ip, port, &msg, &msg) < 0) || (msg.getType() < 0))
         return -1;

      res = m_Router.join(m_strLocalHost.c_str(), ip, m_iLocalPort, *(int*)msg.getData());
   }
   if (res < 0)
      return -1;

   if (initLocalFile() < 0)
      return -1;

   pthread_t msgserver;
   pthread_create(&msgserver, NULL, run, this);
   pthread_detach(msgserver);

   // ignore TCP broken pipe
   signal(SIGPIPE, SIG_IGN);

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

void* Server::run(void* s)
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

      //cout << "recv CB " << msg->getType() << " " << ip << " " << port << endl;

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
               /*
               int r = (int)(filelist.size() * rand() / double(RAND_MAX));
               set<CFileAttr, CAttrComp>::iterator i = filelist.begin();
               for (int j = 0; j < r; ++ j)
                  ++ i;

               msg->setData(0, i->m_pcHost, strlen(i->m_pcHost) + 1);
               msg->setData(64, (char*)(&i->m_iPort), 4);
               msg->m_iDataLength = 4 + 64 + 4;

               cout << "locate " << filename << " " << filelist.size() << " " << i->m_pcHost << " " << i->m_iPort << endl;
               */

               // feedback all copies of the requested file
               int num = 0;
               for (set<CFileAttr, CAttrComp>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
               {
                  msg->setData(num * 68, i->m_pcHost, strlen(i->m_pcHost) + 1);
                  msg->setData(num * 68 + 64, (char*)(&i->m_iPort), 4);
                  ++ num;
               }
               msg->m_iDataLength = 4 + num * (64 + 4);

               cout << "locate " << filename << ": " << filelist.size() << " found!" << endl;
            }

            self->m_GMP.sendto(ip, port, id, msg);
            break;
         }

      case 2: // open file
         {
            char* filename = msg->getData();

            int conn = *(int*)(msg->getData() + 64);
            int mode = *(int*)(msg->getData() + 68);
            char cert[1024];
            if (msg->m_iDataLength > 4 + 64 + 4 + 4)
               strcpy(cert, msg->getData() + 72);
            else
               cert[0] = '\0';

            set<CFileAttr, CAttrComp> filelist;
            if (self->m_LocalFile.lookup(filename, &filelist) < 0)
            {
               CFileAttr attr;
               memcpy(attr.m_pcName, filename, 64);
               attr.m_iAttr = 3;
               attr.m_llSize = 0;
               memcpy(attr.m_pcHost, self->m_strLocalHost.c_str(), 64);
               attr.m_iPort = self->m_iLocalPort;
               self->m_LocalFile.insert(attr);
               filelist.insert(attr);;
            }

            // set IO attributes: READ WRITE
            // mode &= attr.m_iType;
            // TO DO: check ownership and previlege;
            ifstream ifs;
            ifs.open((self->m_strHomeDir + "/.cert/" + filename + ".cert").c_str());
            char ecert[1024];
            ecert[0] = '\0';
            ifs.getline(ecert, 1024);

            if ((0 == strlen(cert)) || (0 == strlen(ecert)))
               mode = 0;
            else
            {
               unsigned char sha[SHA_DIGEST_LENGTH + 1];
               SHA1((const unsigned char*)cert, strlen(cert), sha);
               sha[SHA_DIGEST_LENGTH] = '\0';
               stringstream shastr(stringstream::in | stringstream::out);
               for (int i = 0; i < SHA_DIGEST_LENGTH; i += 4)
                  shastr << *(int32_t*)(sha + i);

               if (shastr.str() == string(ecert))
                  mode = 1;
               else
                  mode = 0;
            }

            self->m_AccessLog.insert(ip, port, msg->getData());

            cout << "===> start file server " << endl;

            UDTSOCKET u;
            int t;

            if (1 == conn)
               u = UDT::socket(AF_INET, SOCK_STREAM, 0);
            else
               t = socket(AF_INET, SOCK_STREAM, 0);

            sockaddr_in my_addr;
            my_addr.sin_family = AF_INET;
            my_addr.sin_port = 0;
            my_addr.sin_addr.s_addr = INADDR_ANY;
            memset(&(my_addr.sin_zero), '\0', 8);

            int res;
            if (1 == conn)
               res = UDT::bind(u, (sockaddr*)&my_addr, sizeof(my_addr));
            else
               res = bind(t, (sockaddr*)&my_addr, sizeof(my_addr));

            if (((1 == conn) && (UDT::ERROR == res)) || ((2 == conn) && (-1 == res)))
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;

               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            if (1 == conn)
               UDT::listen(u, 1);
            else
               listen(t, 1);

            Param2* p = new Param2;
            p->s = self;
            p->fn = msg->getData();
            p->u = u;
            p->t = t;
            p->c = conn;
            p->m = mode;

            pthread_t file_handler;
            pthread_create(&file_handler, NULL, fileHandler, p);
            pthread_detach(file_handler);

            int size = sizeof(sockaddr_in);
            if (1 == conn)
               UDT::getsockname(u, (sockaddr*)&my_addr, &size);
            else
               getsockname(t, (sockaddr*)&my_addr, (socklen_t*)&size);

            msg->setData(0, (char*)&my_addr.sin_port, 4);
            msg->m_iDataLength = 4 + 4;
            //cout << "feedback port " << my_addr.sin_port <<endl;

            self->m_GMP.sendto(ip, port, id, msg);

            //cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

            break;
        }

      case 3: // add a new file
         {
            CFileAttr attr;
            attr.deserialize(msg->getData(), msg->m_iDataLength - 4);

            //cout << "remote file : " << attr.m_pcName << " " << attr.m_llSize << endl;

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

            if (self->m_LocalFile.lookup(filename) > 0)
            {
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

      case 7: // remove file from RemoteFileIndex
         {
            self->m_RemoteFile.remove(msg->getData());
            msg->m_iDataLength = 4;

            break;
         }

      case 200: // open a SQL connection
         {
            char* filename = msg->getData();

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

            UDTSOCKET u;
            int t;

            int conn = *(int*)(msg->getData() + 64);
            if (1 == conn)
               u = UDT::socket(AF_INET, SOCK_STREAM, 0);
            else
               t = socket(AF_INET, SOCK_STREAM, 0);

            sockaddr_in my_addr;
            my_addr.sin_family = AF_INET;
            my_addr.sin_port = 0;
            my_addr.sin_addr.s_addr = INADDR_ANY;
            memset(&(my_addr.sin_zero), '\0', 8);

            int res;
            if (1 == conn)
               res = UDT::bind(u, (sockaddr*)&my_addr, sizeof(my_addr));
            else
               res = bind(t, (sockaddr*)&my_addr, sizeof(my_addr));

            if (((1 == conn) && (UDT::ERROR == res)) || ((2 == conn) && (-1 == res)))
            {
               msg->setType(-msg->getType());
               msg->m_iDataLength = 4;
               self->m_GMP.sendto(ip, port, id, msg);
               break;
            }

            if (1 == conn)
               UDT::listen(u, 1);
            else
               listen(t, 1);

            Param3* p = new Param3;
            p->s = self;
            p->fn = msg->getData();
            p->q = msg->getData() + 64 + 4;
            p->u = u;
            p->t = t;
            p->c = conn;

            pthread_t sql_handler;
            pthread_create(&sql_handler, NULL, SQLHandler, p);
            pthread_detach(sql_handler);

            int size = sizeof(sockaddr_in);
            if (1 == conn)
               UDT::getsockname(u, (sockaddr*)&my_addr, &size);
            else
               getsockname(t, (sockaddr*)&my_addr, (socklen_t*)&size);

            msg->setData(0, (char*)&my_addr.sin_port, 4);
            msg->m_iDataLength = 4 + 4;
            //cout << "feedback port " << my_addr.sin_port <<endl;

            self->m_GMP.sendto(ip, port, id, msg);

            //cout << "responded " << ip << " " << port << " " << msg->getType() << " " << msg->m_iDataLength << endl;

            break; 
         }

      default:
         {
            Param1* p = new Param1;
            p->s = self;
            memcpy(p->ip, ip, 64);
            p->id = id;
            p->port = port;
            p->msg = new CCBMsg(*msg);

            pthread_t process_thread;
            pthread_create(&process_thread, NULL, process, p);
            pthread_detach(process_thread);

            break;
         }
      }

      //cout << "respond CB " << msg->getType() << endl;
   }

   delete msg;
   return NULL;
}

void* Server::process(void* p)
{
   Server* self = ((Param1*)p)->s;
   char* ip = ((Param1*)p)->ip;
   int port = ((Param1*)p)->port;
   int32_t id = ((Param1*)p)->id;
   CCBMsg* msg = ((Param1*)p)->msg;

   cout << "recv request " << msg->getType() << endl;

   switch (msg->getType())
   {
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
      break;
   }

   self->m_GMP.sendto(ip, port, id, msg);

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
      usleep(500);

      // TO DO
      // check disk file for size update

      int fid = DHash::hash(i->first.c_str(), m_iKeySpace);

      if (-1 == m_Router.lookup(fid, &loc))
         continue;

      set<CFileAttr, CAttrComp>::iterator attr = i->second.begin();

      // if the "loc" already have the file information, no need to update
      if (0 == strcmp(loc.m_pcIP, attr->m_pcNameHost))
         continue;

      // notify the current name holder to remove this file from its index
      if (strlen(attr->m_pcNameHost) > 0)
      {
         msg.setType(7);
         strcpy(msg.getData(), i->first.c_str());
         msg.m_iDataLength = 4 + strlen(i->first.c_str()) + 1;
         m_GMP.rpc(attr->m_pcNameHost, attr->m_iPort, &msg, &msg);
      }

      // Dangerous const cast!!!
      strcpy((char*)attr->m_pcNameHost, loc.m_pcIP);
      const_cast<int&>(attr->m_iNamePort) = m_iLocalPort;

      msg.setType(3);
      attr->serialize(msg.getData(), msg.m_iDataLength);
      msg.m_iDataLength += 4;
      assert(msg.m_iDataLength < 1024);
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
      usleep(500);

      // check if the original file still exists
      int c = 0;
      for (set<CFileAttr, CAttrComp>::iterator j = i->second.begin(); j != i->second.end();)
      {
         msg.setType(6);
         msg.setData(0, j->m_pcName, strlen(j->m_pcName) + 1);
         msg.m_iDataLength = 4 + strlen(j->m_pcName) + 1;

         int r = m_GMP.rpc(j->m_pcHost, j->m_iPort, &msg, &msg);

         if ((r <= 0) || (msg.getType() < 0))
         {
            i->second.erase(j);
            j = i->second.begin();
            for (int k = 0; k < c; ++ k)
               ++ j;
         }
         else
         {
            ++ j;
            ++ c;
         }
      }

      if (i->second.size() == 0)
         m_RemoteFile.remove(i->first);
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