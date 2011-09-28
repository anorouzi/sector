/*****************************************************************************
Copyright 2005 - 2011 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 04/24/2011
*****************************************************************************/

#ifndef WIN32
   #include <arpa/inet.h>
   #include <pthread.h>
   #include <sys/socket.h>
#endif
#include <fstream>
#include <iostream>
#include <signal.h>
#include <sys/types.h>

#include "sector.h"
#include "security.h"

using namespace std;
using namespace sector;

int User::serialize(const vector<string>& input, string& buf) const
{
   buf = "";
   for (vector<string>::const_iterator i = input.begin(); i != input.end(); ++ i)
   {
      buf.append(*i);
      buf.append(";");
   }

   return buf.length() + 1;
}

SSource::~SSource()
{
}

SServer::SServer():
m_iKeySeed(1),
m_iPort(0),
m_pSecuritySource(NULL)
{
}

int SServer::init(const int& port, const char* cert, const char* key)
{
   SSLTransport::init();

   m_iPort = port;

   if (m_SSL.initServerCTX(cert, key) < 0)
   {
      cerr << "cannot initialize security infomation with provided key/certificate.\n";
      return -1;
   }

   if (m_SSL.open(NULL, m_iPort) < 0)
   {
      cerr << "port is not available.\n";
      return -1;
   }

   m_SSL.listen();

   return 0;
}

void SServer::close()
{
   m_SSL.close();
   SSLTransport::destroy();
}

int SServer::setSecuritySource(SSource* src)
{
   if (NULL == src)
      return -1;

   m_pSecuritySource = src;
   return 0;
}

void SServer::run()
{
   // security source must be initialized and set before the server is running
   if (NULL == m_pSecuritySource)
      return;

#ifndef WIN32
   //ignore SIGPIPE
   sigset_t ps;
   sigemptyset(&ps);
   sigaddset(&ps, SIGPIPE);
   pthread_sigmask(SIG_BLOCK, &ps, NULL);
#endif

   while (true)
   {
      char ip[64];
      int port;
      SSLTransport* s = m_SSL.accept(ip, port);
      if (NULL == s)
         continue;

      // only a master node can query security information
      if (!m_pSecuritySource->matchMasterACL(ip))
      {
         s->close();
         continue;
      };

      Param* p = new Param;
      p->ip = ip;
      p->port = port;
      p->sserver = this;
      p->ssl = s;

#ifndef WIN32
      pthread_t t;
      pthread_create(&t, NULL, process, p);
      pthread_detach(t);
#else
      DWORD ThreadID;
      HANDLE hThread = CreateThread(NULL, 0, process, p, NULL, &ThreadID);
      CloseHandle (hThread);
#endif
   }
}

int32_t SServer::generateKey()
{
   return m_iKeySeed ++;
}

#ifndef WIN32
   void* SServer::process(void* p)
#else
   DWORD WINAPI SServer::process(void* p)
#endif
{
   SServer* self = ((Param*)p)->sserver;
   SSLTransport* s = ((Param*)p)->ssl;
   delete (Param*)p;
cout << "thread 11\n";
   while (true)
   {
      int32_t cmd;
      if (s->recv((char*)&cmd, 4) <= 0)
         goto EXIT;
cout << "cmd = " << cmd << endl;
      // check if the security source has been updated (e.g., user account change)
      if (self->m_pSecuritySource->isUpdated())
         self->m_pSecuritySource->refresh();

      switch (cmd)
      {
      case 1: // slave node join
      {
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t key = self->generateKey();
         if (!self->m_pSecuritySource->matchSlaveACL(ip))
            key = SectorError::E_ACL;
         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;

         break;
      }

      case 2: // user login
      {
         char user[64];
         if (s->recv(user, 64) <= 0)
            goto EXIT;
         char password[128];
         if (s->recv(password, 128) <= 0)
            goto EXIT;
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t key;
         User u;

         if (self->m_pSecuritySource->retrieveUser(user, password, ip, u) >= 0)
            key = self->generateKey();
         else
            key = SectorError::E_SECURITY;

         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;

         if (key > 0)
         {
            string buf;
            int32_t size;

            size = u.serialize(u.m_vstrReadList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            size = u.serialize(u.m_vstrWriteList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            int exec = u.m_bExec ? 1 : 0;
            if (s->send((char*)&exec, 4) <= 0)
               goto EXIT;
         }

         break;
      }

      case 3: // master join
      {
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t res = 1;
         if (!self->m_pSecuritySource->matchMasterACL(ip))
            res = SectorError::E_ACL;
         if (s->send((char*)&res, 4) <= 0)
            goto EXIT;

         break;
      }

      case 4: // master init
      {
         int32_t key = self->generateKey();

         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;

         break;
      }

      default:
         goto EXIT;
      }
   }

EXIT:
   s->close();
   delete s;
   return NULL;
}
