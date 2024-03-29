/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

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
   Yunhong Gu, last updated 08/19/2010
*****************************************************************************/

#include "security.h"
#include <sector.h>
#include <fstream>
#include <iostream>
#include <pthread.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace std;


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


SServer::SServer():
m_iKeySeed(1),
m_iPort(0)
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

   return 1;
}

void SServer::close()
{
   m_SSL.close();
   SSLTransport::destroy();
}

int SServer::loadMasterACL(SSource* src, const void* param)
{
   return src->loadACL(m_vMasterACL, param);
}

int SServer::loadSlaveACL(SSource* src, const void* param)
{
   return src->loadACL(m_vSlaveACL, param);
}

int SServer::loadShadowFile(SSource* src, const void* param)
{
   return src->loadUsers(m_mUsers, param);
}

bool SServer::match(const vector<IPRange>& acl, const char* ip)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip, &addr) < 0)
      return false;

   for (vector<IPRange>::const_iterator i = acl.begin(); i != acl.end(); ++ i)
   {
      if ((addr.s_addr & i->m_uiMask) == (i->m_uiIP & i->m_uiMask))
         return true;
   }

   return false;
}

const User* SServer::match(const map<string, User>& users, const char* name, const char* password, const char* ip)
{
   map<string, User>::const_iterator i = users.find(name);

   if (i == users.end())
      return NULL;

   if (i->second.m_strPassword != password)
      return NULL;

   if (!match(i->second.m_vACL, ip))
      return NULL;

   return &(i->second);
}


void SServer::run()
{
   //ignore SIGPIPE
   sigset_t ps;
   sigemptyset(&ps);
   sigaddset(&ps, SIGPIPE);
   pthread_sigmask(SIG_BLOCK, &ps, NULL);

   while (true)
   {
      char ip[64];
      int port;
      SSLTransport* s = m_SSL.accept(ip, port);
      if (NULL == s)
         continue;

      // only a master node can query security information
      if (!match(m_vMasterACL, ip))
      {
         s->close();
         continue;
      };

      Param* p = new Param;
      p->ip = ip;
      p->port = port;
      p->sserver = this;
      p->ssl = s;

      pthread_t t;
      pthread_create(&t, NULL, process, p);
      pthread_detach(t);
   }
}

int32_t SServer::generateKey()
{
   return m_iKeySeed ++;
}

void* SServer::process(void* p)
{
   SServer* self = ((Param*)p)->sserver;
   SSLTransport* s = ((Param*)p)->ssl;
   delete (Param*)p;

   while (true)
   {
      int32_t cmd;
      if (s->recv((char*)&cmd, 4) <= 0)
         goto EXIT;

      switch (cmd)
      {
      case 1: // slave node join
      {
         char ip[64];
         if (s->recv(ip, 64) <= 0)
            goto EXIT;

         int32_t key = self->generateKey();
         if (!self->match(self->m_vSlaveACL, ip))
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
         const User* u = self->match(self->m_mUsers, user, password, ip);
         if (u != NULL)
            key = self->generateKey();
         else
            key = -1;

         if (s->send((char*)&key, 4) <= 0)
            goto EXIT;

         if (key > 0)
         {
            string buf;
            int32_t size;

            size = u->serialize(u->m_vstrReadList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            size = u->serialize(u->m_vstrWriteList, buf);
            if ((s->send((char*)&size, 4) <= 0) || (s->send(buf.c_str(), size) <= 0))
               goto EXIT;

            int exec = u->m_bExec ? 1 : 0;
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
         if (!self->match(self->m_vMasterACL, ip))
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
