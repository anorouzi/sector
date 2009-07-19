/*****************************************************************************
Copyright (c) 2001 - 2009, The Board of Trustees of the University of Illinois.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above
  copyright notice, this list of conditions and the
  following disclaimer.

* Redistributions in binary form must reproduce the
  above copyright notice, this list of conditions
  and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the University of Illinois
  nor the names of its contributors may be used to
  endorse or promote products derived from this
  software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 07/07/2009
*****************************************************************************/

#include "security.h"
#include <constant.h>
#include <fstream>
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <signal.h>
#include <conf.h>
#include <dirent.h>
#include <sys/stat.h>

using namespace std;

ACL::ACL()
{
   m_vIPList.clear();
}

ACL::~ACL()
{
}

int ACL::init(const char* aclfile)
{
   ifstream af(aclfile);

   if (af.fail() || af.bad())
      return -1;

   char line[128];
   while (!af.eof())
   {
      af.getline(line, 128);
      addIPRange(line);
   }

   af.close();

   return m_vIPList.size();
}

int ACL::addIPRange(const char* ip)
{
   char buf[128];
   unsigned int i = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i)
   {
      if ('/' == ip[i])
         break;

      buf[i] = ip[i];
   }
   buf[i] = '\0';

   in_addr addr;
   if (inet_pton(AF_INET, buf, &addr) <= 0)
      return -1;

   IPRange entry;
   entry.m_uiIP = addr.s_addr;
   entry.m_uiMask = 0xFFFFFFFF;

   if (i == strlen(ip))
   {
      m_vIPList.insert(m_vIPList.end(), entry);
      return 0;
   }

   if ('/' != ip[i])
      return -1;
   ++ i;

   bool format = false;
   int j = 0;
   for (unsigned int n = strlen(ip); i < n; ++ i, ++ j)
   {
      if ('.' == ip[i])
         format = true;

      buf[j] = ip[i];
   }
   buf[j] = '\0';

   if (format)
   {
      //255.255.255.0
      if (inet_pton(AF_INET, buf, &addr) < 0)
         return -1;
      entry.m_uiMask = addr.s_addr;
   }
   else
   {
      char* p;
      unsigned int bit = strtol(buf, &p, 10);

      if ((p == buf) || (bit > 32) || (bit < 0))
         return -1;

      if (bit < 32)
         entry.m_uiMask = ((unsigned int)1 << bit) - 1;
   }

   m_vIPList.insert(m_vIPList.end(), entry);

   return 0;
}

bool ACL::match(const char* ip)
{
   in_addr addr;
   if (inet_pton(AF_INET, ip, &addr) < 0)
      return false;

   for (vector<IPRange>::iterator i = m_vIPList.begin(); i != m_vIPList.end(); ++ i)
   {
      if ((addr.s_addr & i->m_uiMask) == (i->m_uiIP & i->m_uiMask))
         return true;
   }

   return false;
}

int User::init(const char* name, const char* ufile)
{
   m_iID = 0;
   m_strName = name;
   m_strPassword = "";
   m_vstrReadList.clear();
   m_vstrWriteList.clear();
   m_bExec = false;
   m_llQuota = -1;

   ConfParser parser;
   Param param;

   if (0 != parser.init(ufile))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("PASSWORD" == param.m_strName)
         m_strPassword = param.m_vstrValue[0];
      else if ("READ_PERMISSION" == param.m_strName)
         m_vstrReadList = param.m_vstrValue;
      else if ("WRITE_PERMISSION" == param.m_strName)
         m_vstrWriteList = param.m_vstrValue;
      else if ("EXEC_PERMISSION" == param.m_strName)
      {
         if (param.m_vstrValue[0] == "TRUE")
            m_bExec = true;
      }
      else if ("ACL" == param.m_strName)
      {
         for (vector<string>::iterator i = param.m_vstrValue.begin(); i != param.m_vstrValue.end(); ++ i)
            m_ACL.addIPRange(i->c_str());
      }
      else if ("QUOTA" == param.m_strName)
         m_llQuota = atoll(param.m_vstrValue[0].c_str());
      else
         cerr << "unrecongnized user configuration parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 1;
}

int User::serialize(const vector<string>& input, string& buf)
{
   buf = "";
   for (vector<string>::const_iterator i = input.begin(); i != input.end(); ++ i)
   {
      buf.append(*i);
      buf.append(";");
   }

   return buf.length() + 1;
}

int Shadow::init(const string& path)
{
   dirent **namelist;
   int n = scandir(path.c_str(), &namelist, 0, alphasort);

   if (n < 0)
      return -1;

   m_mUser.clear();

   for (int i = 0; i < n; ++ i)
   {
      // skip "." and ".."
      if ((strcmp(namelist[i]->d_name, ".") == 0) || (strcmp(namelist[i]->d_name, "..") == 0))
      {
         free(namelist[i]);
         continue;
      }

      struct stat s;
      stat((path + "/" + namelist[i]->d_name).c_str(), &s);

      if (S_ISDIR(s.st_mode))
      {
         free(namelist[i]);
         continue;
      }

      User u;
      if (u.init(namelist[i]->d_name, (path + "/" + namelist[i]->d_name).c_str()) > 0)
         m_mUser[u.m_strName] = u;

      free(namelist[i]);
   }
   free(namelist);

   return m_mUser.size();
}

User* Shadow::match(const char* name, const char* password, const char* ip)
{
   map<string, User>::iterator i = m_mUser.find(name);

   if (i == m_mUser.end())
      return NULL;

   if (i->second.m_strPassword != password)
      return NULL;

   if (!(i->second.m_ACL.match(ip)))
      return NULL;

   return &(i->second);
}


SServer::SServer():
m_iKeySeed(1),
m_iPort(0)
{
}

int SServer::init(const int& port, const char* cert, const char* key)
{
   signal(SIGPIPE, SIG_IGN);

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

int SServer::loadMasterACL(const char* aclfile)
{
   return m_MasterACL.init(aclfile);
}

int SServer::loadSlaveACL(const char* aclfile)
{
   return m_SlaveACL.init(aclfile);
}

int SServer::loadShadowFile(const char* shadowpath)
{
   return m_Shadow.init(shadowpath);
}

void SServer::close()
{
   m_SSL.close();
   SSLTransport::destroy();
}

void SServer::run()
{
   while (true)
   {
      char ip[64];
      int port;
      SSLTransport* s = m_SSL.accept(ip, port);
      if (NULL == s)
         continue;

      // only a master node can query security information
      if (!m_MasterACL.match(ip))
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
   signal(SIGPIPE, SIG_IGN);

   SServer* self = ((Param*)p)->sserver;
   SSLTransport* s = ((Param*)p)->ssl;

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
         if (!self->m_SlaveACL.match(ip))
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
         User* u;
         if ((u = self->m_Shadow.match(user, password, ip)) != NULL)
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
         if (!self->m_MasterACL.match(ip))
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

EXIT:
   s->close();
   delete (Param*)p;
   return NULL;
}
