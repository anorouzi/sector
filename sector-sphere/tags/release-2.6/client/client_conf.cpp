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
   Yunhong Gu, last updated 04/05/2011
*****************************************************************************/

#include <sector.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <sys/stat.h>
#include <iostream>
#include <osportable.h>
#ifdef WIN32
   #define atoll _atoi64
#endif

using namespace std;

ClientConf::ClientConf():
m_strUserName(),
m_strPassword(),
m_strMasterIP(),
m_iMasterPort(6000),
m_strCertificate(),
m_llMaxCacheSize(10000000),
m_iFuseReadAheadBlock(1000000),
m_llMaxWriteCacheSize(10000000)
{
}

int ClientConf::init(const string& path)
{
   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("MASTER_ADDRESS" == param.m_strName)
      {
         char buf[128];
         strncpy(buf, param.m_vstrValue[0].c_str(), 128);

         unsigned int i = 0;
         for (unsigned int n = strlen(buf); i < n; ++ i)
         {
            if (buf[i] == ':')
               break;
         }

         buf[i] = '\0';
         m_strMasterIP = buf;
         m_iMasterPort = atoi(buf + i + 1);
      }
      else if ("USERNAME" == param.m_strName)
      {
         m_strUserName = param.m_vstrValue[0];
      }
      else if ("PASSWORD" == param.m_strName)
      {
         m_strPassword = param.m_vstrValue[0];
      }
      else if ("CERTIFICATE" == param.m_strName)
      {
         m_strCertificate = param.m_vstrValue[0];
      }
      else if ("MAX_CACHE_SIZE" == param.m_strName)
      {
         m_llMaxCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
      }
      else if ("FUSE_READ_AHEAD_BLOCK" == param.m_strName)
      {
         m_iFuseReadAheadBlock = atoi(param.m_vstrValue[0].c_str()) * 1000000;
      }
      else if ("MAX_READ_CACHE_SIZE" == param.m_strName)
      {
         m_llMaxWriteCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
      }
      else
      {
         cerr << "unrecongnized client.conf parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}


int Session::loadInfo(const char* conf)
{
   string sector_home, conf_file_path;
   if (ConfLocation::locate(sector_home) < 0)
      conf_file_path = conf;
   else
      conf_file_path = sector_home + "/conf/client.conf";

   m_ClientConf.init(conf_file_path);

   if (m_ClientConf.m_strMasterIP == "")
   {
      cout << "please input the master address (e.g., 123.123.123.123:1234): ";
      string addr;
      cin >> addr;

      char buf[128];
      strncpy(buf, addr.c_str(), 128);

      unsigned int i = 0;
      for (unsigned int n = strlen(buf); i < n; ++ i)
      {
         if (buf[i] == ':')
            break;
      }

      buf[i] = '\0';
      m_ClientConf.m_strMasterIP = buf;
      m_ClientConf.m_iMasterPort = atoi(buf + i + 1);
   }

   if (m_ClientConf.m_strUserName == "")
   {
      cout << "please input the user name: ";
      cin >> m_ClientConf.m_strUserName;
   }

   if (m_ClientConf.m_strPassword == "")
   {
      cout << "please input the password: ";
      cin >> m_ClientConf.m_strPassword;
   }

   SNode s;
   if (LocalFS::stat(m_ClientConf.m_strCertificate, s) < 0)
   {
      if (LocalFS::stat(sector_home + "/conf/master_node.cert", s) == 0)
      {
         m_ClientConf.m_strCertificate = sector_home + "/conf/master_node.cert";
      }
      else
      {
         m_ClientConf.m_strCertificate = "";
         cout << "WARNING: couldn't locate the master certificate, will try to download one from the master node.\n";
      }
   }

   return 1;
}

void Utility::print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

int Utility::login(Sector& client)
{
   Session s;
   s.loadInfo("../conf/client.conf");

   int result = 0;

   if ((result = client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

   return 0;
}

int Utility::logout(Sector& client)
{
   client.logout();
   client.close();
   return 0;
}

