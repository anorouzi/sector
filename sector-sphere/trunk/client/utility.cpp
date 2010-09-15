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
   Yunhong Gu, last updated 09/14/2010
*****************************************************************************/

#include <conf.h>
#include <utility.h>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;

bool WildCard::isWildCard(const string& path)
{
   if (path.find('*') != string::npos)
      return true;

   if (path.find('?') != string::npos)
      return true;

   return false;
}

bool WildCard::match(const string& card, const string& path)
{
   const char* p = card.c_str();
   const char* q = path.c_str();

   unsigned int i = 0;
   unsigned int j = 0;
   while ((i < card.length()) && (j < path.length()))
   {
      switch (p[i])
      {
      case '*':
         if (i == card.length() - 1)
            return true;

         while (p[i] == '*')
            ++ i;

         for (; j < path.length(); ++ j)
         {
            if (((q[j] == p[i]) || (p[i] == '?') ) && match(p + i, q + j))
               return true;
         }

         return false;

      case '?':
         break;

      default:
         if (p[i] != q[j])
            return false;
      }

      ++ i;
      ++ j;
   }

   if ((i != card.length()) || (j != path.length()))
      return false;

   return true;
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

   struct stat t;
   if (stat(m_ClientConf.m_strCertificate.c_str(), &t) < 0)
   {
      if (stat((sector_home + "/conf/master_node.cert").c_str(), &t) == 0)
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

int CmdLineParser::parse(int argc, char** argv)
{
   m_vSFlags.clear();
   m_mDFlags.clear();
   m_vParams.clear();

   bool dash = false;
   string key;
   for (int i = 1; i < argc; ++ i)
   {
      if (argv[i][0] == '-')
      {
         if ((strlen(argv[i]) >= 2) && (argv[i][1] == '-'))
         {
            // --f
            m_vSFlags.push_back(argv[i] + 2);
            dash = false;
         }
         else
         {
            // -f [val]
            dash = true;
            key = argv[i] + 1;
            m_mDFlags[key] = "";
         }
      }
      else
      {
         if (!dash)
         {
            // param
            m_vParams.push_back(argv[i]);
         }
         else
         {
            // -f val
            m_mDFlags[key] = argv[i];
            dash = false;
         }
      }
   }

   return 0;
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
