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

#ifndef WIN32
   #include <sys/socket.h>
   #include <arpa/inet.h>
   #include <unistd.h>
#endif
#include <conf.h>
#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;

int ConfLocation::locate(string& loc)
{
   // search for configuration files from 1) $SECTOR_HOME, 2) ../, or 3) /opt/sector

   struct stat t;

   char* system_env = getenv("SECTOR_HOME");
   if (NULL != system_env)
      loc = system_env;

   if (stat((loc + "/conf").c_str(), &t) == 0)
      return 0;

   if (stat("../conf", &t) == 0)
   {
      loc = "../";
   }
   else if (stat("/opt/sector/conf", &t) == 0)
   {
      loc = "/opt/sector";
   }
   else
   {
      cerr << "cannot locate Sector configurations from either $SECTOR_HOME, ../, or /opt/sector.";
      return -1;
   }

   return 0;   
}

int ConfParser::init(const string& path)
{
   m_ConfFile.open(path.c_str(), ios::in);

   if (m_ConfFile.bad() || m_ConfFile.fail())
   {
      cerr << "unable to locate or open the configuration file: " << path << endl;
      return -1;
   }

   while (!m_ConfFile.eof())
   {
      char buf[1024];
      m_ConfFile.getline(buf, 1024);

      if ('\0' == *buf)
         continue;

      //skip comments
      if ('#' == buf[0])
         continue;

      //TODO: skip lines with all blanks and tabs

      m_vstrLines.insert(m_vstrLines.end(), buf);
   }

   m_ConfFile.close();

   m_ptrLine = m_vstrLines.begin();
   m_iLineCount = 1;

   return 0;
}

void ConfParser::close()
{
}

int ConfParser::getNextParam(Param& param)
{
   //param format
   // NAME
   // < tab >value1
   // < tab >value2
   // < tab >...
   // < tab >valuen

   if (m_ptrLine == m_vstrLines.end())
      return -1;

   param.m_strName = "";
   param.m_vstrValue.clear();

   while (m_ptrLine != m_vstrLines.end())
   {
      char buf[1024];
      strcpy(buf, m_ptrLine->c_str());

      // no blanks or tabs in front of name line
      if ((' ' == buf[0]) || ('\t' == buf[0]))
      {
         cerr << "Configuration file parsing error at line " << m_iLineCount << ": " << buf << endl;
         return -1;
      }

      char* str = buf;
      string token = "";

      if (NULL == (str = getToken(str, token)))
      {
         m_ptrLine ++;
         m_iLineCount ++;
         continue;
      }
      param.m_strName = token;

      // scan param values
      m_ptrLine ++;
      m_iLineCount ++;
      while (m_ptrLine != m_vstrLines.end())
      {
         strcpy(buf, m_ptrLine->c_str());

         if (('\0' == *buf) || ('\t' != *buf))
            break;

         str = buf;
         if (NULL == (str = getToken(str, token)))
         {
            //TODO: line count is incorrect, doesn't include # and blank lines
            cerr << "Configuration file parsing error at line " << m_iLineCount << ": " << buf << endl;
            return -1;
         }

         param.m_vstrValue.insert(param.m_vstrValue.end(), token);

         m_ptrLine ++;
         m_iLineCount ++;
      }

      return param.m_vstrValue.size();
   }

   return -1;
}

char* ConfParser::getToken(char* str, string& token)
{
   char* p = str;

   // skip blank spaces
   while ((' ' == *p) || ('\t' == *p))
      ++ p;

   // nothing here...
   if ('\0' == *p)
      return NULL;

   token = "";
   while ((' ' != *p) && ('\t' != *p) && ('\0' != *p))
   {
      token.append(1, *p);
      ++ p;
   }

   return p;
}


MasterConf::MasterConf():
m_iServerPort(0),
m_strSecServIP(),
m_iSecServPort(0),
m_iMaxActiveUser(1024),
m_strHomeDir("./"),
m_iReplicaNum(1),
m_MetaType(MEMORY),
m_iSlaveTimeOut(300),
m_llSlaveMinDiskSpace(10000000000LL),
m_iClientTimeOut(600),
m_iLogLevel(1)
{
}

int MasterConf::init(const string& path)
{
   ConfParser parser;
   Param param;

   if (0 != parser.init(path))
      return -1;

   while (parser.getNextParam(param) >= 0)
   {
      if (param.m_vstrValue.empty())
         continue;

      if ("SECTOR_PORT" == param.m_strName)
         m_iServerPort = atoi(param.m_vstrValue[0].c_str());
      else if ("SECURITY_SERVER" == param.m_strName)
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
         m_strSecServIP = buf;
         m_iSecServPort = atoi(buf + i + 1);
      }
      else if ("MAX_ACTIVE_USER" == param.m_strName)
         m_iMaxActiveUser = atoi(param.m_vstrValue[0].c_str());
      else if ("DATA_DIRECTORY" == param.m_strName)
      {
         m_strHomeDir = param.m_vstrValue[0];
         if (m_strHomeDir.c_str()[m_strHomeDir.length() - 1] != '/')
            m_strHomeDir += "/";
      }
      else if ("REPLICA_NUM" == param.m_strName)
         m_iReplicaNum = atoi(param.m_vstrValue[0].c_str());
      else if ("META_LOC" == param.m_strName)
      {
         if ("MEMORY" == param.m_vstrValue[0])
            m_MetaType = MEMORY;
         else if ("DISK" == param.m_vstrValue[0])
            m_MetaType = DISK;
      }
      else if ("SLAVE_TIMEOUT" == param.m_strName)
      {
         m_iSlaveTimeOut = atoi(param.m_vstrValue[0].c_str());
         if (m_iSlaveTimeOut < 120)
            m_iSlaveTimeOut = 120;
      }
      else if ("SLAVE_MIN_DISK_SPACE" == param.m_strName)
      {
#ifndef WIN32
         m_llSlaveMinDiskSpace = atoll(param.m_vstrValue[0].c_str()) * 1000000;
#else
         m_llSlaveMinDiskSpace = _atoi64(param.m_vstrValue[0].c_str()) * 1000000;
#endif
      }
      else if ("CLIENT_TIMEOUT" == param.m_strName)
      {
         m_iClientTimeOut = atoi(param.m_vstrValue[0].c_str());
      }
      else if ("LOG_LEVEL" == param.m_strName)
      {
         m_iLogLevel = atoi(param.m_vstrValue[0].c_str());
      }
      else
      {
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
      }
   }

   parser.close();

   return 0;
}

SlaveConf::SlaveConf():
m_strMasterHost(),
m_iMasterPort(6000),
m_strHomeDir("./"),
m_llMaxDataSize(-1),
m_iMaxServiceNum(64),
m_strLocalIP(),
m_strPublicIP(),
m_iClusterID(0),
m_MetaType(MEMORY)
{
}

int SlaveConf::init(const string& path)
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
         m_strMasterHost = buf;
         m_iMasterPort = atoi(buf + i + 1);
      }
      else if ("DATA_DIRECTORY" == param.m_strName)
      {
         m_strHomeDir = param.m_vstrValue[0];
         if (m_strHomeDir.c_str()[m_strHomeDir.length() - 1] != '/')
            m_strHomeDir += "/";
      }
      else if ("MAX_DATA_SIZE" == param.m_strName)
      {
#ifndef WIN32
         m_llMaxDataSize = atoll(param.m_vstrValue[0].c_str()) * 1024 * 1024;
#else
         m_llMaxDataSize = _atoi64(param.m_vstrValue[0].c_str()) * 1024 * 1024;
#endif
      }
      else if ("MAX_SERVICE_INSTANCE" == param.m_strName)
         m_iMaxServiceNum = atoi(param.m_vstrValue[0].c_str());
      else if ("LOCAL_ADDRESS" == param.m_strName)
         m_strLocalIP = param.m_vstrValue[0];
      else if ("PUBLIC_ADDRESS" == param.m_strName)
         m_strPublicIP = param.m_vstrValue[0];
      else if ("META_LOC" == param.m_strName)
      {
         if ("MEMORY" == param.m_vstrValue[0])
            m_MetaType = MEMORY;
         else if ("DISK" == param.m_vstrValue[0])
            m_MetaType = DISK;
      }
      else
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

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
#ifndef WIN32
         m_llMaxCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
#else
         m_llMaxCacheSize = _atoi64(param.m_vstrValue[0].c_str()) * 1000000;
#endif
      }
      else if ("FUSE_READ_AHEAD_BLOCK" == param.m_strName)
      {
         m_iFuseReadAheadBlock = atoi(param.m_vstrValue[0].c_str()) * 1000000;
      }
      else if ("MAX_READ_CACHE_SIZE" == param.m_strName)
      {
#ifndef WIN32
         m_llMaxWriteCacheSize = atoll(param.m_vstrValue[0].c_str()) * 1000000;
#else
         m_llMaxWriteCacheSize = _atoi64(param.m_vstrValue[0].c_str()) * 1000000;
#endif
      }
      else
         cerr << "unrecongnized client.conf parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

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
