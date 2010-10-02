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
#else
    #define _WIN32_IE 0x501
    #include <shlobj.h>

    #include <direct.h>
    #include "statfs.h"
    #include "common.h"
#endif

#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>

#include "conf.h"

using namespace std;

int ConfLocation::locate(string& loc)
{
   // search for configuration files from 1) $SECTOR_HOME, 2) ../, or 3) /opt/sector

   struct stat t;

#ifndef WIN32
   char* system_env = getenv("SECTOR_HOME");
   if (NULL != system_env)
      loc = system_env;
#else
   char system_env[MAX_PATH+1]="";
   size_t req_size = 0;
   errno_t err = getenv_s (&req_size, system_env, "SECTOR_HOME");
   if (err == 0 && system_env[0] != '\0')
      loc = system_env;

   win_to_unix_path (loc);

#ifdef _DEBUG
    printf ("Config: %s\n", (loc + "/conf").c_str());
#endif
#endif

   if (stat((loc + "/conf").c_str(), &t) == 0)
      return 0;

   if (stat("../conf", &t) == 0)
   {
      loc = "../";
   }
#ifndef WIN32
   else if (stat("/opt/sector/conf", &t) == 0)
   {
      loc = "/opt/sector";
   }
#endif
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
   while ('\t' != *p && '\0' != *p)
   {
      token.append(1, *p);
      ++ p;
   }

   // trim trailing blank spcaces if any
   size_t endpos = token.find_last_not_of(' ');
   if (string::npos == endpos)
       token = "";    // nothing but blanks!
   else if (endpos+1 != token.length())
       token = token.substr( 0, endpos);

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
#ifdef WIN32
        win_to_unix_path (m_strHomeDir);

        struct stat t;
        if (stat(m_strHomeDir.c_str(), &t) != 0) 
        {
            char szPath[MAX_PATH]="";
            if (SUCCEEDED(SHGetFolderPath(NULL, 
                                         CSIDL_COMMON_APPDATA | CSIDL_FLAG_CREATE, 
                                         NULL, 
                                         SHGFP_TYPE_DEFAULT, 
                                         szPath)))
            {
                m_strHomeDir.assign (szPath);
                char first = param.m_vstrValue[0][0];
                if (first != '/' && first != '\\')
                    m_strHomeDir += "\\";
                m_strHomeDir.append(param.m_vstrValue[0]);
                win_to_unix_path (m_strHomeDir);
            }
        }

#ifdef _DEBUG
         printf ("DATA_DIRECTORY: %s\n", m_strHomeDir.c_str());
#endif
#endif
         char last = m_strHomeDir.c_str()[m_strHomeDir.length() - 1];
         if (last != '/' && last != '\\')
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
#ifdef WIN32
        win_to_unix_path (m_strHomeDir);

        struct stat t;
        if (stat(m_strHomeDir.c_str(), &t) != 0) 
        {
            char szPath[MAX_PATH]="";
            if (SUCCEEDED(SHGetFolderPath(NULL, 
                                         CSIDL_COMMON_APPDATA | CSIDL_FLAG_CREATE, 
                                         NULL, 
                                         SHGFP_TYPE_DEFAULT, 
                                         szPath)))
            {
                m_strHomeDir.assign (szPath);
                char first = param.m_vstrValue[0][0];
                if (first != '/' && first != '\\')
                    m_strHomeDir += "\\";
                m_strHomeDir.append(param.m_vstrValue[0]);
                win_to_unix_path (m_strHomeDir);
            }
        }

#ifdef _DEBUG
         printf ("DATA_DIRECTORY: %s\n", m_strHomeDir.c_str());
#endif
#endif
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
