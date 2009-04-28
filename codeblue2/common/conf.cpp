/*****************************************************************************
Copyright � 2006 - 2009, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Sector: A Distributed Storage and Computing Infrastructure

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

Sector is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

Sector is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 04/22/2009
*****************************************************************************/


#include "conf.h"
#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <cstring>
#include <cstdlib>

using namespace std;

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

      if (strlen(buf) == 0)
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

         if ((strlen(buf) == 0) || ('\t' != buf[0]))
            break;

         str = buf;
         if (NULL == (str = getToken(str, token)))
         {
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

int MasterConf::init(const string& path)
{
   m_iServerPort = 2237;	// CBFS

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
         strcpy(buf, param.m_vstrValue[0].c_str());

         unsigned int i = 0;
         for (; i < strlen(buf); ++ i)
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
      else
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

int SlaveConf::init(const string& path)
{
   m_strMasterHost = "";
   m_iMasterPort = 0;
   m_llMaxDataSize = -1;
   m_iMaxServiceNum = 2;
   m_strLocalIP = "";
   m_strPublicIP = "";
   
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
         strcpy(buf, param.m_vstrValue[0].c_str());

         unsigned int i = 0;
         for (; i < strlen(buf); ++ i)
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
         m_llMaxDataSize = atoll(param.m_vstrValue[0].c_str()) * 1024 * 1024;
      else if ("MAX_SERVICE_INSTANCE" == param.m_strName)
         m_iMaxServiceNum = atoi(param.m_vstrValue[0].c_str());
      else if ("LOCAL_ADDRESS" == param.m_strName)
         m_strLocalIP = param.m_vstrValue[0];
      else if ("PUBLIC_ADDRESS" == param.m_strName)
         m_strPublicIP = param.m_vstrValue[0];
      else
         cerr << "unrecongnized system parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}

int ClientConf::init(const std::string& path)
{
   m_strUserName = "";
   m_strPassword = "";
   m_strMasterIP = "";
   m_iMasterPort = 0;
   m_strCertificate = "";

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
         strcpy(buf, param.m_vstrValue[0].c_str());

         unsigned int i = 0;
         for (; i < strlen(buf); ++ i)
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
      else
         cerr << "unrecongnized client.conf parameter: " << param.m_strName << endl;
   }

   parser.close();

   return 0;
}
