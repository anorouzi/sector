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
#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <sector.h>

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
