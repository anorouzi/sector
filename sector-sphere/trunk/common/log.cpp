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


#include "log.h"
#include <common.h>
#include <time.h>
#include <string>
#include <cstring>
#include <iostream>

using namespace std;

LogStringTag::LogStringTag(const int tag, const int level)
{
   m_iTag = tag;
   m_iLevel = level;
}

SectorLog::SectorLog():
m_iLevel(1),
m_iDay(-1)
{
}

SectorLog::~SectorLog()
{
}

int SectorLog::init(const char* path)
{
   m_strLogPath = path;
   time_t t = time(NULL);
   tm date;
#ifndef WIN32
   gmtime_r(&t, &date);
#else
   gmtime_s(&date, &t);
#endif
   m_iDay = date.tm_mday;

   char fn[32];
   sprintf(fn, "%d.%d.%d.log", date.tm_mon + 1, date.tm_mday, date.tm_year + 1900);

   m_LogFile.open((m_strLogPath + "/" + fn).c_str(), ios::app);

   if (m_LogFile.bad() || m_LogFile.fail())
      return -1;

   return 0;
}

void SectorLog::close()
{
   m_LogFile.close();
}

void SectorLog::setLevel(const int level)
{
   if (level >= 0)
      m_iLevel = level;
}

SectorLog& SectorLog::operator<<(const LogStringTag& tag)
{
   CGuardEx lg(m_LogLock);

   #ifndef WIN32
   int key = pthread_self();
   #else
   int key = GetCurrentThreadId();
   #endif

   if (tag.m_iTag == LogTag::START)
   {
      LogString ls;
      ls.m_iLevel = tag.m_iLevel;
      m_mStoredString[key] = ls;
   }
   else if (tag.m_iTag == LogTag::END)
   {
      map<int, LogString>::iterator i = m_mStoredString.find(key);
      if (i != m_mStoredString.end())
      {
         insert_(i->second.m_strLog.c_str(), i->second.m_iLevel);
         m_mStoredString.erase(i);
      }
   }

   return *this;
}

SectorLog& SectorLog::operator<<(const std::string& message)
{
   CGuardEx lg(m_LogLock);

   #ifndef WIN32
   int key = pthread_self();
   #else
   int key = GetCurrentThreadId();
   #endif

   map<int, LogString>::iterator i = m_mStoredString.find(key);
   if (i != m_mStoredString.end())
   {
      i->second.m_strLog += message;
   }

   return *this;
}

SectorLog& SectorLog::operator<<(const int64_t& val)
{
   CGuardEx lg(m_LogLock);

   #ifndef WIN32
   int key = pthread_self();
   #else
   int key = GetCurrentThreadId();
   #endif

   map<int, LogString>::iterator i = m_mStoredString.find(key);
   if (i != m_mStoredString.end())
   {
      char buf[64];
      sprintf(buf, "%lld", (long long)val);
      i->second.m_strLog += buf;
   }

   return *this;
}

void SectorLog::insert_(const char* text, const int level)
{
   #ifdef DEBUG
   if (level == LogLevel::SCREEN)
   {
      cout << text << endl;
      return;
   }
   #endif

   if (level > m_iLevel)
      return;

   checkLogFile();

   time_t t = time(NULL);
   char ct[64];
   sprintf(ct, "%s", ctime(&t));
   ct[strlen(ct) - 1] = '\0';
   m_LogFile << ct << "\t" << text << endl;
   m_LogFile.flush();
}

void SectorLog::insert(const char* text, const int level)
{
   CGuardEx lg(m_LogLock);
   insert_(text, level);
}

void SectorLog::logUserActivity(const char* user, const char* ip, const char* cmd, const char* file, const int res, const char* info, const int level)
{
   if (level > m_iLevel)
      return;

   char* text = new char[128 + strlen(file)];
   if (res >= 0)
      sprintf(text, "user request => USER: %s  IP: %s  CMD: %s  FILE/DIR: %s  RESULT: %d  SLAVE: %s", user, ip, cmd, file, res, info);
   else
      sprintf(text, "user request => USER: %s  IP: %s  CMD: %s  FILE/DIR: %s  RESULT: %d", user, ip, cmd, file, res);
   insert(text, level);
   delete [] text;
}

void SectorLog::checkLogFile()
{
   time_t t = time(NULL);
   tm date;
#ifndef WIN32
   gmtime_r(&t, &date);
#else
   gmtime_s(&date, &t);
#endif
   if (date.tm_mday == m_iDay)
      return;

   m_iDay = date.tm_mday;

   char fn[32];
   sprintf(fn, "%d.%d.%d.log", date.tm_mon + 1, date.tm_mday, date.tm_year + 1900);

   m_LogFile.close();
   m_LogFile.open((m_strLogPath + "/" + fn).c_str(), ios::app);
}
