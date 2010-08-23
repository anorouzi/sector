/*****************************************************************************
Copyright (c) 2005 - 2009, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu, last updated 11/25/2008
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
   pthread_mutex_init(&m_LogLock, NULL);
}

SectorLog::~SectorLog()
{
   pthread_mutex_destroy(&m_LogLock);
}

int SectorLog::init(const char* path)
{
   m_strLogPath = path;
   time_t t = time(NULL);
   tm date;
   gmtime_r(&t, &date);
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
   CGuard lg(m_LogLock);

   if (tag.m_iTag == LogTag::START)
   {
      LogString ls;
      ls.m_iLevel = tag.m_iLevel;
      int key = pthread_self();
      m_mStoredString[key] = ls;
   }
   else if (tag.m_iTag == LogTag::END)
   {
      int key = pthread_self();
      map<int, LogString>::iterator i = m_mStoredString.find(key);
      if (i != m_mStoredString.end())
      {
         insert_((i->second.m_strLog + "\n").c_str(), i->second.m_iLevel);
         m_mStoredString.erase(i);
      }
   }

   return *this;
}

SectorLog& SectorLog::operator<<(const std::string& message)
{
   CGuard lg(m_LogLock);

   int key = pthread_self();
   map<int, LogString>::iterator i = m_mStoredString.find(key);
   if (i != m_mStoredString.end())
   {
      i->second.m_strLog += message;
   }

   return *this;
}

SectorLog& SectorLog::operator<<(const int64_t& val)
{
   CGuard lg(m_LogLock);

   int key = pthread_self();
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
      cout << text;
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
   CGuard lg(m_LogLock);
   insert_(text, level);
}

void SectorLog::logUserActivity(const char* user, const char* ip, const char* cmd, const char* file, const char* res, const char* slave, const int level)
{
   if (level > m_iLevel)
      return;

   char* text = new char[128 + strlen(file)];
   sprintf(text, "user request => USER: %s  IP: %s  CMD: %s  FILE/DIR: %s  RESULT: %s  SLAVE: %s", user, ip, cmd, file, res, slave);
   insert(text, level);
   delete [] text;
}

void SectorLog::checkLogFile()
{
   time_t t = time(NULL);
   tm date;
   gmtime_r(&t, &date);
   if (date.tm_mday == m_iDay)
      return;

   m_iDay = date.tm_mday;

   char fn[32];
   sprintf(fn, "%d.%d.%d.log", date.tm_mon + 1, date.tm_mday, date.tm_year + 1900);

   m_LogFile.close();
   m_LogFile.open((m_strLogPath + "/" + fn).c_str(), ios::app);
}
