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
   Yunhong Gu, last updated 06/29/2008
*****************************************************************************/


#ifndef __SECTOR_LOG_H__
#define __SECTOR_LOG_H__

#include <pthread.h>
#include <fstream>
#include <map>

struct LogLevel
{
   static const int LEVEL_0 = 0;
   static const int LEVEL_1 = 1;
   static const int LEVEL_2 = 2;
   static const int LEVEL_3 = 3;
   static const int LEVEL_4 = 4;
   static const int LEVEL_5 = 5;
   static const int LEVEL_6 = 6;
   static const int LEVEL_7 = 7;
   static const int LEVEL_8 = 8;
   static const int LEVEL_9 = 9;
   static const int SCREEN = 10;
};

struct LogTag
{
   static const int START = 0;
   static const int END = 1;
};

struct LogString
{
   int m_iLevel;
   std::string m_strLog;
};

class LogStringTag
{
public:
   LogStringTag(const int tag, const int level = LogLevel::SCREEN);

public:
   int m_iLevel;
   int m_iTag;
};

class SectorLog
{
public:
   SectorLog();
   ~SectorLog();

public:
   int init(const char* path);
   void close();

   void setLevel(const int level);

   void insert(const char* text, const int level = 1);
   void logUserActivity(const char* user, const char* ip, const char* cmd, const char* file, const char* res, const char* slave, const int level = 1);

   SectorLog& operator<<(const LogStringTag& tag);
   SectorLog& operator<<(const std::string& message);
   SectorLog& operator<<(const int64_t& val);

private:
   void insert_(const char* text, const int level = 1);
   void checkLogFile();

private:
   int m_iLevel;
   int m_iDay;

   std::string m_strLogPath;
   std::ofstream m_LogFile;

   pthread_mutex_t m_LogLock;

   std::map<int, LogString> m_mStoredString;
};

#endif
