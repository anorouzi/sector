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


#ifndef __SECTOR_LOG_H__
#define __SECTOR_LOG_H__

#include <udt.h>
#include <fstream>
#include <map>
#include <osportable.h>

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

struct LogStringTag
{
   LogStringTag() {}
   LogStringTag(const int tag, const int level = LogLevel::SCREEN): m_iTag(tag), m_iLevel(level) {}

   int m_iTag;
   int m_iLevel;
};

struct LogStart: LogStringTag
{
   LogStart(const int level = LogLevel::SCREEN) {m_iTag = LogTag::START; m_iLevel = level;}
};

struct LogEnd: LogStringTag
{
   LogEnd() {m_iTag = LogTag::END;};
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
   void logUserActivity(const char* user, const int uid, const char* ip, const char* cmd, const char* file, const int res, const char* info = NULL, const int level = 1);

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

   CMutex m_LogLock;

   std::map<int, LogString> m_mStoredString;
};

#endif
