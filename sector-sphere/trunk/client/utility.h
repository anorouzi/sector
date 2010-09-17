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

#ifndef WIN32
   #define SECTOR_API
#else
   #ifdef SECTOR_EXPORTS
      #define SECTOR_API __declspec(dllexport)
   #else
      #define SECTOR_API __declspec(dllimport)
   #endif
   #pragma warning( disable: 4251 )
#endif

#include <conf.h>
#include <sector.h>

class SECTOR_API WildCard
{
public:
   static bool isWildCard(const std::string& path);
   static bool match(const std::string& card, const std::string& path);
};

class SECTOR_API Session
{
public:
   int loadInfo(const char* conf = NULL);

public:
   ClientConf m_ClientConf;
};

class CmdLineParser
{
public:
   int parse(int argc, char** argv);

public:
   std::vector<std::string> m_vSFlags;           // --f
   std::map<std::string, std::string> m_mDFlags; // -f val
   std::vector<std::string> m_vParams;           // parameter
};

class Utility
{
public:
   static void print_error(int code);
   static int login(Sector& client);
   static int logout(Sector& client);
};
