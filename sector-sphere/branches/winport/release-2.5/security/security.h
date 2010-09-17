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

#ifndef __SECURITY_H__
#define __SECURITY_H__

#include <ssltransport.h>
#include <map>
#include <vector>
#include <string>
#include <stdint.h>

#ifdef WIN32
    #ifdef SECURITY_EXPORTS
        #define SECURITY_API __declspec(dllexport)
    #else
        #define SECURITY_API __declspec(dllimport)
    #endif
#else
    #define SECURITY_API
#endif


struct Key
{
   char m_pcIP[64];
   int32_t m_iPort;
   int32_t m_iPass;		// pass phrase
   int32_t m_iValidPeriod;	// seconds
};

struct IPRange
{
   uint32_t m_uiIP;
   uint32_t m_uiMask;
};

class SECURITY_API User
{
public:
   int serialize(const std::vector<std::string>& input, std::string& buf) const;

public:
   int32_t m_iID;

   std::string m_strName;
   std::string m_strPassword;
   std::vector<IPRange> m_vACL;

   std::vector<std::string> m_vstrReadList;
   std::vector<std::string> m_vstrWriteList;
   bool m_bExec;

   int64_t m_llQuota;
};

class SECURITY_API SSource
{
public:
   virtual ~SSource() {}

public:
   virtual int loadACL(std::vector<IPRange>& acl, const void* src) = 0;
   virtual int loadUsers(std::map<std::string, User>& users, const void* src) = 0;

public:
   //virtual int passwd(const std::string& user, const std::string& password);
};

class SECURITY_API SServer
{
public:
   SServer();
   ~SServer() {}

public:
   int init(const int& port, const char* cert, const char* key);
   void close();

   int loadMasterACL(SSource* src, const void* param);
   int loadSlaveACL(SSource* src, const void* param);
   int loadShadowFile(SSource* src, const void* param);

   void run();

private:
   int32_t m_iKeySeed;
   int32_t generateKey();

private:
   struct Param
   {
      std::string ip;
      int port;
      SServer* sserver;
      SSLTransport* ssl; 
   };
#ifndef WIN32
   static void* process(void* p);
#else
   static unsigned long __stdcall process(void* p);
#endif

private:
   int m_iPort;
   SSLTransport m_SSL;

private:
   std::vector<IPRange> m_vMasterACL;
   std::vector<IPRange> m_vSlaveACL;
   std::map<std::string, User> m_mUsers;

private:
   static bool match(const std::vector<IPRange>& acl, const char* ip);
   static const User* match(const std::map<std::string, User>& users, const char* name, const char* password, const char* ip);
};

#endif
