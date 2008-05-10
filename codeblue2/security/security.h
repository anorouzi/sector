/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/04/2008
*****************************************************************************/

#ifndef __SECURITY_H__
#define __SECURITY_H__

#include "ssltransport.h"
#include <map>
#include <vector>
#include <string>
#include <stdint.h>

struct Key
{
   char m_pcIP[64];
   int32_t m_iPort;
   int32_t m_iPass;		// pass phrase
   int32_t m_iValidPeriod;	// seconds
};

class ACL
{
public:
   ACL();
   ~ACL();

public:
   int init(const char* aclfile);
   bool match(const char* ip);

   int addIPRange(const char* ip);

private:
   struct IPRange
   {
      uint32_t m_uiIP;
      uint32_t m_uiMask;
   };

   std::vector<IPRange> m_vIPList;
};

class User
{
public:
   int init(const char* name, const char* ufile);
   int serialize(const std::vector<std::string>& input, std::string& buf);

public:
   int32_t m_iID;
   std::string m_strName;
   std::string m_strPassword;
   std::vector<std::string> m_vstrReadList;
   std::vector<std::string> m_vstrWriteList;
   std::vector<std::string> m_vstrExecList;
   ACL m_ACL;
   int64_t m_llQuota;
};

class Shadow
{
public:
   Shadow() {}
   ~Shadow() {}

   int init(const std::string& path);
   User* match(const char* name, const char* password, const char* ip);

private:
   std::map<std::string, User> m_mUser;
};

class SServer
{
public:
   SServer();
   ~SServer() {}

public:
   int init(const int& port, const char* cert, const char* key);
   int loadACL(const char* aclfile);
   int loadShadowFile(const char* shadowfile);
   void close();

   void run();

private:
   int32_t m_iKeySeed;
   int32_t generateKey();

   struct Param
   {
      std::string ip;
      int port;
      SServer* sserver;
      SSLTransport* ssl; 
   };
   static void* process(void* p);

private:
   int m_iPort;
   SSLTransport m_SSL;
   ACL m_ACL;
   Shadow m_Shadow;
};

class SClient
{
public:
   SClient() {}
   ~SClient() {}

public:
   int init(const char* cert);
   void close();

   int connect(char* ip, int port);
   int sendReq(const char* name, const char* password);
   int recvRes(int32_t& code);

private:
   SSLTransport m_SSL;
};

#endif
