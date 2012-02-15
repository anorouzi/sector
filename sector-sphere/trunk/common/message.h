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

#ifndef __SECTOR_MESSAGE_H__
#define __SECTOR_MESSAGE_H__

#include <iostream>
#include <vector>

#include "gmp.h"
#include "sector.h"
#include "topology.h"

namespace sector
{

/* 
   Sector Msg Format:
   Request type
   User key
   User token
   Sector version

   Type-specific data
*/

// Note: these messages can probably use protobuf, if necessary.

class SectorMsg: public CUserMessage
{
public:
   SectorMsg():
      m_iKey(0),
      m_iToken(0),
      m_iVersion(SectorVersion),
      m_iType(0) { std::cout << "base\n";}

   virtual ~SectorMsg() {}

   // To be removed. For compability only.
   int32_t getType() const;
   void setType(const int32_t& type);
   int32_t getKey() const;
   void setKey(const int32_t& key);

   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

   virtual int serialize() { return 0; }
   virtual int deserialize() { std::cout << "xxx\n"; return 0; }

   void serializeHdr();
   void deserializeHdr();

   // Copy the whole message buffer and header + source address.
   virtual void replicate(const SectorMsg& msg);

public:
   int size() { return m_iHdrSize + m_iDataLength; }

   // TODO: check if message has been serialized.

public:
   static const int m_iHdrSize = 16;
   int m_iKey;
   int m_iToken;
   int m_iVersion;
   int m_iType;

public:
   // The address from which this request comes from.
   // These fields are filled when they are received by GMP.
   // No serialization.
   std::string m_strSourceIP;
   int m_iSourcePort;
};

struct CliLoginReq: public SectorMsg
{
   CliLoginReq():
      m_strUser(),
      m_strPasswd(),
      m_iPort(),
      m_iDataPort()
   {
      m_iType = 2;
   }

   std::string m_strUser;
   std::string m_strPasswd;
   int32_t m_iPort;
   int32_t m_iDataPort;
   char m_pcCryptoKey[16];
   char m_pcCryptoIV[8];

   virtual int serialize();
   virtual int deserialize();
};

struct CliLoginRes: public SectorMsg
{
   CliLoginRes() {}

   int32_t m_iCliKey;
   int32_t m_iCliToken;
   Topology m_Topology;
   int32_t m_iRouterKey;
   std::map<uint32_t, Address> m_mMasters;

   virtual int serialize();
   virtual int deserialize();
};

struct SlvLoginReq: public SectorMsg
{
   SlvLoginReq()
   {
std::cout << "slave class\n";
      m_iType = 1;
   }

   std::string m_strHomeDir;
   std::string m_strBase;
   int32_t m_iPort;
   int32_t m_iDataPort;
   int64_t m_llAvailDisk;
   int32_t m_iSlaveID;

   virtual int serialize();
   virtual int deserialize();
};

struct SlvLoginRes: public SectorMsg
{
   SlvLoginRes() {}

   int32_t m_iSlaveID;
   int32_t m_iRouterKey;
   std::vector<Address> m_vMasters;

   virtual int serialize();
   virtual int deserialize();
};

struct MstLoginReq: public SectorMsg
{
   MstLoginReq()
   {
      m_iType = 3;
   }

   int32_t m_iServerPort;
   int32_t m_iRouterKey;

   virtual int serialize();
   virtual int deserialize();
};

struct MstLoginRes: public SectorMsg
{
   MstLoginRes(): m_pcSlaveBuf(NULL), m_pcUserBuf(NULL) {}
   ~MstLoginRes()
   {
      delete [] m_pcSlaveBuf;
      delete [] m_pcUserBuf;
   }

   int32_t m_iResponse;
   std::map<uint32_t, Address> m_mMasters;
   int32_t m_iSlaveNum;
   int32_t m_iSlaveBufSize;
   char* m_pcSlaveBuf;
   int32_t m_iUserNum;
   int32_t m_iUserBufSize;
   char* m_pcUserBuf;

   virtual int serialize();
   virtual int deserialize();
};

/*
struct FSReq
{
   string m_strPath1;			// File/Dir to be processed
   string m_strPath2;			// Destination file/Dir for "rename" etc.
   std::vector<int32_t> m_vOptions;	// Options such as file open mode, time stamp, etc.

   virtual int serialize();
   int deserialize();
};

struct FSRes
{
   int32_t m_iCode;
   vector<SNode> m_vFileList;

   virtual int serialize();
   int deserialize();
};
*/

}  // namespace sector

#endif
