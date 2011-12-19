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

#include <vector>

#include "gmp.h"
#include "sector.h"
#include "topology.h"

namespace sector
{

/* 
   Sector Msg Format:
   Request Type
   User Key
   User Token
   Type-Specific Data
*/

class SectorMsg: public CUserMessage
{
public:
   SectorMsg() {m_iDataLength = m_iHdrSize;}
   virtual ~SectorMsg() {}

   int32_t getType() const;
   void setType(const int32_t& type);
   int32_t getKey() const;
   void setKey(const int32_t& key);
   char* getData() const;
   void setData(const int& offset, const char* data, const int& len);

   virtual int serialize() { return 0; }
   virtual int deserialize() { return 0; }

   void serializeHdr();
   void deserializeHdr();

public:
   static const int m_iHdrSize = 8;
   int m_iKey;
   int m_iToken;
   int m_iType;
};

struct CliLoginReq: public SectorMsg
{
   int32_t m_iCmd;
   std::string m_strUser;
   std::string m_strPasswd;
   int32_t m_iKey;
   int32_t m_iGMPPort;
   int32_t m_iDataPort;
   char m_pcCryptoKey[16];
   char m_pcCryptoIV[8];

   virtual int serialize();
   virtual int deserialize();
};

struct CliLoginRes: public SectorMsg
{
   int32_t m_iKey;
   int32_t m_iToken;
   Topology m_Topology;
   std::vector<Address> m_Masters;

   virtual int serialize();
   virtual int deserialize();
};

/*
struct FSCmd
{
   string m_strPath1;			// File/Dir to be processed
   string m_strPath2;			// Destination file/Dir for "rename" etc.
   std::vector<int32_t> m_vOptions;	// Options such as file open mode, time stamp, etc.

   virtual int serialize();
   virtual int deserialize();
};

struct FSResponse
{
   int32_t m_iCode;
   vector<SNode> m_vFileList;

   virtual int serialize();
   virtual int deserialize();
};
*/

}  // namespace sector

#endif
