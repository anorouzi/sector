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

#include <stdint.h>
#include <string.h>

#include "message.h"

using namespace std;
using namespace sector;

int32_t SectorMsg::getType() const
{
   return *(int32_t*)(m_pcBuffer + 12);
}

void SectorMsg::setType(const int32_t& type)
{
   m_iType = type;
   *(int32_t*)(m_pcBuffer + 12) = m_iType;
}

int32_t SectorMsg::getKey() const
{
   return *(int32_t*)m_pcBuffer;
}

void SectorMsg::setKey(const int32_t& key)
{
   m_iKey = key;
   *(int32_t*)m_pcBuffer = m_iKey;
}

char* SectorMsg::getData() const
{
   return m_pcBuffer + m_iHdrSize;
}

void SectorMsg::setData(const int& offset, const char* data, const int& len)
{
   const int end_pos = m_iHdrSize + offset + len;

   if (end_pos > m_iBufLength)
      resize(end_pos);

   memcpy(m_pcBuffer + m_iHdrSize + offset, data, len);

   if (m_iDataLength < end_pos)
      m_iDataLength = end_pos;
}

void SectorMsg::serializeHdr()
{
   *(int32_t*)m_pcBuffer = m_iKey;
   *(int32_t*)(m_pcBuffer + 4) = m_iToken;
   *(int32_t*)(m_pcBuffer + 8) = m_iVersion;
   *(int32_t*)(m_pcBuffer + 12) = m_iType;
}

void SectorMsg::deserializeHdr()
{
   m_iKey = *(int32_t*)m_pcBuffer;
   m_iToken = *(int32_t*)(m_pcBuffer + 4);
   m_iVersion = *(int32_t*)(m_pcBuffer + 8);
   m_iType = *(int32_t*)(m_pcBuffer + 12);
}

void SectorMsg::replicate(const SectorMsg& msg)
{
   m_iDataLength = msg.m_iDataLength;
   resize(m_iDataLength);
   memcpy(m_pcBuffer, msg.m_pcBuffer, m_iDataLength);
   deserialize();
   // These two fields are not serialized.
   m_strSourceIP = msg.m_strSourceIP;
   m_iSourcePort = msg.m_iSourcePort;
}

namespace
{

// TODO: add boundary check.

void PRINT_INT32(char** p, const int32_t& val)
{
   *(int32_t*)(*p) = 4;
   *(int32_t*)(*p + 4) = val;
   *p += 8;
}

void RESTORE_INT32(char** p, int32_t& val)
{
   val = *(int32_t*)(*p + 4);
   *p += 8;
}

void PRINT_INT64(char** p, const int64_t& val)
{
   *(int32_t*)(*p) = 4;
   *(int64_t*)(*p + 4) = val;
   *p += 12;
}

void RESTORE_INT64(char** p, int64_t& val)
{
   val = *(int64_t*)(*p + 4);
   *p += 12;
}

void PRINT_STR(char** p, const string& val)
{
   *(int32_t*)(*p) = val.size() + 1;
   strncpy(*p + 4, val.c_str(), val.size() + 1);
   *p += 4 + val.size() + 1;
}

void RESTORE_STR(char** p, string& val)
{
   const int size = *(int32_t*)(*p);
   // Strictly speaking, *p+4 cannot contain '0', which introduces
   // unnessary limitation. Should fix this later.
   val = *p + 4;
   *p += size + 4;
}

void PRINT_CHAR_ARR(char** p, const char* val, int len)
{
   *(int32_t*)(*p) = len;
   memcpy(*p + 4, val, len);
   *p += len + 4;
}

void RESTORE_CHAR_ARR(char** p, char* val, int& len)
{
   len = *(int32_t*)(*p);
   memcpy(val, *p + 4, len);
   *p += len + 4;
}

void PRINT_ADDR(char** p, const Address& addr)
{
   PRINT_STR(p, addr.m_strIP);
   PRINT_INT32(p, addr.m_iPort);
}

void RESTORE_ADDR(char** p, Address& addr)
{
   RESTORE_STR(p, addr.m_strIP);
   RESTORE_INT32(p, addr.m_iPort);
}

void PRINT_ADDR_ARR(char** p, const vector<Address>& addr)
{
   *(int32_t*)(*p) = addr.size();
   *p += 4;
   for (vector<Address>::const_iterator i = addr.begin(); i != addr.end(); ++ i)
   {
      PRINT_ADDR(p, *i);
   }
}

void RESTORE_ADDR_ARR(char** p, vector<Address>& addr)
{
   int32_t size = *(int32_t*)(*p);
   *p += 4;
   addr.resize(size);
   for (vector<Address>::iterator i = addr.begin(); i != addr.end(); ++ i)
   {
      RESTORE_ADDR(p, *i);
   }
}

}  // anonymous namespace


int CliLoginReq::serialize()
{
   m_iDataLength = m_iHdrSize +
      m_strUser.size() + 1 +
      m_strPasswd.size() + 1 +
      sizeof(int32_t) + sizeof(int32_t) + 16 + 8 +
      6 * sizeof(int32_t);

   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_STR(&p, m_strUser);
   PRINT_STR(&p, m_strPasswd);
   PRINT_INT32(&p, m_iPort);
   PRINT_INT32(&p, m_iDataPort);
   PRINT_CHAR_ARR(&p, m_pcCryptoKey, 16);
   PRINT_CHAR_ARR(&p, m_pcCryptoIV, 8);

   return m_iDataLength;
}

int CliLoginReq::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_STR(&p, m_strUser);
   RESTORE_STR(&p, m_strPasswd);
   RESTORE_INT32(&p, m_iPort);
   RESTORE_INT32(&p, m_iDataPort);
   int len;
   RESTORE_CHAR_ARR(&p, m_pcCryptoKey, len);
   RESTORE_CHAR_ARR(&p, m_pcCryptoIV, len);

   return 0;
}

int CliLoginRes::serialize()
{
   int topo_len = m_Topology.getTopoDataSize();
   char* topo_buf = new char[topo_len];
   m_Topology.serialize(topo_buf, topo_len);

   // TODO: better calculation.
   int master_len = 80 * m_mMasters.size();

   m_iDataLength = m_iHdrSize + topo_len + master_len;
   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iCliKey);
   PRINT_INT32(&p, m_iCliToken);
   PRINT_CHAR_ARR(&p, topo_buf, topo_len);
   PRINT_INT32(&p, m_iRouterKey);
   PRINT_INT32(&p, m_mMasters.size());
   for (map<uint32_t, Address>::const_iterator i = m_mMasters.begin(); i != m_mMasters.end(); ++ i)
   {
      PRINT_INT32(&p, i->first);
      PRINT_ADDR(&p, i->second);
   }

   delete [] topo_buf;

   m_iDataLength = p - m_pcBuffer;
   return m_iDataLength;
}

int CliLoginRes::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iCliKey);
   RESTORE_INT32(&p, m_iCliToken);

   int32_t topo_size = *(int32_t*)p;
   p += 4;
   m_Topology.deserialize(p, topo_size);
   p += topo_size;

   RESTORE_INT32(&p, m_iRouterKey);

   int master_num = 0;
   RESTORE_INT32(&p, master_num);
   for (int i = 0; i < master_num; ++ i)
   {
      int key;
      Address addr;
      RESTORE_INT32(&p, key);
      RESTORE_ADDR(&p, addr);
      m_mMasters[key] = addr;
   }

   return 0;
}

int SlvLoginReq::serialize()
{
   m_iDataLength = m_iHdrSize +
      m_strHomeDir.size() + 1 +
      m_strBase.size() + 1 +
      sizeof(int32_t) +
      sizeof(int32_t) +
      sizeof(int64_t) +
      sizeof(int32_t) +
      6 * sizeof(int32_t);

   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_STR(&p, m_strHomeDir);
   PRINT_STR(&p, m_strBase);
   PRINT_INT32(&p, m_iPort);
   PRINT_INT32(&p, m_iDataPort);
   PRINT_INT64(&p, m_llAvailDisk);
   PRINT_INT32(&p, m_iSlaveID);

   return 0;
}

int SlvLoginReq::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_STR(&p, m_strHomeDir);
   RESTORE_STR(&p, m_strBase);
   RESTORE_INT32(&p, m_iPort);
   RESTORE_INT32(&p, m_iDataPort);
   RESTORE_INT64(&p, m_llAvailDisk);
   RESTORE_INT32(&p, m_iSlaveID);
   return 0;
}

int SlvLoginRes::serialize()
{
   // TODO: better calculation.
   int master_len = 80 * m_vMasters.size();

   m_iDataLength = m_iHdrSize + sizeof(int32_t) + master_len + 2 * sizeof(int32_t);
   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iSlaveID);
   PRINT_INT32(&p, m_iRouterKey);
   PRINT_ADDR_ARR(&p, m_vMasters);

   m_iDataLength = p - m_pcBuffer;
   return 0;  
}

int SlvLoginRes::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iSlaveID);
   RESTORE_INT32(&p, m_iSlaveID);
   RESTORE_ADDR_ARR(&p, m_vMasters);
   return 0;
}

int MstLoginReq::serialize()
{
   m_iDataLength = m_iHdrSize + sizeof(int32_t) + sizeof(int32_t) + 2 * sizeof(int32_t);
   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iServerPort);
   PRINT_INT32(&p, m_iRouterKey);
   return 0;
}

int MstLoginReq::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iServerPort);
   RESTORE_INT32(&p, m_iRouterKey);
   return 0;
}

int MstLoginRes::serialize()
{
   m_iDataLength = m_iHdrSize +
                   sizeof(int32_t) +
                   sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + m_iSlaveBufSize +
                   sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + m_iUserBufSize;
   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iResponse);
   PRINT_INT32(&p, m_iSlaveNum);
   PRINT_INT32(&p, m_iSlaveBufSize);
   PRINT_CHAR_ARR(&p, m_pcSlaveBuf, m_iSlaveBufSize);
   PRINT_INT32(&p, m_iUserNum);
   PRINT_INT32(&p, m_iUserBufSize);
   PRINT_CHAR_ARR(&p, m_pcUserBuf, m_iUserBufSize);
   return 0;
}

int MstLoginRes::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iResponse);
   RESTORE_INT32(&p, m_iSlaveNum);
   RESTORE_INT32(&p, m_iSlaveBufSize);
   RESTORE_CHAR_ARR(&p, m_pcSlaveBuf, m_iSlaveBufSize);
   RESTORE_INT32(&p, m_iUserNum);
   RESTORE_INT32(&p, m_iUserBufSize);
   RESTORE_CHAR_ARR(&p, m_pcUserBuf, m_iUserBufSize);
   return 0;
}
