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
#include <iostream>
#include "message.h"

using namespace std;
using namespace sector;

int32_t SectorMsg::getType() const
{
   return *(int32_t*)m_pcBuffer;
}

void SectorMsg::setType(const int32_t& type)
{
   m_iType = type;
   *(int32_t*)m_pcBuffer = m_iType;
}

int32_t SectorMsg::getKey() const
{
   return *(int32_t*)(m_pcBuffer + 4);
}

void SectorMsg::setKey(const int32_t& key)
{
   m_iKey = key;
   *(int32_t*)(m_pcBuffer + 4) = m_iKey;
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
   *(int32_t*)m_pcBuffer = m_iType;
   *(int32_t*)(m_pcBuffer + 4) = m_iKey;
}

void SectorMsg::deserializeHdr()
{
   m_iType = *(int32_t*)m_pcBuffer;
   m_iKey = *(int32_t*)(m_pcBuffer + 4);
}

namespace
{

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

void PRINT_STR(char** p, const string& val)
{
   *(int32_t*)(*p) = val.size() + 1;
   strcpy(*p + 4, val.c_str());
   *p += 4 + val.size() + 1;
}

void RESTORE_STR(char** p, string& val)
{
   const int size = *(int32_t*)(*p);
   // Strictly speaking, *p+4 cannot contail '0', which introduces
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
      sizeof(int32_t) + m_strUser.size() + 1 + m_strPasswd.size() + 1 +
      sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + 16 + 8 +
      8 * sizeof(int32_t);

   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iCmd);
   PRINT_STR(&p, m_strUser);
   PRINT_STR(&p, m_strPasswd);
   PRINT_INT32(&p, m_iKey);
   PRINT_INT32(&p, m_iGMPPort);
   PRINT_INT32(&p, m_iDataPort);
   PRINT_CHAR_ARR(&p, m_pcCryptoKey, 16);
   PRINT_CHAR_ARR(&p, m_pcCryptoIV, 8);

   return m_iDataLength;
}

int CliLoginReq::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iCmd);
   RESTORE_STR(&p, m_strUser);
   RESTORE_STR(&p, m_strPasswd);
   RESTORE_INT32(&p, m_iKey);
   RESTORE_INT32(&p, m_iGMPPort);
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
   int master_len = 80 * m_Masters.size();

   m_iDataLength = m_iHdrSize + topo_len + master_len;
   if (m_iDataLength > m_iBufLength)
      resize(m_iDataLength);

   serializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   PRINT_INT32(&p, m_iKey);
   PRINT_INT32(&p, m_iToken);
   PRINT_CHAR_ARR(&p, topo_buf, topo_len);
   PRINT_ADDR_ARR(&p, m_Masters);

   delete [] topo_buf;

   m_iDataLength = p - m_pcBuffer;
   return m_iDataLength;
}

int CliLoginRes::deserialize()
{
   deserializeHdr();
   char* p = m_pcBuffer + m_iHdrSize;
   RESTORE_INT32(&p, m_iKey);
   RESTORE_INT32(&p, m_iToken);

   int32_t topo_size = *(int32_t*)p;
   p += 4;
   m_Topology.deserialize(p, topo_size);
   p += topo_size;

   RESTORE_ADDR_ARR(&p, m_Masters);

   return 0;
}
