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
#include "message.h"

int32_t SectorMsg::getType() const
{
   return *(int32_t*)m_pcBuffer;
}

void SectorMsg::setType(const int32_t& type)
{
   *(int32_t*)m_pcBuffer = type;
}

int32_t SectorMsg::getKey() const
{
   return *(int32_t*)(m_pcBuffer + 4);
}

void SectorMsg::setKey(const int32_t& key)
{
   *(int32_t*)(m_pcBuffer + 4) = key;
}

char* SectorMsg::getData() const
{
   return m_pcBuffer + m_iHdrSize;
}

void SectorMsg::setData(const int& offset, const char* data, const int& len)
{
   while (m_iHdrSize + offset + len > m_iBufLength)
      resize(m_iBufLength << 1);

   memcpy(m_pcBuffer + m_iHdrSize + offset, data, len);

   if (m_iDataLength < m_iHdrSize + offset + len)
      m_iDataLength = m_iHdrSize + offset + len;
}
