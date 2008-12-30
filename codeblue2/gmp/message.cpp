/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Group Messaging Protocol (GMP)

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

GMP is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option)
any later version.

GMP is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along
with this program.  If not, see <http://www.gnu.org/licenses/>.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 12/29/2008
*****************************************************************************/

#include <string>
#include <message.h>

using namespace std;

CUserMessage::CUserMessage():
m_iDataLength(0),
m_iBufLength(1500)
{
   m_pcBuffer = new char[m_iBufLength];
}

CUserMessage::CUserMessage(const int& len):
m_iDataLength(0),
m_iBufLength(len)
{
   if (m_iBufLength < 8)
      m_iBufLength = 8;

   m_pcBuffer = new char[m_iBufLength];
}

CUserMessage::CUserMessage(const CUserMessage& msg):
m_iDataLength(msg.m_iDataLength),
m_iBufLength(msg.m_iBufLength)
{
   m_pcBuffer = new char[m_iBufLength];
   memcpy(m_pcBuffer, msg.m_pcBuffer, m_iDataLength + 4);
}

CUserMessage::~CUserMessage()
{
   delete [] m_pcBuffer;
}

int CUserMessage::resize(const int& len)
{
   m_iBufLength = len;

   if (m_iBufLength < m_iDataLength)
      m_iBufLength = m_iDataLength;

   char* temp = new char[m_iBufLength];
   memcpy(temp, m_pcBuffer, m_iDataLength);
   delete [] m_pcBuffer;
   m_pcBuffer = temp;

   return m_iBufLength;
}


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
