/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

Group Messaging Protocol (GMP)

National Center for Data Mining (NCDM)
University of Illinois at Chicago
http://www.ncdm.uic.edu/

This library is free software; you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation; either version 2.1 of the License, or (at
your option) any later version.

This library is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this library; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu [gu@lac.uic.edu], last updated 01/25/2007
*****************************************************************************/


#include <string.h>
#include <message.h>

using namespace cb;


CUserMessage::CUserMessage():
m_iDataLength(0),
m_iBufLength(65536)
{
   m_pcBuffer = new char[m_iBufLength];
}

CUserMessage::CUserMessage(const int& len):
m_iDataLength(0),
m_iBufLength(len)
{
   if (m_iBufLength < 4)
      m_iBufLength = 4;

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


int32_t CRTMsg::getType() const
{
   return *(int32_t*)m_pcBuffer;
}

void CRTMsg::setType(const int32_t& type)
{
   *(int32_t*)m_pcBuffer = type;
}

char* CRTMsg::getData() const
{
   return m_pcBuffer + 4;
}

void CRTMsg::setData(const int& offset, const char* data, const int& len)
{
   while (4 + offset + len > m_iBufLength)
      resize(m_iBufLength << 1);

   memcpy(m_pcBuffer + 4 + offset, data, len);
}


int32_t CCBMsg::getType() const
{
   return *(int32_t*)m_pcBuffer;
}

void CCBMsg::setType(const int32_t& type)
{
   *(int32_t*)m_pcBuffer = type;
}

char* CCBMsg::getData() const
{
   return m_pcBuffer + 4;
}

void CCBMsg::setData(const int& offset, const char* data, const int& len)
{
   while (4 + offset + len > m_iBufLength)
      resize(m_iBufLength << 1);

   memcpy(m_pcBuffer + 4 + offset, data, len);
}
