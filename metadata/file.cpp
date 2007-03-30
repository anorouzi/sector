/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
All Rights Reserved.

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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <file.h>

using namespace std;
using namespace cb;

CFileAttr::CFileAttr() 
{
   m_pcName[0] = '\0';
   m_uiID = 0;
   m_llTimeStamp = Time::getTime();
   m_iAttr = 0;
   m_iType = 0;
   m_llSize = 0;
   m_pcHost[0] = '\0';
   m_iPort = 0;
   m_pcNameHost[0] = '\0';
   m_iNamePort = 0;
}

CFileAttr::~CFileAttr()
{
}

CFileAttr& CFileAttr::operator=(const CFileAttr& f)
{
   strcpy(m_pcName, f.m_pcName);
   m_uiID = f.m_uiID;
   m_llTimeStamp = f.m_llTimeStamp;
   m_iAttr = f.m_iAttr;
   m_iType = f.m_iType;
   m_llSize = f.m_llSize;
   strcpy(m_pcHost, f.m_pcHost);
   m_iPort = f.m_iPort;
   strcpy(m_pcNameHost, f.m_pcNameHost);
   m_iNamePort = f.m_iNamePort;

   return *this;
}

void CFileAttr::serialize(char* attr, int& len) const
{
   char* p = attr;

   memcpy(p, m_pcName, 64);
   p += 64;
   memcpy(p, m_pcHost, 64);
   p += 64;

   sprintf(p, "%d %lld %d %lld %d \n", m_uiID, m_llTimeStamp, m_iType, m_llSize, m_iPort);

   len = 64 * 2 + strlen(p) + 1;
}

void CFileAttr::deserialize(const char* attr, const int& len)
{
   char* p = (char*)attr;

   memcpy(m_pcName, p, 64);
   p += 64;
   memcpy(m_pcHost, p, 64);
   p += 64;

   sscanf(p, "%d %lld %d %lld %d", &m_uiID, &m_llTimeStamp, &m_iType, &m_llSize, &m_iPort);
}
