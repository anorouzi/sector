/*****************************************************************************
Copyright © 2006, 2007, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 02/23/2007
*****************************************************************************/


#include <file.h>
#include <util.h>

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

   return *this;
}

void CFileAttr::serialize(char* attr, int& len) const
{
   char* p = attr;

   memcpy(p, m_pcName, 64);
   p += 64;

   sprintf(p, "%d %lld %d %lld\n", m_uiID, m_llTimeStamp, m_iType, m_llSize);

   len = 64 + strlen(p) + 1;
}

void CFileAttr::deserialize(const char* attr, const int& len)
{
   char* p = (char*)attr;

   memcpy(m_pcName, p, 64);
   p += 64;

   sscanf(p, "%d %lld %d %lld", &m_uiID, &m_llTimeStamp, &m_iType, &m_llSize);
}
