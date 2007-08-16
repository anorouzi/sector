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
   Yunhong Gu [gu@lac.uic.edu], last updated 04/06/2007
*****************************************************************************/


#ifndef __FILE_H__
#define __FILE_H__

#include <udt.h>

namespace cb
{

class CFSClient;

class CFileAttr
{
public:
   CFileAttr();
   virtual ~CFileAttr();

   CFileAttr& operator=(const CFileAttr& f);

public:
   void serialize(char* attr, int& len) const;
   void deserialize(const char* attr, const int& len);

public:
   char m_pcName[64];           // unique file name
   uint32_t m_uiID;	        // id
   int64_t m_llTimeStamp;       // time stamp
   int32_t m_iAttr;	        // 01: READ	10: WRITE	11: READ&WRITE
   int32_t m_iType;		// 0: normal	1: cache	2: semantics	3: operator	4: record index
   int64_t m_llSize;		// size
   char m_pcChecksum[20];	// SHA digest, checksum
};

struct CAttrComp
{
   bool operator()(const CFileAttr& a1, const CFileAttr& a2) const
   {
      return (strcmp(a1.m_pcName, a2.m_pcName) > 0);
   }
};

}; // namespace

#endif
