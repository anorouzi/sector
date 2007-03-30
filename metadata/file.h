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


#ifndef __FILE_H__
#define __FILE_H__

#ifndef WIN32
   #include <stdint.h>
#endif
#include <string>
#include <set>
#include <util.h>

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

   int32_t m_iType;		// 0: normal	1: cache	2: semantics	

   int64_t m_llSize;		// size

   char m_pcHost[64];		// loc ip
   int32_t m_iPort;		// loc port

   char m_pcNameHost[64];	// ip for name server
   int32_t m_iNamePort;		// port for name server
};

struct CAttrComp
{
   bool operator()(const CFileAttr& a1, const CFileAttr& a2) const
   {
      int nc = strcmp(a1.m_pcName, a2.m_pcName);
      if (nc != 0)
          return (nc > 0);

      int hc = strcmp(a1.m_pcHost, a2.m_pcHost);
      if (hc != 0)
          return (hc > 0);

      return (a1.m_iPort > a2.m_iPort);
   }
};

}; // namespace

#endif
