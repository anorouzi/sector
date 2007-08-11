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


#ifndef __KNOWLEDGE_BASE_H__
#define __KNOWLEDGE_BASE_H__

#include <sys/types.h>
#include <string>

using namespace std;

namespace cb
{

class KnowledgeBase
{
public:
   KnowledgeBase():
   m_iNumConn(0)
   {
   }

public:
   int init();
   int refresh();

   static int64_t getTotalDataSize(const string& path);

public:
   int m_iNumConn;

   int m_iCPUIndex;
   int m_iMemSize;

   int m_iDiskReadIndex;
   int m_iDiskWriteIndex;
};

} // namespace

#endif
