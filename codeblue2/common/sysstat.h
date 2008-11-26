/*****************************************************************************
Copyright © 2006 - 2008, The Board of Trustees of the University of Illinois.
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
   Yunhong Gu [gu@lac.uic.edu], last updated 07/06/2008
*****************************************************************************/


#ifndef __SECTOR_SYSSTAT_H__
#define __SECTOR_SYSSTAT_H__

#include <stdint.h>
#include <topology.h>

class SysStat
{
public:
   int64_t m_llStartTime;

   int64_t m_llAvailDiskSpace;
   int64_t m_llTotalFileSize;
   int64_t m_llTotalFileNum;

   int64_t m_llTotalSlaves;

   std::vector<SlaveNode> m_vSlaveList;
   std::vector<Cluster> m_vCluster;

public:
   int serialize(char* buf, int& size, std::map<int, SlaveNode>& sl, Cluster& c);
   int deserialize(char* buf, const int& size);

   void print();

public:
   static const int g_iSize;
};

#endif
