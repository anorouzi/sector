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

#include "sysstat.h"
#include <time.h>
#include <iostream>

using namespace std;

const int SysStat::g_iSize = 40;

int SysStat::serialize(char* buf, int& size)
{
   if (size < g_iSize)
      return -1;

   *(int64_t*)buf = m_llStartTime;
   *(int64_t*)(buf + 8) = m_llAvailDiskSpace;
   *(int64_t*)(buf + 16) = m_llTotalFileSize;
   *(int64_t*)(buf + 24) = m_llTotalFileNum;
   *(int64_t*)(buf + 32) = m_llTotalSlaves;

   size = 40;

   return 0;
}

int SysStat::deserialize(char* buf, const int& size)
{
   if (size < g_iSize)
      return -1;

   m_llStartTime = *(int64_t*)buf;
   m_llAvailDiskSpace = *(int64_t*)(buf + 8);
   m_llTotalFileSize = *(int64_t*)(buf + 16);
   m_llTotalFileNum = *(int64_t*)(buf + 24);
   m_llTotalSlaves = *(int64_t*)(buf + 32);

   return 0;
}

void SysStat::print()
{
   cout << "Sector System Information:" << endl;
   time_t st = m_llStartTime;
   cout << "Running since " << ctime(&st);
   cout << "Available Disk Size " << m_llAvailDiskSpace / 1024 / 1024 << " MB" << endl;
   cout << "Total File Size " << m_llTotalFileSize / 1024 /1024 << " MB" << endl;
   cout << "Total Number of Files " << m_llTotalFileNum << endl;
   cout << "Total Number of Slave Nodes " << m_llTotalSlaves << endl;
}
