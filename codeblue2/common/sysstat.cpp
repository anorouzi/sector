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
   Yunhong Gu [gu@lac.uic.edu], last updated 10/28/2008
*****************************************************************************/

#include "sysstat.h"
#include <time.h>
#include <iostream>

using namespace std;

const int SysStat::g_iSize = 40;

int SysStat::serialize(char* buf, int& size, map<int, SlaveNode>& sl)
{
   if (size < g_iSize + sl.size() * 48)
      return -1;

   *(int64_t*)buf = m_llStartTime;
   *(int64_t*)(buf + 8) = m_llAvailDiskSpace;
   *(int64_t*)(buf + 16) = m_llTotalFileSize;
   *(int64_t*)(buf + 24) = m_llTotalFileNum;
   *(int64_t*)(buf + 32) = m_llTotalSlaves;

   char* p = buf + 40;
   for (map<int, SlaveNode>::iterator i = sl.begin(); i != sl.end(); ++ i)
   {
      //std::string m_strIP;
      //int64_t m_llAvailDiskSpace;
      //int64_t m_llTotalFileSize;
      //int64_t m_llCurrMemUsed;
      //int64_t m_llCurrCPUUsed;

      strcpy(p, i->second.m_strIP.c_str());
      *(int64_t*)(p + 16) = i->second.m_llAvailDiskSpace;
      *(int64_t*)(p + 24) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 32) = i->second.m_llCurrMemUsed;
      *(int64_t*)(p + 40) = i->second.m_llCurrCPUUsed;

      p += 48;
   }

   size = 40 + sl.size() * 48;

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

   int n = (size - 40) / 48;
   m_vSlaveList.resize(n);
   char* p = buf + 40;
   for (vector<SlaveNode>::iterator i = m_vSlaveList.begin(); i != m_vSlaveList.end(); ++ i)
   {
      i->m_strIP = p;
      i->m_llAvailDiskSpace = *(int64_t*)(p + 16);
      i->m_llTotalFileSize = *(int64_t*)(p + 24);
      i->m_llCurrMemUsed = *(int64_t*)(p + 32);
      i->m_llCurrCPUUsed = *(int64_t*)(p + 40);
   }

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

   cout << "------------------------------------------------------------\n";

   for (vector<SlaveNode>::iterator i = m_vSlaveList.begin(); i != m_vSlaveList.end(); ++ i)
   {
      cout << i->m_strIP << " " << i->m_llAvailDiskSpace << " " << i->m_llTotalFileSize << " " << i->m_llCurrMemUsed << " " << i->m_llCurrCPUUsed << endl;
   }
}
