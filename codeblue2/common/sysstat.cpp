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
   Yunhong Gu [gu@lac.uic.edu], last updated 11/12/2008
*****************************************************************************/

#include "sysstat.h"
#include <time.h>
#include <iostream>

using namespace std;

const int SysStat::g_iSize = 40;

int SysStat::serialize(char* buf, int& size, map<int, SlaveNode>& sl, Cluster& c)
{
   if (size < g_iSize + 8 + c.m_mSubCluster.size() * 48 + sl.size() * 72)
      return -1;

   *(int64_t*)buf = m_llStartTime;
   *(int64_t*)(buf + 8) = m_llAvailDiskSpace;
   *(int64_t*)(buf + 16) = m_llTotalFileSize;
   *(int64_t*)(buf + 24) = m_llTotalFileNum;
   *(int64_t*)(buf + 32) = m_llTotalSlaves;

   char* p = buf + 40;
   *(int64_t*)p = c.m_mSubCluster.size();
   p += 8;
   for (map<int, Cluster>::iterator i = c.m_mSubCluster.begin(); i != c.m_mSubCluster.end(); ++ i)
   {
      *(int64_t*)p = i->second.m_iClusterID;
      *(int64_t*)(p + 8) = i->second.m_iTotalNodes;
      *(int64_t*)(p + 16) = i->second.m_llAvailDiskSpace;
      *(int64_t*)(p + 24) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 32) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 40) = i->second.m_llTotalOutputData;

      p += 48;
   }

   p = buf + 40 + 8 + c.m_mSubCluster.size() * 48;
   for (map<int, SlaveNode>::iterator i = sl.begin(); i != sl.end(); ++ i)
   {
      strcpy(p, i->second.m_strIP.c_str());
      *(int64_t*)(p + 16) = i->second.m_llAvailDiskSpace;
      *(int64_t*)(p + 24) = i->second.m_llTotalFileSize;
      *(int64_t*)(p + 32) = i->second.m_llCurrMemUsed;
      *(int64_t*)(p + 40) = i->second.m_llCurrCPUUsed;
      *(int64_t*)(p + 48) = i->second.m_llTotalInputData;
      *(int64_t*)(p + 56) = i->second.m_llTotalOutputData;
      *(int64_t*)(p + 64) = i->second.m_llTimeStamp;

      p += 72;
   }

   size = 40 + 8 + c.m_mSubCluster.size() * 48 + sl.size() * 72;

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

   char* p = buf + 40;
   int c = *(int64_t*)p;
   m_vCluster.resize(c);
   p += 8;
   for (vector<Cluster>::iterator i = m_vCluster.begin(); i != m_vCluster.end(); ++ i)
   {
      i->m_iClusterID = *(int64_t*)p;
      i->m_iTotalNodes = *(int64_t*)(p + 8);
      i->m_llAvailDiskSpace = *(int64_t*)(p + 16);
      i->m_llTotalFileSize = *(int64_t*)(p + 24);
      i->m_llTotalInputData = *(int64_t*)(p + 32);
      i->m_llTotalOutputData = *(int64_t*)(p + 40);

      p += 48;
   }

   int n = (size - 40 - 8 - c * 48) / 72;
   m_vSlaveList.resize(n);
   p = buf + 40 + 8 + c * 48;
   for (vector<SlaveNode>::iterator i = m_vSlaveList.begin(); i != m_vSlaveList.end(); ++ i)
   {
      i->m_strIP = p;
      i->m_llAvailDiskSpace = *(int64_t*)(p + 16);
      i->m_llTotalFileSize = *(int64_t*)(p + 24);
      i->m_llCurrMemUsed = *(int64_t*)(p + 32);
      i->m_llCurrCPUUsed = *(int64_t*)(p + 40);
      i->m_llTotalInputData = *(int64_t*)(p + 48);
      i->m_llTotalOutputData = *(int64_t*)(p + 56);
      i->m_llTimeStamp = *(int64_t*)(p + 64);

      p += 72;
   }

   return 0;
}

void SysStat::print()
{
   const int MB = 1024 * 1024;

   cout << "Sector System Information:" << endl;
   time_t st = m_llStartTime;
   cout << "Running since " << ctime(&st);
   cout << "Available Disk Size " << m_llAvailDiskSpace / MB << " MB" << endl;
   cout << "Total File Size " << m_llTotalFileSize / MB << " MB" << endl;
   cout << "Total Number of Files " << m_llTotalFileNum << endl;
   cout << "Total Number of Slave Nodes " << m_llTotalSlaves << endl;

   cout << "------------------------------------------------------------\n";

   cout << "Total number of clusters " << m_vCluster.size() << endl;
   cout << "Cluster_ID  Total_Nodes  AvailDisk(MB)  FileSize(MB)  NetIn(MB)  NetOut(MB)\n";
   for (vector<Cluster>::iterator i = m_vCluster.begin(); i != m_vCluster.end(); ++ i)
   {
      cout << i->m_iClusterID << ":  " 
           << i->m_iTotalNodes << "  " 
           << i->m_llAvailDiskSpace / MB << "  " 
           << i->m_llTotalFileSize / MB << "  " 
           << i->m_llTotalInputData / MB << "  " 
           << i->m_llTotalOutputData / MB << endl;
   }

   cout << "------------------------------------------------------------\n";
   cout << "SLAVE_ID  IP  TS(us)  AvailDisk(MB)  TotalFile(MB)  Mem(MB)  CPU(us)  NetIn(MB)  NetOut(MB)\n";

   int s = 1;
   for (vector<SlaveNode>::iterator i = m_vSlaveList.begin(); i != m_vSlaveList.end(); ++ i)
   {
      cout << s++ << ":  "
           << i->m_strIP << "  " 
           << i->m_llTimeStamp << "  " 
           << i->m_llAvailDiskSpace / MB << "  " 
           << i->m_llTotalFileSize / MB << "  " 
           << i->m_llCurrMemUsed / MB << "  " 
           << i->m_llCurrCPUUsed << "  " 
           << i->m_llTotalInputData / MB << "  " 
           << i->m_llTotalOutputData / MB << endl;
   }
}
