/*****************************************************************************
Copyright 2005 - 2010 The Board of Trustees of the University of Illinois.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
*****************************************************************************/

/*****************************************************************************
written by
   Yunhong Gu, last updated 01/12/2010
*****************************************************************************/

#include <sector.h>
#include <conf.h>
#include <iostream>

using namespace std;

void print_error(int code)
{
   cerr << "ERROR: " << code << " " << SectorError::getErrorMsg(code) << endl;
}

string format(const int64_t& val)
{
   string fmt_val = "";

   int64_t left = val;
   while (left > 0)
   {
      int section = left % 1000;
      left = left / 1000;

      char buf[8];
      if (left > 0)
         sprintf(buf, "%03d", section);
      else
         sprintf(buf, "%d", section);

      if (fmt_val.c_str()[0] == 0)
         fmt_val = buf;
      else
         fmt_val = string(buf) + "," + fmt_val;
   }

   // nothing left, assign 0
   if (fmt_val.c_str()[0] == 0)
      fmt_val = "0";

   return fmt_val;
}

string format(const string& str, const int len)
{
   string fmt_str = str;

   for (int i = fmt_str.length(); i < len; ++ i)
      fmt_str += " ";

   return fmt_str;
}

string toString(const int64_t& val)
{
   char buf[64];
   sprintf(buf, "%lld", val);

   return buf;
}

string format(const int64_t& val, const int len)
{
   return format(toString(val), len);
}

void print(const SysStat& s)
{
   const float MB = 1024.0 * 1024.0;

   cout << endl;

   cout << "Sector System Information:" << endl;
   time_t st = s.m_llStartTime;
   cout << "Running since:               " << ctime(&st);
   cout << "Available Disk Size:         " << format(s.m_llAvailDiskSpace / MB) << " MB" << endl;
   cout << "Total File Size:             " << format(s.m_llTotalFileSize / MB) << " MB" << endl;
   cout << "Total Number of Files:       " << s.m_llTotalFileNum << endl;
   cout << "Total Number of Slave Nodes: " << s.m_llTotalSlaves << endl;

   cout << "------------------------------------------------------------\n";
   cout << format("MASTER ID", 10) << format("IP", 16) << "PORT" << endl;
   for (vector<SysStat::MasterStat>::const_iterator i = s.m_vMasterList.begin(); i != s.m_vMasterList.end(); ++ i)
   {
      cout << format(i->m_iID, 10) << format(i->m_strIP, 16) << i->m_iPort << endl;
   }

   cout << "------------------------------------------------------------\n";

   int total_cluster = 0;
   for (vector<SysStat::ClusterStat>::const_iterator i = s.m_vCluster.begin(); i != s.m_vCluster.end(); ++ i)
   {
      if (i->m_iTotalNodes > 0)
         ++ total_cluster;
   }

   cout << "Total number of clusters:    " << total_cluster << endl;
   cout << format("Cluster_ID", 12)
        << format("Total_Nodes", 12)
        << format("AvailDisk(MB)", 15)
        << format("FileSize(MB)", 15)
        << format("NetIn(MB)", 15)
        << format("NetOut(MB)", 15) << endl;
   for (vector<SysStat::ClusterStat>::const_iterator i = s.m_vCluster.begin(); i != s.m_vCluster.end(); ++ i)
   {
      if (i->m_iTotalNodes <= 0)
         continue;

      cout << format(i->m_iClusterID, 12)
           << format(i->m_iTotalNodes, 12)
           << format(format(i->m_llAvailDiskSpace / MB), 15)
           << format(format(i->m_llTotalFileSize / MB), 15)
           << format(i->m_llTotalInputData / MB, 15)
           << format(i->m_llTotalOutputData / MB, 15) << endl;
   }

   cout << "------------------------------------------------------------\n";
   cout << format("SLAVE_ID", 10)
        << format("Address", 24)
        << format("AvailDisk(MB)", 15)
        << format("TotalFile(MB)", 15)
        << format("Mem(MB)", 12)
        << format("CPU(us)", 12)
        << format("NetIn(MB)", 15)
        << format("NetOut(MB)", 15)
        << format("TS(us)", 20) << endl;

   for (vector<SysStat::SlaveStat>::const_iterator i = s.m_vSlaveList.begin(); i != s.m_vSlaveList.end(); ++ i)
   {
      cout << format(i->m_iID, 10)
           << format(i->m_strIP + ":" + toString(i->m_iPort) , 24)
           << format(format(i->m_llAvailDiskSpace / MB), 15)
           << format(format(i->m_llTotalFileSize / MB), 15)
           << format(i->m_llCurrMemUsed / MB, 12)
           << format(i->m_llCurrCPUUsed, 12)
           << format(i->m_llTotalInputData / MB, 15)
           << format(i->m_llTotalOutputData / MB, 15)
           << format(i->m_llTimeStamp, 20) << endl;
   }

   cout << endl;
}

int main(int argc, char** argv)
{
   if (argc != 1)
   {
      cerr << "USAGE: sysinfo\n";
      return -1;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   int result = 0;
   if ((result = client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort)) < 0)
   {
      print_error(result);
      return -1;
   }
   if ((result = client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str())) < 0)
   {
      print_error(result);
      return -1;
   }

   SysStat sys;
   result = client.sysinfo(sys);
   if (result >= 0)
      print(sys);
   else
      print_error(result);

   client.logout();
   client.close();

   return result;
}
