#include <sector.h>
#include <conf.h>
#include <iostream>

using namespace std;


string format(const int64_t& val)
{
   string fmt_val = "";

   int64_t left = val;
   while (left > 0)
   {
      int section = left % 1000;
      left = left / 1000;

      char buf[8];
      sprintf(buf, "%d", section);

      if (fmt_val.length() == 0)
         fmt_val = buf;
      else
         fmt_val = fmt_val + "," + buf;
   }

   return fmt_val;
}

string format(const string& str, const int len)
{
   string fmt_str = str;

   for (int i = fmt_str.length(); i < len; ++ i)
      fmt_str += " ";

   return fmt_str;
}

string format(const int64_t& val, const int len)
{
   char buf[64];
   sprintf(buf, "%lld", val);

   return format(buf, len);
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
        << format("NetIn(MB)", 10)
        << format("NetOut(MB)", 10) << endl;
   for (vector<SysStat::ClusterStat>::const_iterator i = s.m_vCluster.begin(); i != s.m_vCluster.end(); ++ i)
   {
      if (i->m_iTotalNodes <= 0)
         continue;

      cout << format(i->m_iClusterID, 12)
           << format(i->m_iTotalNodes, 12)
           << format(format(i->m_llAvailDiskSpace / MB), 15)
           << format(format(i->m_llTotalFileSize / MB), 15)
           << format(i->m_llTotalInputData / MB, 10)
           << format(i->m_llTotalOutputData / MB, 10) << endl;
   }

   cout << "------------------------------------------------------------\n";
   cout << format("SLAVE_ID", 10)
        << format("IP", 16)
        << format("TS(us)", 20)
        << format("AvailDisk(MB)", 15)
        << format("TotalFile(MB)", 15)
        << format("Mem(MB)", 12)
        << format("CPU(us)", 12)
        << format("NetIn(MB)", 10)
        << format("NetOut(MB)", 10) << endl;

   for (vector<SysStat::SlaveStat>::const_iterator i = s.m_vSlaveList.begin(); i != s.m_vSlaveList.end(); ++ i)
   {
      cout << format(i->m_iID, 10)
           << format(i->m_strIP, 16)
           << format(i->m_llTimeStamp, 20)
           << format(format(i->m_llAvailDiskSpace / MB), 15)
           << format(format(i->m_llTotalFileSize / MB), 15)
           << format(i->m_llCurrMemUsed / MB, 12)
           << format(i->m_llCurrCPUUsed, 12)
           << format(i->m_llTotalInputData / MB, 10)
           << format(i->m_llTotalOutputData / MB, 10) << endl;
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

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (client.login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   SysStat sys;
   int r = client.sysinfo(sys);
   if (r >= 0)
      print(sys);
   else
      cerr << "Error happened, failed to retrieve any system information.\n";

   client.logout();
   client.close();

   return r;
}
