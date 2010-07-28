#include <sector.h>
#include <conf.h>
#include <iostream>

using namespace std;

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
      sys.print();
   else
      cerr << "Error happened, failed to retrieve any system information.\n";

   client.logout();
   client.close();

   return r;
}
