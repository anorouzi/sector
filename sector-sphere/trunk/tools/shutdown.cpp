#include <iostream>
#include <sector.h>
#include <conf.h>

using namespace std;

int main(int argc, char** argv)
{
   if ((argc != 2) && (argc != 3))
   {
      cout << "USAGE: sector_shutdown -a | -i <slave id> | -d <slave IP:port> | -r <rack topo path>\n";
      return -1;
   }

   Sector client;

   Session s;
   s.loadInfo("../conf/client.conf");

   if (client.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;

   cout << "please input root password:";
   string passwd;
   cin >> passwd;

   if (client.login("root", passwd, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   CmdLineParser clp;
   if (clp.parse(argc, argv) <= 0)
   {
      cout << "USAGE: sector_shutdown -a | -i <slave id> | -d <slave IP:port> | -r <rack topo path>\n";
      return -1;
   }

   int r = 0;
   string type = clp.m_mParams.begin()->first;

   if (type == "a")
      r = client.shutdown(1);
   else if (type == "i")
      r = client.shutdown(2, clp.m_mParams.begin()->second);
   else if (type == "d")
      r = client.shutdown(3, clp.m_mParams.begin()->second);
   else if (type == "r")
      r = client.shutdown(4, clp.m_mParams.begin()->second);
   else
   {
      cout << "USAGE: sector_shutdown -a | -i <slave id> | -d <slave IP:port> | -r <rack topo path>\n";
   }

   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   client.logout();
   client.close();

   return r;
}
