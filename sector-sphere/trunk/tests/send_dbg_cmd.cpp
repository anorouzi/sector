#include <iostream>
#include <client.h>
#include <conf.h>

using namespace std;

int main(int argc, char** argv)
{
   CmdLineParser clp;
   if (clp.parse(argc, argv) <= 0)
   {
      cout << "USAGE: sector_shutdown -a | -i <slave id> | -d <slave IP:port> | -r <rack topo path>\n";
      return -1;
   }

   int32_t id = 0;
   int32_t code = 0;
   for (map<string, string>::iterator i = clp.m_mParams.begin(); i != clp.m_mParams.end(); ++ i)
   {
      if (i->first == "i")
         id = atoi(i->second.c_str());
      else if (i->first == "c")
         code = atoi(i->second.c_str());
      else
      {
         cout << "USAGE: send_dbg_cmd -i <slave id> -c <cmd code>\n";
         return -1;
      }
   }

   if ((id == 0) || (code == 0))
   {
      cout << "USAGE: send_dbg_cmd -i <slave id> -c <cmd code>\n";
      return -1;
   }

   Session s;
   s.loadInfo("../conf/client.conf");

   Client c;
   if (c.init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;

   string passwd = s.m_ClientConf.m_strPassword;
   if (s.m_ClientConf.m_strUserName != "root")
   {
      cout << "please input root password:";
      cin >> passwd;
   }

   if (c.login("root", passwd, s.m_ClientConf.m_strCertificate.c_str()) < 0)
      return -1;

   int r = c.sendDebugCode(id, code);

   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   c.logout();
   c.close();

   return r;
}
