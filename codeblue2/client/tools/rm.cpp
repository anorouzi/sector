#include <fsclient.h>
#include <util.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      cout << "USAGE: rm <dir>\n";
      return -1;
   }

   Session s;
   s.loadInfo("../client.conf");

   if (Sector::init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (Sector::login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword) < 0)
      return -1;

   int r = Sector::remove(argv[1]);
   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   Sector::logout();
   Sector::close();

   return 1;
}
