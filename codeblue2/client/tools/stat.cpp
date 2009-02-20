#include <fsclient.h>
#include <iostream>
#include <util.h>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 2)
   {
      cout << "USAGE: stat file\n";
      return -1;
   }

   Session s;
   s.loadInfo("../client.conf");

   if (Sector::init(s.m_ClientConf.m_strMasterIP, s.m_ClientConf.m_iMasterPort) < 0)
      return -1;
   if (Sector::login(s.m_ClientConf.m_strUserName, s.m_ClientConf.m_strPassword) < 0)
      return -1;

   SNode attr;
   int r = Sector::stat(argv[1], attr);

   if (r < 0)
   {
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;
   }
   else if (attr.m_bIsDir)
   {
      cout << argv[1] << " is a directory.\n";
   }
   else
   {
      cout << "FILE NAME: " << attr.m_strName << endl;
      cout << "SIZE: " << attr.m_llSize << " bytes" << endl;
      time_t ft = attr.m_llTimeStamp;
      cout << "LAST MODIFIED: " << ctime(&ft);
      cout << "LOCATION: ";
      for (set<Address, AddrComp>::iterator i = attr.m_sLocation.begin(); i != attr.m_sLocation.end(); ++ i)
      {
         cout << i->m_strIP << ":" << i->m_iPort << " ";
      }
      cout << endl;
   }

   Sector::logout();
   Sector::close();

   return 1;
}
