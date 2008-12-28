#include <client.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      cout << "USAGE: stat <ip> <port> file\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));

   if (Sector::login("test", "xxx") < 0)
   {
      cerr << "login failed. check password or IP ACL.\n";
      return -1;
   }

   SNode attr;
   if (Sector::stat(argv[3], attr) < 0)
   {
      cerr << "file does not exist.\n";
   }
   else if (attr.m_bIsDir)
   {
      cout << argv[3] << " is a directory.\n";
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
