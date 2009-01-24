#include <fsclient.h>
#include <iostream>
#include <iomanip>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 4)
   {
      cout << "USAGE: ls <ip> <port> <dir>\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   vector<SNode> filelist;
   int r = Sector::list(argv[3], filelist);
   if (r < 0)
      cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

   for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
   {
      cout << setiosflags(ios::left) << setw(40) << i->m_strName << "\t";
      if (i->m_bIsDir)
         cout << "<dir>" << endl;
      else
      {
         time_t t = i->m_llTimeStamp;
         cout << i->m_llSize << " bytes " << "\t" << ctime(&t);
      }
   }

   Sector::logout();
   Sector::close();

   return 1;
}
