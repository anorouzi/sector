#include <fsclient.h>
#include <util.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   if (argc != 5)
   {
      cout << "USAGE: cp <ip> <port> <src_file/dir> <dst_file/dir>\n";
      return -1;
   }

   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");


   string path = argv[3];
   bool wc = WildCard::isWildCard(path);

   if (!wc)
   {
      int r = Sector::copy(argv[3], argv[4]);
      if (r < 0)
         cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;
   }
   else
   {
      SNode attr;
      if (Sector::stat(argv[4], attr) < 0)
      {
         cout << "destination directory does not exist.\n";
      }
      else
      {
         string orig = path;
         size_t p = path.rfind('/');
         if (p == string::npos)
            path = "/";
         else
         {
            path = path.substr(0, p);
            orig = orig.substr(p + 1, orig.length() - p);
         }

         vector<SNode> filelist;
         int r = Sector::list(path, filelist);
         if (r < 0)
            cout << "ERROR: " << r << " " << SectorError::getErrorMsg(r) << endl;

         vector<string> filtered;
         for (vector<SNode>::iterator i = filelist.begin(); i != filelist.end(); ++ i)
         {
cout << "list " << i->m_strName << endl;
            if (WildCard::match(orig, i->m_strName))
{
               filtered.push_back(path + "/" + i->m_strName);
cout << "filtered " << path + "/" + i->m_strName << endl;
}
         }

         for (vector<string>::iterator i = filtered.begin(); i != filtered.end(); ++ i)
            Sector::copy(*i, argv[4]);
      }
   }

   Sector::logout();
   Sector::close();

   return 1;
}
