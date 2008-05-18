#include <fsclient.h>
#include <iostream>

using namespace std;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));
   Sector::login("test", "xxx");

   vector<SNode> filelist;
   Sector::list("angle", filelist);

   SectorFile f1, f2;

   if (f1.open("angle/features.data", 1) < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }

   char buf[1024];
   f1.read(buf, 0, 10);

   buf[10] = '\0';
   cout << "res = " << buf << endl;

   f1.download("local.dat", false);

   f1.close();

   if (f2.open("angle/test.dat", 2) < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }

   f2.upload("testfs");

   f2.close();

   SNode attr;
   Sector::stat("output.csv", attr);
   cout << attr.m_strName << " " << attr.m_llSize << endl;

   Sector::logout();
   Sector::close();

   return 1;
}
