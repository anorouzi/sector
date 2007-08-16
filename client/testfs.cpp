#include <fsclient.h>
#include <iostream>

using namespace std;
using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

   File* f1 = Sector::createFileHandle();
   if (NULL == f1)
      return -1;

   if (f1->open("test.txt", 1) < 0)
   {
      cout << "error to open file." << endl;
      return -1;
   }

   char buf[1024];
   f1->read(buf, 0, 10);

   string test = "**********Hello World!";

   f1->write(test.c_str(), 20, test.length());

   f1->close();

   f1->open("test.txt");
   int res = f1->read(buf, 0, 10);

   buf[10] = '\0';
   cout << "res = " << res << " " << buf << endl;

   f1->write(test.c_str(), 20, test.length());
   f1->close();

   f1->open("test.txt");
   f1->read(buf, 0, 10);
   f1->write(test.c_str(), 20, test.length());
   f1->close();

   Sector::releaseFileHandle(f1);

   CFileAttr attr;
   Sector::stat("test.txt", attr);
   cout << attr.m_pcName << " " << attr.m_llSize << endl;

   Sector::stat("rate.txt", attr);
   cout << attr.m_pcName << " " << attr.m_llSize << endl;

   Sector::close();

   return 1;
}
