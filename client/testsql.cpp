#include "sqlclient.h"
using namespace cb;

int main(int argc, char** argv)
{
   Sector::init(argv[1], atoi(argv[2]));

cout << "connected\n";

   vector<DataAttr> attr;
   Sector::getSemantics("stream.dat", attr);

   Semantics::display(attr);

   Query* q = Sector::createQueryHandle();
   if (q->open("SELECT * FROM stream.dat;") < 0)
   {
      cout << "open failed\n";
      return -1;
   }

   char res[80];
   int rows = 10;
   int size = 80;
   q->fetch(res, rows, size);

   char* p = res;
   for (int i = 0; i < rows; ++ i)
   {
      cout << *(int*)p << " " << *(float*)(p + 4) << endl;
      p += 8;
   }

   q->close();
   Sector::releaseQueryHandle(q);

   Sector::close();

   return 0;
}
