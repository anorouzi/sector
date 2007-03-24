#include "sqlclient.h"
using namespace cb;

int main(int argc, char** argv)
{
   SQLClient sqlclient;

   sqlclient.connect(argv[1], atoi(argv[2]));

cout << "connected\n";

   vector<DataAttr> attr;
   sqlclient.getSemantics("stream.dat", attr);

   Semantics::display(attr);

   Query* q = sqlclient.createQueryHandle();
   q->open("SELECT * FROM stream.dat;");

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
   sqlclient.releaseQueryHandle(q);

   return 0;
}
