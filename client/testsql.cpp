#include "sqlclient.h"

int main(int argc, char** argv)
{
   SQLClient sqlclient;

   sqlclient.connect(argv[1], atoi(argv[2]));

cout << "connected\n";

   vector<DataAttr> attr;
   sqlclient.getSemantics("test.txt", attr);

   Semantics::display(attr);

   Query* q = sqlclient.createQueryHandle();
   q->open("SELECT * FROM test.txt;");
   q->close();
   sqlclient.releaseQueryHandle(q);

   return 0;
}
