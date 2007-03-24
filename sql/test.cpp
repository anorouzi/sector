#include <iostream>
#include "sql.h"

using namespace std;
using namespace cb;

int main()
{
//   char* sql = "SELECT attr1, attr2, attr3 FROM ta1, ta2 WHERE a = 1;";
//   char* sql = "SELECT attr1, attr2, attr3 FROM ta1, ta2;";
   char* sql = "SELECT * FROM stream.dat;";
   SQLExpr expr;

   if (0 != SQLParser::parse(sql, expr))
   {
      cout << "grammar error!\n";
      return -1;
   }

   cout << endl << "ATTR LIST" << endl;
   for (vector<string>::iterator i = expr.m_vstrFieldList.begin(); i != expr.m_vstrFieldList.end(); ++ i)
      cout << *i << endl;

   cout << endl << "TABLE LIST" << endl;
   for (vector<string>::iterator i = expr.m_vstrTableList.begin(); i != expr.m_vstrTableList.end(); ++ i)
      cout << *i << endl;

   return 1;
}
