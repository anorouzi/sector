#include <iostream>
#include "sql.h"

using namespace std;

int main()
{
   char* sql = "SELECT attr1, attr2 FROM ta;";

   CQueryAttr* q = CParser::parse(sql, strlen(sql));

   cout << endl << "ATTR LIST" << endl;
   for (vector<string>::iterator i = q->m_vAttrList.begin(); i != q->m_vAttrList.end(); ++ i)
      cout << *i << endl;

   cout << endl << "TABLE LIST" << endl;
   for (vector<string>::iterator i = q->m_vTableList.begin(); i != q->m_vTableList.end(); ++ i)
      cout << *i << endl;


   return 1;
}
