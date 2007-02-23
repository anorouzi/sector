#include "data.h"

int main()
{
   vector<DataAttr> attr;
   Semantics::loadSemantics("semantics1.sem", attr);
   Semantics::display(attr);

   return 0;
}
