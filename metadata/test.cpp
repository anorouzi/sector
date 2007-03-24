#include "data.h"

int main()
{
   vector<DataAttr> attr;
   cb::Semantics::loadSemantics("semantics1.sem", attr);
   cb::Semantics::display(attr);

   return 0;
}
