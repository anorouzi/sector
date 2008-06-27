#include <master.h>

int main()
{
   Master m;

   if (m.init() < 0)
      return -1;

   m.run();

   return 1;
}
