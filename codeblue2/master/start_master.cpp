#include <master.h>

int main(int argc, char** argv)
{
   Master m;

   if (m.init() < 0)
      return -1;

   if (argc == 3)
   {
      if (m.join(argv[1], atoi(argv[2])) < 0)
         return -1;
   }

   m.run();

   return 1;
}
